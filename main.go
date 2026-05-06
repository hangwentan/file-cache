package main

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
	"golang.org/x/sync/singleflight"
)

var (
	ctx         = context.Background()
	redisClient *redis.Client
)

// 文件元信息结构
type FileMeta struct {
	ParentPath  string `json:"ParentPath"`
	Sign        string `json:"sign"`
	FilePath    string `json:"filePath"`
	FileNasPath string `json:"fileNasPath"`
	FileType    string `json:"fileType"`
	FileName    string `json:"fileName"`
	Number      int    `json:"number"`
	Level       int    `json:"level,omitempty"` // 按需保留
	VisitStatus int    `json:"visitStatus"`     // ✅ 是否已读
}

type Response struct {
	Code int    `json:"code"`
	Data any    `json:"data"`
	Msg  string `json:"msg"`
}

var nasRoot = "/opt/devNas2"
var fileNasPrefix = "/homes/项目主动备份"

func getUserReadKey(userId string) string {
	return "read_dirs:" + userId
}

func getAllUserIds() []string {
	var ids []string
	iter := redisClient.Scan(ctx, 0, "read_dirs:*", 500).Iterator()
	for iter.Next(ctx) {
		key := iter.Val() // e.g., read_dirs:123
		userId := strings.TrimPrefix(key, "read_dirs:")
		ids = append(ids, userId)
	}
	if err := iter.Err(); err != nil {
		log.Printf("扫描用户列表失败: %v\n", err)
	}
	return ids
}

// ===== 工具函数 =====
func md5Sign(text string) string {
	hash := md5.Sum([]byte(text))
	return hex.EncodeToString(hash[:])
}

// ===== 初始化 Redis =====
func initRedis() {
	redisClient = redis.NewClient(&redis.Options{
		Addr:     "192.168.1.122:32768",
		Password: "",
		DB:       0,
	})
	if err := redisClient.Ping(ctx).Err(); err != nil {
		panic("Redis连接失败: " + err.Error())
	}
}

// ===== 缓存目录结构（递归） =====
// seen 非 nil 时会记录本次写入的所有 key，便于刷新结束后清理孤立残留；on-demand 调用传 nil。
func cacheDirectoryRecursive(dir string, currentLevel, maxLevel int, ttl time.Duration, seen map[string]struct{}) error {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return err
	}
	return cacheDirEntriesRecursive(dir, entries, currentLevel, maxLevel, ttl, seen)
}

// 内部递归实现：复用上层已读取的 entries，避免对同一目录重复 ReadDir
func cacheDirEntriesRecursive(dir string, entries []os.DirEntry, currentLevel, maxLevel int, ttl time.Duration, seen map[string]struct{}) error {
	if currentLevel > maxLevel {
		return nil
	}

	type childWork struct {
		path    string
		entries []os.DirEntry
	}

	pipe := redisClient.TxPipeline()
	zItems := make([]redis.Z, 0, len(entries))
	children := make([]childWork, 0)

	for _, entry := range entries {
		name := entry.Name()
		if name == "#recycle" {
			continue
		}

		fullPath := filepath.Join(dir, name)
		isDir := entry.IsDir() // 等价于 entry.Info().IsDir()，省去一次 Lstat 系统调用
		fileType := "file"
		childrenCount := 0

		if isDir {
			fileType = "dir"
			childEntries, err := os.ReadDir(fullPath)
			if err != nil {
				log.Printf("跳过无法读取的子目录: %s，错误: %v\n", fullPath, err)
			} else {
				childrenCount = len(childEntries)
				if currentLevel+1 <= maxLevel {
					children = append(children, childWork{fullPath, childEntries})
				}
			}
		}

		meta := FileMeta{
			ParentPath:  dir,
			Sign:        md5Sign(fullPath),
			FilePath:    fullPath,
			FileNasPath: strings.Replace(fullPath, nasRoot, fileNasPrefix, 1),
			FileType:    fileType,
			FileName:    name,
			Number:      childrenCount,
			Level:       currentLevel,
		}

		data, err := json.Marshal(meta)
		if err != nil {
			log.Printf("序列化元信息失败: %s，错误: %v\n", fullPath, err)
			continue
		}
		pipe.Set(ctx, "filemeta:"+fullPath, data, ttl)
		if seen != nil {
			seen["filemeta:"+fullPath] = struct{}{}
		}
		zItems = append(zItems, redis.Z{Score: 0, Member: name})
	}

	// 一次往返 flush 当前目录的所有写入；MULTI/EXEC 保证 Del+ZAdd 不被并发读取看到中间空状态
	dirKey := "dir:" + dir
	pipe.Del(ctx, dirKey) // 清掉旧成员，防止已删除的文件名残留在 ZSet 中
	if len(zItems) > 0 {
		pipe.ZAdd(ctx, dirKey, zItems...)
		if ttl > 0 {
			pipe.Expire(ctx, dirKey, ttl)
		}
		if seen != nil {
			seen[dirKey] = struct{}{}
		}
	}
	if _, err := pipe.Exec(ctx); err != nil {
		log.Printf("Redis pipeline 执行失败 (%s): %v\n", dir, err)
	}

	// 递归处理子目录，复用前面已读取的 entries
	for _, c := range children {
		if err := cacheDirEntriesRecursive(c.path, c.entries, currentLevel+1, maxLevel, ttl, seen); err != nil {
			log.Printf("递归缓存 %s 失败: %v\n", c.path, err)
		}
	}

	return nil
}

// ===== 定时刷新缓存（不设过期时间） =====
func startCacheRefresher(root string, interval time.Duration, maxDepth int) {
	go func() {
		for {
			start := time.Now()
			log.Println("刷新缓存中...")
			seen := make(map[string]struct{})
			if err := cacheDirectoryRecursive(root, 1, maxDepth, 0, seen); err != nil {
				log.Printf("刷新缓存失败: %v\n", err)
			} else {
				pruned := pruneOrphanCacheKeys(seen)
				log.Printf("刷新缓存完成：写入 key=%d，清理孤立 key=%d，耗时 %s\n", len(seen), pruned, time.Since(start))
			}
			time.Sleep(interval)
		}
	}()
}

// pruneOrphanCacheKeys 清理本轮刷新没写入、且无 TTL 的 filemeta:/dir: key（即文件已从磁盘删除的残留）
// 跳过有 TTL 的 key（来自 buildCacheOnDemand，会自然过期）
func pruneOrphanCacheKeys(seen map[string]struct{}) int {
	var candidates []string
	for _, pattern := range []string{"filemeta:*", "dir:*"} {
		iter := redisClient.Scan(ctx, 0, pattern, 500).Iterator()
		for iter.Next(ctx) {
			key := iter.Val()
			if _, ok := seen[key]; ok {
				continue
			}
			candidates = append(candidates, key)
		}
		if err := iter.Err(); err != nil {
			log.Printf("扫描缓存 key 失败 (%s): %v\n", pattern, err)
		}
	}
	if len(candidates) == 0 {
		return 0
	}

	// 用 Pipeline 批量取 TTL，过滤掉有 TTL 的（属于按需缓存）
	const batchSize = 500
	var orphans []string
	for begin := 0; begin < len(candidates); begin += batchSize {
		end := min(begin+batchSize, len(candidates))
		batch := candidates[begin:end]

		pipe := redisClient.Pipeline()
		ttlCmds := make([]*redis.DurationCmd, len(batch))
		for i, k := range batch {
			ttlCmds[i] = pipe.TTL(ctx, k)
		}
		if _, err := pipe.Exec(ctx); err != nil {
			log.Printf("批量 TTL 查询失败: %v\n", err)
			continue
		}
		for i, k := range batch {
			ttl, err := ttlCmds[i].Result()
			if err != nil {
				continue
			}
			// go-redis 对 TTL 的特殊返回值是直接的纳秒数，不会乘以精度：
			//   -1（即 -1ns）= 存在但无 TTL（刷新管理的 key）→ 孤立项，待删
			//   -2（即 -2ns）= key 不存在（被并发删了）→ 跳过
			//   >= 0 = 有 TTL（按需缓存），让其自然过期
			// 兼容老版本可能返回 -1*time.Second / -2*time.Second 的情况
			if ttl >= 0 {
				continue
			}
			if ttl == -2 || ttl == -2*time.Second {
				continue
			}
			orphans = append(orphans, k)
		}
	}

	pruned := 0
	for begin := 0; begin < len(orphans); begin += batchSize {
		end := min(begin+batchSize, len(orphans))
		if err := redisClient.Del(ctx, orphans[begin:end]...).Err(); err != nil {
			log.Printf("删除孤立 key 失败: %v\n", err)
			continue
		}
		pruned += end - begin
	}
	return pruned
}

// 目录及其成员的内存快照，用于在多用户处理中共享一次 Redis 读取
type dirSnapshot struct {
	path  string
	names []string
}

// 一次性把所有 dir:* 数据加载到内存，避免每个用户都重新 SCAN+ZRange
func loadAllDirs() ([]dirSnapshot, error) {
	var keys []string
	iter := redisClient.Scan(ctx, 0, "dir:*", 500).Iterator()
	for iter.Next(ctx) {
		keys = append(keys, iter.Val())
	}
	if err := iter.Err(); err != nil {
		return nil, err
	}

	snapshots := make([]dirSnapshot, 0, len(keys))
	const batchSize = 100
	for begin := 0; begin < len(keys); begin += batchSize {
		end := min(begin+batchSize, len(keys))
		batch := keys[begin:end]

		pipe := redisClient.Pipeline()
		cmds := make([]*redis.StringSliceCmd, len(batch))
		for i, key := range batch {
			cmds[i] = pipe.ZRange(ctx, key, 0, -1)
		}
		if _, err := pipe.Exec(ctx); err != nil {
			return nil, err
		}

		for i, key := range batch {
			names, err := cmds[i].Result()
			if err != nil {
				log.Printf("ZRange 失败 (%s): %v\n", key, err)
				continue
			}
			path := strings.TrimPrefix(key, "dir:")
			if path == nasRoot {
				continue
			}
			snapshots = append(snapshots, dirSnapshot{path, names})
		}
	}
	return snapshots, nil
}

func propagateUnreadForUser(userId string, dirs []dirSnapshot) {
	readKey := getUserReadKey(userId)

	readDirs, err := redisClient.SMembers(ctx, readKey).Result()
	if err != nil {
		log.Printf("读取已读集合失败 (user=%s): %v\n", userId, err)
		return
	}
	if len(readDirs) == 0 {
		return // 用户没有任何已读记录，无需上溯
	}

	readSet := make(map[string]struct{}, len(readDirs))
	for _, d := range readDirs {
		readSet[d] = struct{}{}
	}

	for _, d := range dirs {
		hasUnread := false
		for _, name := range d.names {
			childPath := filepath.Join(d.path, name)
			if _, ok := readSet[childPath]; !ok {
				hasUnread = true
				break
			}
		}

		if hasUnread {
			// 这个目录也应该标记为"未读"
			delete(readSet, d.path)

			// 向上递归删除父目录标记
			parent := filepath.Dir(d.path)
			for parent != nasRoot && parent != "." && parent != "/" {
				delete(readSet, parent)
				parent = filepath.Dir(parent)
			}
			// 也处理 nasRoot 本身
			delete(readSet, nasRoot)
		}
	}

	// 用 TxPipeline 原子地替换已读集合，避免并发读取看到中间空状态
	pipe := redisClient.TxPipeline()
	pipe.Del(ctx, readKey)
	if len(readSet) > 0 {
		cleaned := make([]any, 0, len(readSet))
		for k := range readSet {
			cleaned = append(cleaned, k)
		}
		pipe.SAdd(ctx, readKey, cleaned...)
	}
	if _, err := pipe.Exec(ctx); err != nil {
		log.Printf("更新已读集合失败 (user=%s): %v\n", userId, err)
	}
}

func startUnreadPropagator(interval time.Duration) {
	go func() {
		for {
			start := time.Now()
			log.Println("开始执行未读状态上溯处理")

			userIds := getAllUserIds()
			if len(userIds) == 0 {
				log.Println("没有用户需要处理")
				time.Sleep(interval)
				continue
			}

			// 一次性加载所有目录数据，所有用户共享，避免 N×SCAN
			dirs, err := loadAllDirs()
			if err != nil {
				log.Printf("加载目录数据失败: %v\n", err)
				time.Sleep(interval)
				continue
			}

			for _, uid := range userIds {
				propagateUnreadForUser(uid, dirs)
			}
			log.Printf("完成未读状态处理：用户=%d，目录=%d，耗时 %s\n", len(userIds), len(dirs), time.Since(start))
			time.Sleep(interval)
		}
	}()
}

// 防止并发请求对同一目录触发重复缓存构建
var cacheBuildSf singleflight.Group

// ===== 用户访问时构建深层缓存（一层，设置1小时过期） =====
func buildCacheOnDemand(dir string) error {
	key := "dir:" + dir
	if exists, _ := redisClient.Exists(ctx, key).Result(); exists > 0 {
		return nil
	}
	// singleflight 让并发请求同一 dir 时只触发一次构建，其余等结果
	_, err, _ := cacheBuildSf.Do(dir, func() (any, error) {
		// 双检：等队列时可能已被前面那次构建写入
		if exists, _ := redisClient.Exists(ctx, key).Result(); exists > 0 {
			return nil, nil
		}
		return nil, cacheDirectoryRecursive(dir, 1, 1, 1*time.Hour, nil)
	})
	return err
}

// ===== 获取目录内容 =====
func listDirectory(dir string, readMap map[string]struct{}) ([]FileMeta, error) {
	names, err := redisClient.ZRange(ctx, "dir:"+dir, 0, -1).Result()
	if err != nil {
		return nil, err
	}
	if len(names) == 0 {
		return nil, nil
	}

	// 一次 MGet 拿完所有 filemeta，避免 N 次 GET 往返
	keys := make([]string, len(names))
	for i, name := range names {
		keys[i] = "filemeta:" + filepath.Join(dir, name)
	}
	values, err := redisClient.MGet(ctx, keys...).Result()
	if err != nil {
		return nil, err
	}

	metas := make([]FileMeta, 0, len(values))
	for i, v := range values {
		if v == nil {
			continue // 元信息缺失（可能正在刷新）
		}
		s, ok := v.(string)
		if !ok {
			continue
		}
		var meta FileMeta
		if err := json.Unmarshal([]byte(s), &meta); err != nil {
			log.Printf("反序列化 filemeta 失败 (%s): %v\n", keys[i], err)
			continue
		}
		if _, ok := readMap[meta.FilePath]; ok {
			meta.VisitStatus = 1
		}
		metas = append(metas, meta)
	}
	return metas, nil
}

// ===== HTTP 接口：预览目录 =====
func handleListDir(w http.ResponseWriter, r *http.Request) {
	queryDir := r.URL.Query().Get("path")
	userId := r.URL.Query().Get("userId")
	if queryDir == "" || userId == "" {
		http.Error(w, "path 或 userId 参数缺失", http.StatusBadRequest)
		return
	}

	// 自动构建缓存（如无）
	if err := buildCacheOnDemand(queryDir); err != nil {
		http.Error(w, "读取目录失败："+err.Error(), http.StatusInternalServerError)
		return
	}

	// 一次性加载用户已读集合，listDirectory 与下面的 allRead 检查共用同一份 map
	readKey := getUserReadKey(userId)
	readDirs, err := redisClient.SMembers(ctx, readKey).Result()
	if err != nil {
		http.Error(w, "读取已读集合失败："+err.Error(), http.StatusInternalServerError)
		return
	}
	readMap := make(map[string]struct{}, len(readDirs))
	for _, p := range readDirs {
		readMap[p] = struct{}{}
	}

	metas, err := listDirectory(queryDir, readMap)
	if err != nil {
		http.Error(w, "读取缓存失败："+err.Error(), http.StatusInternalServerError)
		return
	}

	// 复用 readMap 检查所有子目录是否已读，替代 N 次 SIsMember 往返
	hasDir := false
	allRead := true
	for _, meta := range metas {
		if meta.FileType != "dir" {
			continue
		}
		hasDir = true
		if _, ok := readMap[meta.FilePath]; !ok {
			allRead = false
			break
		}
	}

	// 仅当存在子目录且全部已读才把当前目录标为已读，避免空目录被误标
	if hasDir && allRead {
		redisClient.SAdd(ctx, readKey, queryDir)
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(Response{Code: 1, Data: metas, Msg: "成功"}); err != nil {
		log.Printf("写响应失败: %v\n", err)
	}
}

// ===== HTTP 接口 标记全部已读 =====
func handleMarkRead(w http.ResponseWriter, r *http.Request) {
	path := r.URL.Query().Get("path")
	userId := r.URL.Query().Get("userId")
	if userId == "" {
		http.Error(w, "userId 参数缺失", http.StatusBadRequest)
		return
	}

	readKey := getUserReadKey(userId)

	// 用 SCAN 替代 KEYS：KEYS 会阻塞 Redis 单线程，全量 keyspace 大时可能阻塞数秒
	pattern := "filemeta:*"
	if path != "" {
		pattern = "filemeta:" + escapeRedisGlob(path) + "*"
	}

	// path*  会误匹配兄弟前缀（如 path=/项目A 把 /项目AB 也匹配进来）
	// 用客户端二次过滤保证只命中 path 自身或 path/<sep> 子树
	sep := string(filepath.Separator)
	var members []any
	iter := redisClient.Scan(ctx, 0, pattern, 500).Iterator()
	for iter.Next(ctx) {
		key := iter.Val()
		pathPart := strings.TrimPrefix(key, "filemeta:")
		if pathPart == nasRoot {
			continue
		}
		if path != "" && pathPart != path && !strings.HasPrefix(pathPart, path+sep) {
			continue
		}
		members = append(members, pathPart)
	}
	if err := iter.Err(); err != nil {
		http.Error(w, "扫描文件元信息失败："+err.Error(), http.StatusInternalServerError)
		return
	}

	// 大集合分批 SAdd，避免单条命令过大撑爆客户端输出缓冲
	const sAddBatch = 1000
	for i := 0; i < len(members); i += sAddBatch {
		end := min(i+sAddBatch, len(members))
		if err := redisClient.SAdd(ctx, readKey, members[i:end]...).Err(); err != nil {
			http.Error(w, "标记已读失败："+err.Error(), http.StatusInternalServerError)
			return
		}
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(Response{Code: 1, Data: nil, Msg: "全部目录已标记为已读"}); err != nil {
		log.Printf("写响应失败: %v\n", err)
	}
}

// 转义 Redis glob 特殊字符（*, ?, [, ], \），用作 MATCH 模式中的字面前缀
// 重点是 \：Windows 路径含 \，未转义会被 Redis 当 escape 字符吃掉下一个字符
func escapeRedisGlob(s string) string {
	var b strings.Builder
	b.Grow(len(s))
	for i := range len(s) {
		c := s[i]
		if c == '*' || c == '?' || c == '[' || c == ']' || c == '\\' {
			b.WriteByte('\\')
		}
		b.WriteByte(c)
	}
	return b.String()
}

// ===== 启动 HTTP 服务 =====
func startHTTPServer() {
	http.HandleFunc("/list", handleListDir)
	http.HandleFunc("/markRead", handleMarkRead)

	log.Println("服务启动：http://localhost:8899/list?path=" + nasRoot)
	log.Fatal(http.ListenAndServe(":8899", nil))
}

func main() {
	initRedis()
	startCacheRefresher(nasRoot, 1*time.Hour, 7) // 每小时刷新缓存，缓存 7 层
	startUnreadPropagator(1 * time.Hour)         // 每小时处理一次未读回溯
	startHTTPServer()
}
