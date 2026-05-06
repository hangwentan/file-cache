package main

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
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
	Code int         `json:"code"`
	Data interface{} `json:"data"`
	Msg  string      `json:"msg"`
}

var nasRoot = "/opt/devNas2"
var fileNasPrefix = "/homes/项目主动备份"

func getUserReadKey(userId string) string {
	return "read_dirs:" + userId
}

func getAllUserIds() []string {
	var ids []string
	iter := redisClient.Scan(ctx, 0, "read_dirs:*", 0).Iterator()
	for iter.Next(ctx) {
		key := iter.Val() // e.g., read_dirs:123
		userId := strings.TrimPrefix(key, "read_dirs:")
		ids = append(ids, userId)
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
func cacheDirectoryRecursive(dir string, currentLevel, maxLevel int, ttl time.Duration) error {
	if currentLevel > maxLevel {
		return nil
	}

	entries, err := os.ReadDir(dir)
	if err != nil {
		return err
	}

	var zItems []redis.Z

	for _, entry := range entries {
		name := entry.Name()
		if name == "#recycle" {
			continue
		}

		fullPath := filepath.Join(dir, name)
		info, err := entry.Info()
		if err != nil {
			log.Printf("跳过无法读取 info 的项: %s，错误: %v\n", fullPath, err)
			continue
		}

		// 判断类型更准确
		isDir := info.IsDir()
		fileType := "file"
		if isDir {
			fileType = "dir"
		}

		// 子项数量，仅针对目录
		childrenCount := 0
		if isDir {
			childEntries, err := os.ReadDir(fullPath)
			if err == nil {
				childrenCount = len(childEntries)
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

		// 写入 Redis
		data, _ := json.Marshal(meta)
		redisClient.Set(ctx, "filemeta:"+fullPath, data, ttl)
		zItems = append(zItems, redis.Z{Score: 0, Member: name})

		// 递归目录
		if isDir {
			cacheDirectoryRecursive(fullPath, currentLevel+1, maxLevel, ttl)
		}
	}

	// 写入目录下的文件/子目录名列表
	redisClient.ZAdd(ctx, "dir:"+dir, zItems...)
	if ttl > 0 {
		redisClient.Expire(ctx, "dir:"+dir, ttl)
	}
	return nil
}

// ===== 定时刷新缓存（不设过期时间） =====
func startCacheRefresher(root string, interval time.Duration, maxDepth int) {
	go func() {
		for {
			log.Println("刷新缓存中...")
			err := cacheDirectoryRecursive(root, 1, maxDepth, 0)
			if err != nil {
				log.Println("刷新缓存失败:", err)
			}
			time.Sleep(interval)
		}
	}()
}

func propagateUnreadForUser(userId string) {
	readKey := getUserReadKey(userId)

	// 获取用户已读目录集合
	readDirs, _ := redisClient.SMembers(ctx, readKey).Result()
	readSet := make(map[string]struct{}, len(readDirs))
	for _, d := range readDirs {
		readSet[d] = struct{}{}
	}

	// 遍历所有缓存的目录
	iter := redisClient.Scan(ctx, 0, "dir:*", 0).Iterator()
	for iter.Next(ctx) {
		dirKey := iter.Val() // e.g., dir:/opt/devNas2/项目1
		dirPath := strings.TrimPrefix(dirKey, "dir:")
		if dirPath == nasRoot {
			continue // 不处理根目录
		}

		names, err := redisClient.ZRange(ctx, dirKey, 0, -1).Result()
		if err != nil {
			continue
		}

		// 检查子项中是否存在未读
		hasUnread := false
		for _, name := range names {
			childPath := filepath.Join(dirPath, name)
			if _, ok := readSet[childPath]; !ok {
				hasUnread = true
				break
			}
		}

		if hasUnread {
			// 这个目录也应该标记为“未读”
			delete(readSet, dirPath)

			// 向上递归删除父目录标记
			parent := filepath.Dir(dirPath)
			for parent != nasRoot && parent != "." && parent != "/" {
				delete(readSet, parent)
				parent = filepath.Dir(parent)
			}
			// 也处理 nasRoot 本身
			delete(readSet, nasRoot)
		}
	}

	// 重置用户的已读集合为清洗后的结果
	redisClient.Del(ctx, readKey)
	if len(readSet) > 0 {
		cleaned := make([]interface{}, 0, len(readSet))
		for k := range readSet {
			cleaned = append(cleaned, k)
		}
		redisClient.SAdd(ctx, readKey, cleaned...)
	}
}

func startUnreadPropagator(interval time.Duration) {
	go func() {
		for {
			log.Println("开始执行未读状态上溯处理")
			userIds := getAllUserIds()
			for _, uid := range userIds {
				propagateUnreadForUser(uid)
			}
			log.Println("完成未读状态处理")
			time.Sleep(interval)
		}
	}()
}

// ===== 用户访问时构建深层缓存（一层，设置1小时过期） =====
func buildCacheOnDemand(dir string) error {
	key := "dir:" + dir
	exists, _ := redisClient.Exists(ctx, key).Result()
	if exists > 0 {
		return nil
	}
	return cacheDirectoryRecursive(dir, 1, 1, 1*time.Hour)
}

// ===== 获取目录内容 =====
func listDirectory(dir string, userId string) ([]FileMeta, error) {
	names, err := redisClient.ZRange(ctx, "dir:"+dir, 0, -1).Result()
	if err != nil {
		return nil, err
	}

	// 获取当前用户已读目录
	readKey := getUserReadKey(userId)
	readDirs, _ := redisClient.SMembers(ctx, readKey).Result()
	readMap := make(map[string]struct{}, len(readDirs))
	for _, p := range readDirs {
		readMap[p] = struct{}{}
	}

	var metas []FileMeta
	for _, name := range names {
		fullPath := filepath.Join(dir, name)
		jsonData, err := redisClient.Get(ctx, "filemeta:"+fullPath).Result()
		if err != nil {
			continue
		}
		var meta FileMeta
		json.Unmarshal([]byte(jsonData), &meta)

		if _, ok := readMap[fullPath]; ok {
			meta.VisitStatus = 1
		}

		metas = append(metas, meta)
	}
	return metas, nil
}

func clearUserReadStatus(userId string) {
	redisClient.Del(ctx, getUserReadKey(userId))
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
	err := buildCacheOnDemand(queryDir)
	if err != nil {
		http.Error(w, "读取目录失败："+err.Error(), http.StatusInternalServerError)
		return
	}

	// ✅ 记录为用户已读
	//readKey := getUserReadKey(userId)
	//redisClient.SAdd(ctx, readKey, queryDir)

	metas, err := listDirectory(queryDir, userId)
	if err != nil {
		http.Error(w, "读取缓存失败："+err.Error(), http.StatusInternalServerError)
		return
	}

	// 检查所有子文件/目录是否已读
	readKey := getUserReadKey(userId)
	allRead := true
	for _, meta := range metas {
		if meta.FileType != "dir" {
			continue // 跳过文件，只检查子目录
		}
		isRead, _ := redisClient.SIsMember(ctx, readKey, meta.FilePath).Result()
		if !isRead {
			allRead = false
			break
		}
	}

	// 如果全部子项都已读，则当前目录也标记为已读
	if allRead {
		redisClient.SAdd(ctx, readKey, queryDir)
	}

	w.Header().Set("Content-Type", "application/json")
	err = json.NewEncoder(w).Encode(Response{Code: 1, Data: metas, Msg: "成功"})
	if err != nil {
		fmt.Println(err.Error())
	}
}

// ===== HTTP 接口 标记全部已读 =====
func handleMarkRead(w http.ResponseWriter, r *http.Request) {
	path := r.URL.Query().Get("path")
	// 获取用户ID
	userId := r.URL.Query().Get("userId")
	if userId == "" {
		http.Error(w, "userId 参数缺失", http.StatusBadRequest)
		return
	}

	readKey := getUserReadKey(userId)

	// 获取 filemeta:* 前缀的所有键
	fileMetaPath := "filemeta:*"
	if path != "" {
		fileMetaPath = "filemeta:" + path + "*"
	}
	// 获取所有匹配的键
	keys, err := redisClient.Keys(ctx, fileMetaPath).Result()
	if err != nil {
		http.Error(w, "获取文件元信息失败："+err.Error(), http.StatusInternalServerError)
		return
	}
	// 遍历所有匹配的键 加入到已读集合中
	for _, key := range keys {
		// 提取路径部分
		pathPart := strings.TrimPrefix(key, "filemeta:")
		if pathPart == nasRoot {
			continue // 不处理根目录
		}
		// 将目录加入已读集合
		redisClient.SAdd(ctx, readKey, pathPart)

	}

	w.Header().Set("Content-Type", "application/json")
	err = json.NewEncoder(w).Encode(Response{Code: 1, Data: nil, Msg: "全部目录已标记为已读"})
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
	startCacheRefresher(nasRoot, 1*time.Hour, 7) // 每小时刷新缓存，缓存5层
	startUnreadPropagator(1 * time.Hour)         // 每30分钟处理一次未读回溯
	startHTTPServer()
}
