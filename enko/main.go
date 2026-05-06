package main

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"github.com/redis/go-redis/v9"
	"net/http"
	"os"
	"strings"
	"time"
)

var ctx = context.Background()
var rdb *redis.Client

func InitRedis() {
	rdb = redis.NewClient(&redis.Options{
		Addr:     "192.168.1.122:32768", // 修改为你的 Redis 地址
		Password: "",                    // 没有密码可以为空
		DB:       0,                     // 默认 DB
	})
}

func GetCache(key string) (string, error) {
	val, err := rdb.Get(ctx, key).Result()
	if err == redis.Nil {
		return "", nil // 缓存未命中
	}
	return val, err
}

func SetCache(key string, value interface{}, expiration time.Duration) error {
	b, err := json.Marshal(value)
	if err != nil {
		return err
	}
	return rdb.Set(ctx, key, b, expiration).Err()
}

const (
	//systemPath = "D:\\sugar\\project\\went-project"
	systemPath  = "/opt/devNas/"
	systemPath2 = "/opt/devNas2"
	nasPath     = "/homes/"
	nasPath2    = "/homes/项目主动备份"
)

type ReFile struct {
	ParentPath  string
	Sign        string              `json:"sign"`
	FilePath    string              `json:"filePath"`
	FileNasPath string              `json:"fileNasPath"`
	FileType    string              `json:"fileType"`
	FileName    string              `json:"fileName"`
	Son         []*ReFile           `json:"son"`
	Level       int                 `json:"level"`
	FileTypes   map[string]struct{} `json:"-"`
	Folders     map[string]struct{} `json:"-"`
}

type Paths struct {
	UserPath string `json:"userPath"`
	Path     string `json:"path"`
}
type Path struct {
	Paths []Paths `json:"paths"`
}

type Response struct {
	Code int         `json:"code"`
	Data interface{} `json:"data"`
	Msg  string      `json:"msg"`
}

func GetAllFile(pathname, nasPath string, s, root *ReFile, level int) error {
	rd, err := os.ReadDir(pathname)
	if err != nil {
		fmt.Println("read dir fail:", err)
		return err
	}
	for _, fi := range rd {
		if fi.Name() == "#recycle" {
			continue
		}
		if fi.IsDir() {
			if fi.Name() == "@eaDir" {
				continue
			}
			s1 := &ReFile{
				Sign:        MD5(pathname + "/" + fi.Name()),
				ParentPath:  pathname,
				FilePath:    pathname + "/" + fi.Name(),
				FileNasPath: nasPath + "/" + fi.Name(),
				FileType:    "dir",
				FileName:    fi.Name(),
				Level:       level,
				Son:         make([]*ReFile, 0),
				FileTypes:   make(map[string]struct{}),
				Folders:     make(map[string]struct{}),
			}
			var tempRoot *ReFile
			if root == nil {
				tempRoot = s1
			} else {
				root.Folders[fi.Name()] = struct{}{}
				tempRoot = root
			}
			err = GetAllFile(s1.FilePath, s1.FileNasPath, s1, tempRoot, level+1)
			s.Son = append(s.Son, s1)
			if err != nil {
				fmt.Println("read dir fail:", err)
				return err
			}
		} else {
			fileArrStr := strings.Split(fi.Name(), ".")
			var fileType string
			if len(fileArrStr) > 1 {
				fileType = fileArrStr[len(fileArrStr)-1]
			}
			s1 := &ReFile{
				Sign:        MD5(pathname + "/" + fi.Name()),
				ParentPath:  pathname,
				FilePath:    pathname + "/" + fi.Name(),
				FileNasPath: nasPath + "/" + fi.Name(),
				FileType:    fileType,
				FileName:    fi.Name(),
				Level:       level,
				Son:         make([]*ReFile, 0),
			}
			if root != nil {
				if fileType == "" {
					root.FileTypes["unknown"] = struct{}{}
				} else {
					root.FileTypes[fileType] = struct{}{}
				}
			}
			s.Son = append(s.Son, s1)
		}
	}
	return nil
}

func MD5(v string) string {
	d := []byte(v)
	m := md5.New()
	m.Write(d)
	return hex.EncodeToString(m.Sum(nil))
}

func CheckPath(userPath, path string, s *ReFile) {
	rd, err := os.ReadDir(systemPath + userPath)
	if err != nil {
		return
	}
	for _, fi := range rd {
		if fi.IsDir() {
			_ = GetAllFile(systemPath+userPath+fi.Name()+"/"+path, nasPath+userPath+fi.Name()+"/"+path, s, nil, 0)
		}
	}
	return
}

func GetAllFileList(rw http.ResponseWriter, req *http.Request) {
	cacheKey := "nas:all_file_list"
	if cacheStr, err := GetCache(cacheKey); err == nil && cacheStr != "" {
		var cachedData []*ReFile
		_ = json.Unmarshal([]byte(cacheStr), &cachedData)
		_ = json.NewEncoder(rw).Encode(Response{Code: 1, Data: cachedData, Msg: "缓存命中"})
		return
	}

	reFile := &ReFile{Son: make([]*ReFile, 0), FileTypes: make(map[string]struct{}), Folders: make(map[string]struct{})}
	_ = GetAllFile(systemPath2, nasPath2, reFile, nil, 0)

	_ = SetCache(cacheKey, reFile.Son, 10*time.Minute) // 缓存 5 分钟

	err := json.NewEncoder(rw).Encode(Response{Code: 1, Data: reFile.Son, Msg: "成功"})
	if err != nil {
		fmt.Println(err.Error())
	}
}

func GetFilePath(rw http.ResponseWriter, req *http.Request) {
	var param Path
	err := json.NewDecoder(req.Body).Decode(&param)
	if err != nil {
		json.NewEncoder(rw).Encode(Response{
			Code: 7,
			Msg:  "请求参数异常..",
		})
		return
	}
	reFile := &ReFile{
		Sign:        "",
		FilePath:    "",
		FileNasPath: "",
		FileType:    "",
		FileName:    "",
		Son:         make([]*ReFile, 0),
		FileTypes:   make(map[string]struct{}),
		Folders:     make(map[string]struct{}),
	}
	for _, v := range param.Paths {
		CheckPath(v.UserPath, v.Path, reFile)
	}

	checkFileRule := map[string]map[string][][]string{
		"客户资料": {
			"fileNeed":     make([][]string, 0),
			"folderNeed":   make([][]string, 0),
			"fileSelect":   make([][]string, 0),
			"folderSelect": make([][]string, 0),
		},
		"项目说明": {
			"fileNeed":     make([][]string, 0),
			"folderNeed":   make([][]string, 0),
			"fileSelect":   make([][]string, 0),
			"folderSelect": make([][]string, 0),
		},
		"素材": {
			"fileNeed":     make([][]string, 0),
			"folderNeed":   make([][]string, 0),
			"fileSelect":   make([][]string, 0),
			"folderSelect": make([][]string, 0),
		},
		"过程文件": {
			"fileNeed": make([][]string, 0),
			"folderNeed": [][]string{
				{
					"links",
				},
			},
			"fileSelect": [][]string{
				{
					"psd", "ai",
				},
			},
			"folderSelect": make([][]string, 0),
		},
		"终版文件": {
			"fileNeed": make([][]string, 0),
			"folderNeed": [][]string{
				{
					"links",
				},
			},
			"fileSelect": [][]string{
				{
					"psd", "ai",
				},
			},
			"folderSelect": make([][]string, 0),
		},
		"制作文件": {
			"fileNeed": make([][]string, 0),
			"folderNeed": [][]string{
				{
					"links",
				},
			},
			"fileSelect": [][]string{
				{
					"psd", "ai",
				},
			},
			"folderSelect": make([][]string, 0),
		},
	}
	checkFile := [][]string{
		{
			"客户资料", "0",
		}, {
			"项目说明", "0",
		}, {
			"素材", "0",
		}, {
			"过程文件", "0",
		}, {
			"终版文件", "0",
		}, {
			"制作文件", "0",
		},
	}
	for _, v := range reFile.Son {
	CheckFile:
		for ke, va := range checkFile {
			if v.FileName == va[0] && va[1] == "0" {
				if len(v.FileTypes) < 1 {
					break
				}
				//文件必有
				if len(checkFileRule[va[0]]["fileNeed"]) > 0 {
					for _, val := range checkFileRule[va[0]]["fileNeed"][0] {
						_, ok := v.FileTypes[val]
						if !ok {
							break CheckFile
						}
					}
				}
				//文件多选1
				if len(checkFileRule[va[0]]["fileSelect"]) > 0 {
					for _, val := range checkFileRule[va[0]]["fileSelect"] {
						var tempCheckData int
						for _, val1 := range val {
							_, ok := v.FileTypes[val1]
							if ok {
								tempCheckData = 1
								break
							}
						}
						if tempCheckData == 0 {
							break CheckFile
						}
					}
				}
				//文件夹必有
				if len(checkFileRule[va[0]]["folderNeed"]) > 0 {
					for _, val := range checkFileRule[va[0]]["folderNeed"][0] {
						_, ok := v.FileTypes[val]
						if !ok {
							break CheckFile
						}
					}
				}
				//文件夹多选1
				if len(checkFileRule[va[0]]["folderSelect"]) > 0 {
					for _, val := range checkFileRule[va[0]]["folderSelect"] {
						var tempCheckData int
						for _, val1 := range val {
							_, ok := v.FileTypes[val1]
							if ok {
								tempCheckData = 1
								break
							}
						}
						if tempCheckData == 0 {
							break CheckFile
						}
					}
				}

				checkFile[ke][1] = "1"
			}
		}
	}

	type TempReData struct {
		Path      []*ReFile  `json:"path"`
		CheckFile [][]string `json:"checkFile"`
	}
	reData := TempReData{
		Path:      reFile.Son,
		CheckFile: checkFile,
	}
	err = json.NewEncoder(rw).Encode(Response{
		Code: 1,
		Data: reData,
		Msg:  "成功",
	})
	if err != nil {
		fmt.Println(err.Error())
		return
	}

}

func StartCacheAutoRefresh() {
	ticker := time.NewTicker(10 * time.Minute)
	for {
		refreshAllFileListCache()
		<-ticker.C
	}
}

func refreshAllFileListCache() {
	reFile := &ReFile{
		Son:       make([]*ReFile, 0),
		FileTypes: make(map[string]struct{}),
		Folders:   make(map[string]struct{}),
	}
	err := GetAllFile(systemPath2, nasPath2, reFile, nil, 0)
	if err == nil {
		_ = SetCache("nas:all_file_list", reFile.Son, 10*time.Minute)
		fmt.Println("刷新 all_file_list 缓存成功")
	} else {
		fmt.Println("刷新 all_file_list 缓存失败:", err.Error())
	}
}

func main() {
	InitRedis()
	// 启动定时缓存刷新
	go StartCacheAutoRefresh()

	http.HandleFunc("/getFilePath", GetFilePath)
	http.HandleFunc("/getAllFileList", GetAllFileList)

	http.ListenAndServe(":8899", nil)
}
