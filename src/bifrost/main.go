package main

import (
	bconfig "bifrost/config"
	blog "bifrost/logger"
	"bifrost/zsets"
	"bufio"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	jsoniter "github.com/json-iterator/go"
	"go.uber.org/zap"
	"io"
	"io/ioutil"
	"math/rand"
	"net/http"
	"net/http/pprof"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

//an lock for map operate
var sortSetListLock sync.RWMutex

//json cache lock
var cacheLock sync.RWMutex

//set name and sort list
var sortSetListMap map[string]sortSetInfoObj

//top json result cache map
var sortSetCacheMap map[string][]byte

//log helper
var logger *zap.Logger

//config
var config bconfig.ConfigInfo

//quit server
var quitFlagChan = make(chan int)

//type define
type (
	//sort set map info obj
	sortSetInfoObj struct {
		Key        string
		CreateTime int64
		SortSet    zsets.SortedSet
		SortType   int16 //todo
		Lock       *sync.RWMutex
		BinLogFile *os.File
	}

	//zsets json format
	zsetOutputFormat struct {
		Key   int64  `json:"key"`
		Score int64  `json:"score"`
		Data  string `json:"data"`
	}

	//http response format
	httpResponseFormat struct {
		Code int                 `json:"code"`
		Msg  string              `json:"message"`
		Data *[]zsetOutputFormat `json:"data"`
	}
)

//init the project
func init() {

	/*-----------------
		Cmd argv
	-----------------*/
	configPath := flag.String("c", "", "config file")
	flag.Parse()
	fmt.Println("load config file:" + *configPath)

	//load config
	config = bconfig.LoadConfig(*configPath)

	configJson, err := json.Marshal(config)
	if err != nil {
		fmt.Println("config file json error " + err.Error())
		os.Exit(4)
	}
	fmt.Println("config:" + string(configJson))

	/*-----------------
		Init Log
	-----------------*/
	logger = blog.InitLog(config)

	logger.Info("load config file:" + *configPath)
	logger.Info("log path:" + config.Log.Path)
	logger.Info("log level:" + config.Log.Level)
	logger.Info("log file keep day:" + strconv.Itoa(config.Log.Keep))
	logger.Info("log file backup :" + strconv.Itoa(config.Log.Backup))

	/*-----------------
		Signal process
	-----------------*/
	chanForSignal := make(chan os.Signal)
	//监听指定信号 ctrl+c kill
	signal.Notify(chanForSignal, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT, syscall.SIGUSR1, syscall.SIGUSR2)
	go func() {
		for s := range chanForSignal {
			switch s {
			case syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT:
				fmt.Println("signal kill", s)
				logger.Info("signal kill 15")
				quitFlagChan <- 1
			case syscall.SIGUSR1:
				fmt.Println("usr1", s)
				logger.Info("signal usr1")

			case syscall.SIGUSR2:
				fmt.Println("usr2", s)
				logger.Info("signal usr2")
			default:
				fmt.Println("other", s)
				logger.Info("signal " + s.String())
			}
		}
	}()

	/*-----------------
	Skip List And Cache map
	-----------------*/
	//init map for skiplist list
	sortSetListMap = make(map[string]sortSetInfoObj)

	//init cache map
	sortSetCacheMap = make(map[string][]byte)

	//recovery binlog
	scanRecoveryBinLog(config.App.Binlog)
}

func scanRecoveryBinLog(pathname string) {
	rd, err := ioutil.ReadDir(pathname)
	if err != nil {
		logger.Error("open binlog folder fail:" + err.Error())
		return
	}

	//scan the file by name and load binlog
	for _, fi := range rd {

		//check file ext
		if fi.IsDir() != false || !strings.HasSuffix(fi.Name(), ".bin") {
			continue
		}

		//load binlog
		fullPath := pathname + "/" + fi.Name()

		//ignore too short file name
		if len(fi.Name()) > 4 {
			//remove file ext for map key
			mapKey := fi.Name()[0 : len(fi.Name())-4]

			//load binlog
			recoverBinlogFile(mapKey, fullPath)
		}

	}

}

func recoverBinlogFile(mapKey string, fullPath string) int64 {

	logger.Info("load binlog:" + fullPath)

	//open the file
	file, err := os.OpenFile(fullPath, os.O_RDONLY, 0644)
	if err != nil {
		logger.Error("load binlog error:" + err.Error())
		return 0
	}

	//read by line
	rd := bufio.NewReader(file)
	loadCount := int64(0)

	//first is timestamp of create time
	line, err := rd.ReadString('\n')
	line = strings.Trim(line, "\n")

	//change create time
	createtime, err := strconv.ParseInt(line, 10, 64)
	if err != nil {
		createtime = 0
	}

	//create the skip list
	checkHaveMap(mapKey, createtime)

	for {
		line, err := rd.ReadString('\n')

		//loaded all or eof or error
		if err != nil || io.EOF == err {
			_ = file.Close()
			logger.Info("loaded binlog " + mapKey + " count:" + strconv.FormatInt(loadCount, 10))
			return loadCount
		}

		//decode format of binlog
		lineArray := strings.Split(line, "|$|")

		//bad format
		if len(lineArray) != 3 {
			logger.Error("error binlog format:" + line)
			continue
		}

		//transform key
		key, err := strconv.ParseInt(lineArray[0], 10, 64)
		if err != nil {
			logger.Error("key error format:" + line)
			continue
		}

		//transform score
		score, err := strconv.ParseInt(lineArray[1], 10, 64)
		if err != nil {
			logger.Error("score error format:" + line)
			continue
		}

		//transform data
		data := strings.Trim(lineArray[2], "\n")

		loadCount++
		incrScore(mapKey, key, score, data, false)
	}

}

//an function for http response format
func responseResult(code int, msg string, data *[]zsetOutputFormat) ([]byte, error) {
	var response httpResponseFormat
	var jsonHelper = jsoniter.ConfigCompatibleWithStandardLibrary

	response.Code = code
	response.Msg = msg
	response.Data = data

	return jsonHelper.Marshal(response)
}

//get the day of dawn timestamp
func getTimeFirstTimeStamp(timeObj time.Time) int64 {
	timeStr := timeObj.Format("2006-01-02")
	t, _ := time.Parse("2006-01-02", timeStr)
	timeNumber := t.Unix() - (8 * 3600)
	return timeNumber
}

//check the skip list is created
//not will create one
func checkHaveMap(mapKey string, timestamp int64) {

	sortSetListLock.Lock()

	//check the map already exist?
	if _, ok := sortSetListMap[mapKey]; !ok {

		logger.Info("create zset key:" + mapKey)

		//create an new zsets
		var infoObj sortSetInfoObj
		file, err := os.OpenFile(config.App.Binlog+"/"+mapKey+".bin", os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0644)
		if err != nil {
			logger.Error("open binlog error:" + err.Error())
		}

		//set sort set start time
		if timestamp == 0 {
			timestamp = getTimeFirstTimeStamp(time.Now())
			//first of file is timestamp
			_, _ = file.Write([]byte(strconv.FormatInt(timestamp, 10) + "\n"))
		} else {
			logger.Info("recovery timestamp:" + strconv.FormatInt(timestamp, 10))
		}

		infoObj.Key = mapKey
		infoObj.CreateTime = timestamp
		infoObj.SortSet = *zsets.New()
		infoObj.SortType = 0 //todo:type define
		infoObj.Lock = &sync.RWMutex{}
		infoObj.BinLogFile = file
		sortSetListMap[mapKey] = infoObj
	}
	sortSetListLock.Unlock()
	//todo create log consumer

}

func incrScore(mapKey string, key int64, score int64, data string, recordBinlog bool) {

	sortSetListLock.RLock()
	//sort set lock for expire key delete

	//increse score
	var mapObj = sortSetListMap[mapKey]

	//skip list lock
	mapObj.Lock.Lock()

	sc, ok := mapObj.SortSet.IncrBy(score, key)

	//not exist create one
	if sc == 0 && ok == nil {
		mapObj.SortSet.Set(score, key, data)
	}

	//write binlog
	if recordBinlog && mapObj.BinLogFile != nil {
		//content
		content := strconv.FormatInt(key, 10) + "|$|" + strconv.FormatInt(score, 10) + "|$|" + data

		//write
		_, err := mapObj.BinLogFile.Write([]byte(content + "\n"))
		if err != nil {
			logger.Error("write binlog fail:", zap.String("binlog", content))
		}
	}

	mapObj.Lock.Unlock()
	sortSetListLock.RUnlock()


}

func removeIndex(mapObj sortSetInfoObj)  {

	mapObj.Lock.Lock()
	mapKey := mapObj.Key

	//rename binlog
	timestamp := getTimeFirstTimeStamp(time.Unix(mapObj.CreateTime,0))
	folderPath := config.App.Binlog+"/"+strconv.FormatInt(timestamp,10)

	err := os.MkdirAll(folderPath,0755)
	if err != nil {
		logger.Error("mkdir error:"+err.Error())
	}

	err = os.Rename(config.App.Binlog+"/"+mapKey+".bin", folderPath+"/"+mapKey+".bin")
	if err != nil {
		logger.Error("move error:"+err.Error())
	}

	logger.Info("rename binlog:" + config.App.Binlog + "/" + mapKey + ".bin" + " to " + folderPath+"/"+mapKey+".bin")

	_ = mapObj.BinLogFile.Close()

	//list map delete
	delete(sortSetListMap, mapKey)

	mapObj.Lock.Unlock()

	//delete cache map
	cacheLock.Lock()
	delete(sortSetCacheMap, mapKey)
	cacheLock.Unlock()

}
func addTop(w http.ResponseWriter, r *http.Request) {
	parameter := r.PostForm

	//is test insert?
	test := r.URL.Query().Get("test")

	var mapKey string
	var key int64
	var score int64
	var data string

	//test mode for test
	if len(test) > 0 && test == "tal" {

		//debug mode
		mapKey = "test" + strconv.FormatInt(int64(rand.Intn(3000)), 10)
		key = int64(rand.Intn(500000))
		score = int64(rand.Intn(500000))
		data = "0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789"

	} else {

		//normal mode
		mapKey = parameter.Get("mapkey")
		key, _ = strconv.ParseInt(parameter.Get("key"), 10, 64)
		score, _ = strconv.ParseInt(parameter.Get("score"), 10, 64)
		data = parameter.Get("data")

		//check parameter
		if len(mapKey) == 0 {
			resp, _ := responseResult(-1, "parameter mapkey must fill", nil)
			_, _ = w.Write(resp)
			return
		}
		if len(data) > 500 {
			resp, _ := responseResult(-1, "parameter data too big", nil)
			_, _ = w.Write(resp)
			return
		}
		if key == 0 {
			resp, _ := responseResult(-1, "parameter key must fill", nil)
			_, _ = w.Write(resp)
			return
		}

		if score == 0 {
			resp, _ := responseResult(-1, "parameter score must fill", nil)
			_, _ = w.Write(resp)
			return
		}
	}

	//check sort set exist and create
	checkHaveMap(mapKey, 0)

	//incrScore
	incrScore(mapKey, key, score, data, true)
}

func getTop(w http.ResponseWriter, r *http.Request) {
	//prepare parameter
	parameter := r.URL.Query()

	//which skiplist to show
	var mapKey = parameter.Get("key")

	//check key
	if len(mapKey) == 0 {
		resp, _ := responseResult(-1, "parameter key must fill", nil)
		_, _ = w.Write(resp)
		return
	}

	//todo:limit
	/*
		//limit of key
		if len(parameter.Get("limit")) > 0 {
			limitInt, err := strconv.Atoi(parameter.Get("limit"))
			if (err == nil) {
				limit = limitInt
			} else {
				resp, _ := responseResult(-24, "parameter limit must int", nil)
				w.Write(resp)
			}
		}*/

	//lock the map for get cache
	cacheLock.RLock()

	//return result by cache json
	if result, ok := sortSetCacheMap[mapKey]; ok {
		cacheLock.RUnlock()
		_, _ = w.Write(result)
		return
	}
	cacheLock.RUnlock()

	//not found
	res, _ := responseResult(-2, "key not found", nil)
	_, _ = w.Write(res)
}

//for benchmark test
func testHello(w http.ResponseWriter, r *http.Request) {
	_, _ = w.Write([]byte("hello"))

}

func checkoutPath(){

	//mkdir binlog
	_ = os.MkdirAll(config.App.Binlog,0755)

	//binlog
	err := os.Chmod( config.App.Binlog,0755)
	if err!=nil {
		fmt.Println("check binlog permission path fail. path:"+config.App.Binlog)
		logger.Error("check binlog permission path fail. path:"+config.App.Binlog)
		os.Exit(23)
	}
}

func main() {
	defer logger.Sync()

	logger.Info("server start:" + config.Server.Host + ":" + config.Server.Port)

	//check the binlog path permission
	checkoutPath()

	//async json format result
	cycleTimer := time.NewTimer(time.Duration(config.App.Cachecycle) * time.Second)

	//cycle refresh json result cache
	go func(t *time.Timer) {
		for {
			<-t.C
			sortSetListLock.Lock()

			for mapKey, mapVal := range sortSetListMap {
				//logger.Error("key:" + mapKey + "val:" + mapVal)
				var mapObj = mapVal

				//cycle clean old skip list
				if mapObj.CreateTime+int64(config.App.Keepday*24*3600) < time.Now().Unix() {
					logger.Info("expire key:" + mapKey)
					//remove skip list
					removeIndex(mapObj)
				}

				//make cache list
				index := 0
				mapObj.Lock.RLock()
				result := make([]zsetOutputFormat, mapObj.SortSet.Length())
				mapObj.SortSet.RevRange(0, -1, func(score int64, k int64, data interface{}) {

					result[index].Key = k
					result[index].Score = score
					result[index].Data = data.(string)
					index++

				})
				mapObj.Lock.RUnlock()

				//cache the top result
				res, err := responseResult(0, "ok", &result)
				if err == nil {
					cacheLock.Lock()
					sortSetCacheMap[mapKey] = res
					cacheLock.Unlock()
				}

			}
			sortSetListLock.Unlock()

			//reset timer
			t.Reset(time.Duration(config.App.Cachecycle) * time.Second)
		}
	}(cycleTimer)

	mux := http.NewServeMux()

	//register the api
	mux.Handle("/debug/pprof/", http.HandlerFunc(pprof.Index))
	mux.Handle("/debug/pprof/cmdline", http.HandlerFunc(pprof.Cmdline))
	mux.Handle("/debug/pprof/profile", http.HandlerFunc(pprof.Profile))
	mux.Handle("/debug/pprof/symbol", http.HandlerFunc(pprof.Symbol))
	mux.Handle("/debug/pprof/trace", http.HandlerFunc(pprof.Trace))
	mux.HandleFunc("/addtop", addTop)
	mux.HandleFunc("/gettop", getTop)
	mux.HandleFunc("/testhello", testHello)

	//todo:manage index api delindex

	// start server
	server := &http.Server{Addr: config.Server.Host + ":" + config.Server.Port, Handler: mux}

	go func() {
		if err := server.ListenAndServe(); err != nil {
			logger.Fatal("Listen Server shutdown: " + err.Error())
		}
	}()

	//flag for service shutdown
	<-quitFlagChan

	//close cycle task
	cycleTimer.Stop()

	// gracefully stop server by 60 sec
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	if err := server.Shutdown(ctx); err != nil {
		logger.Error(err.Error())
	}
	logger.Info("server stopped")
}
