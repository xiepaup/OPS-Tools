package main

import "github.com/xiepaup/dbatools/redisTools/redisHealthy/lib/db"

import (
	"fmt"
	"os"
	"strings"
	"time"
)

/**
 *
 * Author  :  xiean
 * EMail   :  xiepaup@163.com
 * GitHub  :  https://github.com/xiepaup
 * Date    :  2018-08-03
 * Project :  dbatools
 **/


const MAX_QUEUE_LEN = 1000

func main() {
	stopChan := make(chan struct{}, 2)
	monitorChan := make(chan interface{}, MAX_QUEUE_LEN)
	r := db.NewRedis("127.0.0.1:31238", "redis@manage")

	redisBasicInfo(r)

	err := r.Monitor(monitorChan, stopChan)
	if err != nil {
		os.Exit(1)
	}
	go consumeCmds(monitorChan, stopChan)
	fmt.Println(fmt.Sprintf("do sleep ... wait queue done"))

	time.Sleep(time.Second * 1000)
}

func redisBasicInfo(r *db.RedisContext) {
	i, err := r.Info()
	if err != nil {
		fmt.Println(fmt.Sprintf("got an error from redis , %+v", err))
		os.Exit(2)
	}
	info := strings.Split(string(i), "\r\n")
	if len(info) <= 0 {
		return
	}

	infoInfo := make(map[string]interface{}, 0)

	for _, l := range info {
		if !strings.Contains(l, ":") || strings.HasPrefix(l, "#") || len(l) == 0 {
			continue
		}
		//#redis_version:2.8.17-t-v0.2
		kv := strings.Split(l, ":")
		infoInfo[kv[0]] = kv[1]
	}

	//fmt.Println(infoInfo)
	showBasicInfo(infoInfo)
}

func showBasicInfo(info map[string]interface{}) {
	fmt.Println(fmt.Sprintf(`
-------------------------------------------------
|version:%-40s|osversion:%-40s|
|uptime:%10s|clients:%5s|role:%6s|cur_qps:%6s|used_memory:%8s|rss_memory:%8s|
-------------------------------------------------`,
		info["redis_version"], info["os"],
		info["uptime_in_seconds"], info["connected_clients"], info["role"],
		info["instantaneous_ops_per_sec"],
		info["used_memory_human"], info["used_memory_peak_human"]))
}

func consumeCmds(mchan chan interface{}, schan chan struct{}) {
	for line := range mchan {
		//#1532945695.049181 [0 10.51.149.210:34992] "RPOP" "com.xiepaup.tendis.source.queue"
		if v, ok := line.(string); ok {
			fmt.Println(fmt.Sprintf("outer lib do parse cmd : %s", v))
		}
	}
}
