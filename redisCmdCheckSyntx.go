package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/gomodule/redigo/redis"
	log "github.com/sirupsen/logrus"
	"io"
	"os"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"
)

var (
	debug        bool
	redis_a_addr string
	redis_a_pswd string
	redis_b_addr string
	redis_b_pswd string
	cmdsFile     string
	TestCmds     []*Command
)

func init() {
	TestCmds = []*Command{}
	flag.StringVar(&redis_a_addr, "a_addr", "0.0.0.0:6379", "redis_a_addr")
	flag.StringVar(&redis_a_pswd, "a_pswd", "test", "redis_a_password")
	flag.StringVar(&redis_b_addr, "b_addr", "0.0.0.0:6389", "redis_b_addr")
	flag.StringVar(&redis_b_pswd, "b_pswd", "test", "redis_b_password")
	flag.BoolVar(&debug, "debug", false, "show debug info")
	flag.StringVar(&cmdsFile, "cmds", "./cmds.json", "files contains test cmds...")
	flag.Parse()

	if debug {
		log.SetLevel(log.DebugLevel)
	}
	loadCmdsForFile()
}

func main() {
	AReceiveChan := make(chan *Command, 0)
	BReceiveChan := make(chan *Command, 0)

	AResultChan := make(chan *RedisResult, 0)
	BResultChan := make(chan *RedisResult, 0)
	stopChan := make(chan struct{}, 0)

	aExe := NewRedisExecute(redis_a_addr, redis_a_pswd)
	bExe := NewRedisExecute(redis_b_addr, redis_b_pswd)

	go aExe.Start(AReceiveChan, AResultChan)
	go bExe.Start(BReceiveChan, BResultChan)

	wg := &sync.WaitGroup{}

	wg.Add(2)
	go SendCmds(AReceiveChan, BReceiveChan, stopChan, wg)
	go CompareResult(AResultChan, BResultChan, stopChan, wg)

	wg.Wait()
	log.Infof("function compare test done !")
}

func SendCmds(ar, br chan *Command, st chan struct{}, wg *sync.WaitGroup) {
	log.Infof("start sendcmds job ...")
	//NOTE(vitoxie) generate commands  use outsider file...
	//cmds := []*Command{&Command{Cmd: "SET", Args: []interface{}{"V1", "1"}}}
	defer wg.Done()
	for _, cmd := range TestCmds {
		if len(cmd.Tags) > 0 {
			for _, tag := range cmd.Tags {
				tag = strings.ToUpper(tag)
				if strings.HasPrefix(tag, TAG_PREFIX_SLEEP) {
					slepp_args, _ := strconv.Atoi(strings.Split(tag, "_")[1])
					time.Sleep(time.Second * time.Duration(slepp_args))
				}
			}
		}
		ar <- cmd
		br <- cmd
	}
	st <- struct{}{}
	log.Infof("Job commands send done !")
}

func CompareResult(ar, br chan *RedisResult, st chan struct{}, wg *sync.WaitGroup) {
	log.Infof("start compare job ...")
	defer wg.Done()
	var jobErr int
	var jobSucc int

	for {
		select {
		case resulta := <-ar:
			log.Debugf("receive result : %v", resulta)
			resultb := <-br
			log.Debugf("receive result : %v", resultb)

			if resulta.Compare(resultb) {
				jobSucc++
				log.Infof("Compare{%-2d:%s%v}\t%s\t<=>\t%s\t PASS", resulta.CmdSeq, resulta.Cmd, resulta.Args, resulta.ToString(), resultb.ToString())
			} else {
				jobErr++
				log.Warnf("Compare{%-2d%s:%v}\t%s\t<=>\t%s\t<<<Failed>>>", resulta.CmdSeq, resulta.Cmd, resulta.Args, resulta.ToString(), resultb.ToString())
			}
		case <-st:
			goto TAG_END
		}
	}
TAG_END:
	log.Infof("Job compare done !")
	log.Infof("total %3d compare failed , %3d compare pass in file : %s", jobErr, jobSucc, cmdsFile)
}

type RedisExecute struct {
	addr     string
	password string
	db       redis.Conn
}

func NewRedisExecute(a, p string) *RedisExecute {
	return &RedisExecute{
		addr:     a,
		password: p,
		db:       ConnRedis(a, p),
	}
}

func (this *RedisExecute) Start(cmdChan chan *Command, resutlChan chan *RedisResult) {
	log.Infof("%s start ...", this.addr)
	for {
		select {
		case cmdinfo := <-cmdChan:
			rst := &RedisResult{Addr: this.addr, Cmd: cmdinfo.Cmd, Args: cmdinfo.Args, CmdSeq: cmdinfo.Seq}
			rst.RTp, rst.Str, rst.Err = this.RedisDoCommand(cmdinfo.Cmd, cmdinfo.Args)
			resutlChan <- rst
			log.Debugf("%s sendback result %v", this.addr, rst)
		}
	}
}

func (this *RedisExecute) RedisDoCommand(cmd string, args []interface{}) (string, string, error) {
	//Do(commandName string, args ...interface{}) (reply interface{}, err error)
	log.Debugf("%s execute custom command : %s:%v", this.addr, cmd, args)
	///s := make([]interface{}, len(args))
	///for i, v := range args {
	///	s[i] = v
	///}
	replay, err := this.db.Do(cmd, args...)
	if err != nil {
		log.Errorf("%s Do command %s:%v failed : %+v", this.addr, cmd, args, err)
		return "", "", err
	}
	if replay == nil {
		return "NULL", "", nil
	}
	var tmpstr string
	log.Debugf("got reply : %s;%#v", reflect.TypeOf(replay).String(), replay)
	switch replay.(type) {
	case []interface{}:
		rep, _ := redis.Strings(replay, nil)
		for _, s := range rep {
			if len(s) == 0 {
				continue
			}
			tmpstr = fmt.Sprintf("%s\r\n%s", tmpstr, s)
		}
	case int64:
		t, _ := redis.Int64(replay, nil)
		tmpstr = fmt.Sprintf("%v", t)
	case []uint8:
		t, _ := redis.Ints(replay, nil)
		tmpstr = fmt.Sprintf("%v", t)
	case bool:
		t, _ := redis.Bool(replay, nil)
		tmpstr = fmt.Sprintf("%v", t)
	default:
		tmpstr, _ = redis.String(replay, nil)
	}
	//log.Debugf("%s after execute command %s:%v ; result : %s:%+v", this.addr, cmd, args, tmpstr, err)
	return reflect.TypeOf(replay).String(), tmpstr, nil
}

//NOTE(vitoxie) global_redis_conn_method
func ConnRedis(addr, pass string) redis.Conn {

	options := []redis.DialOption{
		redis.DialConnectTimeout(5 * time.Second),
		redis.DialReadTimeout(5 * time.Second),
		redis.DialWriteTimeout(5 * time.Second),
		redis.DialPassword(pass),
	}
	addr = fmt.Sprintf("redis://%s", addr)
	redisdb, err := redis.DialURL(addr, options...)
	if err != nil {
		log.Errorf("conn %s err : %+v", addr, err)
	}
	return redisdb
}

func loadCmdsForFile() {
	fh, err := os.Open(cmdsFile)
	if err != nil {
		log.Errorf("open cmds file failed : %+v", err)
		return
	}
	fread := bufio.NewReader(fh)
	fcontent := make([]byte, 0)

	for {
		byteLine, _, err := fread.ReadLine()
		if err != nil {
			if err == io.EOF {
				break
			}
			log.Errorf("read cmds file failed : %+v", err)
			return
		}
		for _, b := range byteLine {
			fcontent = append(fcontent, b)
		}
	}

	err = json.Unmarshal(fcontent, &TestCmds)
	if err != nil {
		log.Errorf("marsh cmds contains failed : %+v", err)
		return
	}
}

const (
	TAG_PREFIX_SLEEP = "SLEEP_"
)

type Command struct {
	Seq  int           `json:"seq"`
	Cmd  string        `json:"cmd"`
	Args []interface{} `json:"args"`
	Tags []string      `json:"tags"`
}

func (this *Command) ToString() {
	log.Infof("commands : %s|%v", this.Cmd, this.Args)
}

type RedisResult struct {
	CmdSeq int
	Addr   string
	Cmd    string
	Args   []interface{}
	RTp    string
	Str    string
	Err    error
}

func (this *RedisResult) Compare(r *RedisResult) bool {
	if this.Cmd != r.Cmd {
		return false
	}
	if fmt.Sprintf("%v", this.Args) != fmt.Sprintf("%v", r.Args) {
		return false
	}
	if this.RTp != r.RTp {
		return false
	}
	if this.Str != r.Str {
		return false
	}
	if this.Err != nil && r.Err != nil {
		return true
	}
	return true
}

func (this *RedisResult) ToString() string {
	//return fmt.Sprintf("%s|{%s %v}|%s:%v|%v", this.Addr, this.Cmd, this.Args, this.RTp, this.Str, this.Err)
	return fmt.Sprintf("|%s|%s:%v|%v|", this.Addr, this.RTp, this.Str, this.Err)
}


/*cmd.json.sample
[
        {"seq":1,   "cmd":"SADD","args":["set_vito_test1","akey"] },
        {"seq":2,   "cmd":"SADD","args":["set_vito_test1","bkey"] }
]

*/
