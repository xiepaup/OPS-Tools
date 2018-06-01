package aof

import (
	"os"
	"errors"
	"bufio"
	"io"
	"fmt"
	"strconv"
)

/*
* PROJECT NAME : redisTools
* AUTH : vitoxie
* DATE : 2018/6/1 15:19
* VERSION :  1.1.1
* EMAIL : xiepaup@163.com
*/

type AOFContext struct {
	AofHandler  *os.File
	SimpleKeys  map[int]*RedisSimpleKeys
	AOFFileName string
	TopNKeys    int
	CurrentDb   int
}

func NewAOFContext(f string) (*AOFContext, error) {
	_, err := os.Stat(f)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, errors.New("File Do Not Exist")
		}
		return nil, err
	} else {
		fh, err := os.Open(f)
		if err != nil {
			return nil, errors.New("Open File Error")
		}
		
		return &AOFContext{
			AofHandler:fh,
			AOFFileName:f,
			SimpleKeys:&RedisSimpleKeys{
				Db:0,
				StringKeys:make(map[string]StringMap),
				ListKeys:make(map[string]ListMap),
				HashKeys:make(map[string]HashMap),
				SetKeys:make(map[string]SetMap),
				ZSetMap:make(map[string]ZSetMap),

			},
		}, nil
	}
}

func (this *AOFContext) setTopN(n int) {
	if n < 1 {
		n = 1
	}
	this.TopNKeys = n
}

func (this *AOFContext) GetBigestTopKey(top int) () {
	this.setTopN(top)
	bufReader := bufio.NewReader(this.AofHandler)
	defer this.AofHandler.Close()
	this.loadAof2Memory(bufReader)
}

func (this *AOFContext) ParseAOF2Command() {
	bufReader := bufio.NewReader(this.AofHandler)
	defer this.AofHandler.Close()
	this.loadAof2Memory(bufReader)
}

func (this *AOFContext) loadAof2Memory(bufReader *bufio.Reader) ([]string) {
	for {
		var cmdStr string
		line, _, err := bufReader.ReadLine()
		if err != nil {
			if err == io.EOF {
				break
			} else {
				panic(fmt.Sprintf("read file %s error : %v", this.AOFFileName, err))
			}
		}

		if line[0] == '*' {
			lineNextCnt, err := strconv.Atoi(string(line[1:]))
			for i := 0; i < lineNextCnt * 2; i++ {
				line, _, err = bufReader.ReadLine()
				if err != nil {
					if err != io.EOF {
						panic(fmt.Sprintf("read file %s error : %v", this.AOFFileName, err))
					}
				}
				if line[0] != '$' {
					cmdStr = fmt.Sprintf("%s %s", cmdStr, string(line))
				}
			}
			fmt.Println(cmdStr)
			//
		}
	}
}

func (this *AOFContext) trans2SimpleKeysMap() {

}

func (this *AOFContext) PrintTopNkeys() {

}






















-------------------------------------------------------------
package aof

/*
* PROJECT NAME : redisTools
* AUTH : vitoxie
* DATE : 2018/6/1 15:22
* VERSION :  1.1.1
* EMAIL : xiepaup@163.com
*/


type RedisSimpleKeys struct {
	Db         int
	StringKeys map[string]StringMap
	ListKeys   map[string]ListMap
	HashKeys   map[string]HashMap
	SetKeys    map[string]SetMap
	ZSetMap    map[string]ZSetMap
}

type KeyInfo struct {
	Db        string
	KeyType   string
	KeyName   string
	KeySize   int
	ValueSize int
	ValueNumb int
}

type TopBigestKeys struct {
	StringTopKeys map[int]*KeyInfo
	ListTopKeys   map[int]*KeyInfo
	HashTopKeys   map[int]*KeyInfo
	SetTopKeys    map[int]*KeyInfo
	ZsetTopKeys   map[int]*KeyInfo
}

const (
	REDIS_PROTO_SPLIT_WORD = "\r\n"
)

// SET key value
type StringMap struct {
	KeyName  string
	KeyLen   int64
	ValueLen int64
}

// RPUSH key value1 [value2]
type ListMap struct {
	ListKeyLen  int64
	ListKeyName string
	Members     []string
}

// HMSET key field1 value1 [field2 value2 ]
type HashMap struct {
	HashKeyLen  int64
	HashKeyName string
	Members     []StringMap
}

//  SADD key member1 [member2]
type SetMap struct {
	SetKeyLen  int64
	SetKeyName string
	Members    []string
}

type Member struct {
	Score     float64
	ValueName string
}

//  ZADD key score1 member1 [score2 member2]
type ZSetMap struct {
	ZSetKeyLen  int64
	ZSetKeyName string
	Members     []Member
}
