package aof

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
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
	SimpleKeys  map[string]*RedisSimpleKeys
	AOFFileName string
	TopNKeys    int
	StatBySize  bool
	CurrentDb   string
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

		aofctx := &AOFContext{
			CurrentDb:   "0",
			AofHandler:  fh,
			AOFFileName: f,
			SimpleKeys:  make(map[string]*RedisSimpleKeys),
		}
		aofctx.SimpleKeys["0"] = NewRedisSimpleKeys()
		return aofctx, nil
	}
}

func (this *AOFContext) setTopN(n int) {
	if n < 1 {
		n = 1
	}
	this.TopNKeys = n
}

func (this *AOFContext) SetStatTypeBySize(bySize bool) {
	this.StatBySize = bySize
}

func (this *AOFContext) GetBiggestTopKeys(top int) {
	this.setTopN(top)
	bufReader := bufio.NewReader(this.AofHandler)
	defer this.AofHandler.Close()
	this.parseAof(bufReader)

	topKeys := NewTopBiggestKeys(this.TopNKeys)

	if this.StatBySize {
		fmt.Println(fmt.Sprintf("will use value size to stat top %d keys ", top))
		this.getBigestTopKeyBySize(topKeys)
	} else {
		fmt.Println(fmt.Sprintf("will use value filed to stat top %d keys ", top))
		this.getBiggestTopKeyByMember(topKeys)
	}

	this.showTopNkeys(topKeys)
}

func (this *AOFContext) getBigestTopKeyBySize(topKeys *TopBigestKeys) {
	//TODO
	return
}

func (this *AOFContext) getBiggestTopKeyByMember(topKeys *TopBigestKeys) {
	//TODO
	return
}


func (this *AOFContext) parseAof(bufReader *bufio.Reader) {
	for {
		var changeDb bool
		var cmdArray []string
		cmdArray = append(cmdArray, this.CurrentDb)

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
			for i := 0; i < lineNextCnt*2; i++ {
				line, _, err = bufReader.ReadLine()
				if err != nil {
					if err != io.EOF {
						panic(fmt.Sprintf("read file %s error : %v", this.AOFFileName, err))
					}
				}

				if line[0] != '$' {
					opStr := string(line[0:])

					if changeDb {
						this.CurrentDb = opStr
						continue
						changeDb = false
					}

					if opStr == "SELECT" {
						changeDb = true
						continue
					}
					cmdArray = append(cmdArray, opStr)
				}
			}
			this.trans2SimpleKeysMap(cmdArray)
		}
	}
}

func (this *AOFContext) trans2SimpleKeysMap(cmdArray []string) {
	// dbN op key value
	if len(cmdArray) < 3 {
		return
	}
	db := cmdArray[0]
	baseCmd := strings.ToUpper(cmdArray[1])
	baseKey := cmdArray[2]

	if _, ok := this.SimpleKeys[db]; !ok {
		this.SimpleKeys[db] = NewRedisSimpleKeys()
	}

	if CommandsOpsAllTypes(baseCmd) {
		this.cmdDoAllTypes(db, baseCmd, baseKey, cmdArray[3:])

	} else if CommandOpsStringType(baseCmd) {
		this.cmdDoStringType(db, baseCmd, baseKey, cmdArray[2:])

	} else if CommandOpsListType(baseCmd) {
		this.cmdDoListType(db, baseCmd, baseKey, cmdArray[2:])

	} else if CommandOpsHashType(baseCmd) {
		this.cmdDoHastType(db, baseCmd, baseKey, cmdArray[3:])

	} else if CommandOpsSetType(baseCmd) {
		this.cmdDoSetType(db, baseCmd, baseKey, cmdArray[3:])

	} else if CommandOpsZSetType(baseCmd) {
		this.cmdDoZSetType(db, baseCmd, baseKey, cmdArray[3:])

	} else {
		fmt.Println(fmt.Sprintf("unkonw command : %#v", cmdArray))
	}

}

func (this *AOFContext) cmdDoAllTypes(db string, baseCmd string, baseKey string, args []string) {
	if baseCmd == "DEL" {
		if _, ok := this.SimpleKeys[db].StringKeys[baseKey]; ok {
			delete(this.SimpleKeys[db].StringKeys, baseKey)
		}
		if _, ok := this.SimpleKeys[db].ListKeys[baseKey]; ok {
			delete(this.SimpleKeys[db].ListKeys, baseKey)
		}
		if _, ok := this.SimpleKeys[db].HashKeys[baseKey]; ok {
			delete(this.SimpleKeys[db].HashKeys, baseKey)
		}
		if _, ok := this.SimpleKeys[db].SetKeys[baseKey]; ok {
			delete(this.SimpleKeys[db].SetKeys, baseKey)
		}
		if _, ok := this.SimpleKeys[db].ZSetMap[baseKey]; ok {
			delete(this.SimpleKeys[db].ZSetMap, baseKey)
		}
	}
	if baseCmd == "CLEANDB" || baseCmd == "FLUSHDB" {
		fmt.Println("todo flushdb --reset mem..")
	}
}



func (this *AOFContext) showTopNkeys(topkeys *TopBigestKeys) {
	for db, vv := range this.SimpleKeys {
		for k, v := range vv.StringKeys {
			fmt.Println(fmt.Sprintf("String : db : %s, k: %s, v:%#v", db, k, v))
		}

		for k, v := range vv.HashKeys {
			for k2, v2 := range v.Members {
				fmt.Println(fmt.Sprintf("Hash : db : %s, k: %s, sk: %s sv:%#v", db, k, k2, v2))
			}
		}
	}
}
