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

func (this *AOFContext) cmdDoStringType(db string, baseCmd string, baseKey string, args []string) {
	stringMap := this.SimpleKeys[db].StringKeys
	strVal := args[1]
	klen := int64(len(baseKey))
	vlen := int64(len(strVal))

	// SETBIT key bits 0/1
	if baseCmd == "SETBIT" {
		setBitsN, _ := strconv.Atoi(strVal)
		if kv, ok := stringMap[baseKey]; ok {
			setBitsN64 := int64(setBitsN)
			vHasBits := kv.ValueLen * 8
			if setBitsN64 > vHasBits {
				tx := (setBitsN64 - vHasBits) / 8
				ty := (setBitsN64 - vHasBits) % 8
				if ty > 0 {
					vlen = tx + 1 + kv.ValueLen
				} else {
					vlen = tx + kv.ValueLen
				}
			} else {
				vlen = kv.ValueLen
			}
		} else {
			vlen = int64(setBitsN / 8)
			if setBitsN%8 != 0 {
				vlen += int64(1)
			}
			//fmt.Println(fmt.Sprintf("set  k :%d,v:%d,%d,%s,%#v", klen, vlen, setBitsN, strVal, args))
			stringMap[baseKey] = NewStringMap(klen, vlen)
		}
	}
	// SET ,SETEX,SETNX,GETSET,
	if kv, ok := stringMap[baseKey]; ok {
		kv.ValueLen = vlen
		kv.KeyLen = klen
	} else {
		stringMap[baseKey] = NewStringMap(klen, vlen)
	}

	//"INCR", "INCRBY", "INCRBYFOLAT", "DECR", "DECRBY"  ---> valueLen to int64-->8
	if baseCmd == "INCR" || baseCmd == "INCRBY" || baseCmd == "INCRBYFOLAT" || baseCmd == "DECR" || baseCmd == "DECRBY" {
		vlen = 8
	}
	//MSET ,MSETNX
	if baseCmd == "MSET" || baseCmd == "MSETNX" {
		mkvLen := len(args)
		if mkvLen%2 != 0 {
			fmt.Println(fmt.Sprintf("bad MSET/MSETNX %v", args))
			return
		}
		for i := 0; i < mkvLen; i += 2 {
			k := args[i]
			v := args[i+1]
			klen = int64(len(k))
			vlen = int64(len(v))
			if kv, ok := stringMap[baseKey]; ok {
				kv.ValueLen = vlen
				kv.KeyLen = klen
			} else {
				stringMap[baseKey] = NewStringMap(klen, vlen)
			}
		}
	}
	// APPEND
	if baseCmd == "APPEND" {
		if kv, ok := stringMap[baseKey]; ok {
			vlen += kv.ValueLen + vlen
		}
	}

}

func (this *AOFContext) cmdDoHastType(db string, baseCmd string, baseKey string, args []string) {
	//"HDEL", "HSET", "HICRBY", "HINCRBYFLOAT", "HSET", "HMSET", "HSETNX"
	if _, ok := this.SimpleKeys[db]; ok {
		klen := int64(len(baseKey))
		if baseCmd == "HDEL" {
			skey := args[0]
			if _, ok := this.SimpleKeys[db].HashKeys[baseKey].Members[skey]; ok {
				delete(this.SimpleKeys[db].HashKeys[baseKey].Members, skey)
			}
		} else {
			if len(args) == 2 {
				skey1 := args[0]
				sval1 := args[1]
				skey1Len := int64(len(skey1))
				sval1Len := int64(len(sval1))
				if baseCmd == "HICRBY" || baseCmd == "HINCRBYFLOAT" {
					sval1Len = int64(8) // TODO----
					if _, ok := this.SimpleKeys[db].HashKeys[baseKey]; !ok {
						this.SimpleKeys[db].HashKeys[baseKey] = NewHashMap(klen)
						this.SimpleKeys[db].HashKeys[baseKey].Members[skey1] = NewStringMap(skey1Len, sval1Len)
					}
				}
				if baseCmd == "HSET" || baseCmd == "HSETNX" {
					if _, ok := this.SimpleKeys[db].HashKeys[baseKey]; ok {
						if hashSkey, ok := this.SimpleKeys[db].HashKeys[baseKey].Members[skey1]; ok {
							hashSkey.ValueLen = sval1Len
						} else {
							this.SimpleKeys[db].HashKeys[baseKey].Members[skey1] = NewStringMap(skey1Len, sval1Len)
						}
					} else {
						this.SimpleKeys[db].HashKeys[baseKey] = NewHashMap(klen)
						this.SimpleKeys[db].HashKeys[baseKey].Members[skey1] = NewStringMap(skey1Len, sval1Len)
					}
				}
			} else if len(args) > 2 && len(args)%2 == 0 {
				if baseCmd == "HMSET" {
					if _, ok := this.SimpleKeys[db].HashKeys[baseKey]; !ok {
						this.SimpleKeys[db].HashKeys[baseKey] = NewHashMap(klen)
					}
					for i := 0; i < len(args); i += 2 {
						sk := args[i]
						sv := args[i+1]
						skLen := int64(len(sk))
						svLen := int64(len(sv))

						if _, ok := this.SimpleKeys[db].HashKeys[baseKey].Members[sk]; ok {
							this.SimpleKeys[db].HashKeys[baseKey].Members[sk].ValueLen = svLen
						} else {
							this.SimpleKeys[db].HashKeys[baseKey].Members[sk] = NewStringMap(skLen, svLen)
						}
					}
				}
			} else {
				fmt.Println(fmt.Sprintf("bad %s command args %#v...", baseCmd, args))
			}
		}
	} else {
		fmt.Println(fmt.Sprintf("this should never be happen , db [%s] does not exist ,detail : %#v", db, args))
	}

}

func (this *AOFContext) cmdDoListType(db string, baseCmd string, baseKey string, args []string) {
	//LTRIM alist 1 2

	klen := int64(len(baseKey))

	if _, ok := this.SimpleKeys[db]; ok {
		if _, ok := this.SimpleKeys[db].ListKeys[baseKey]; ok {

			oldMemberLen := len(this.SimpleKeys[db].ListKeys[baseKey].Members)

			if baseCmd == "LPOP" {
				this.SimpleKeys[db].ListKeys[baseKey].Members = this.SimpleKeys[db].ListKeys[baseKey].Members[1:]
			}
			if baseCmd == "LREM" {
				actDelCnt := 0
				newMember := make([]string, 10)
				delCnt, _ := strconv.Atoi(args[0])
				matchVal := args[1]
				if delCnt > 0 {
					for i := 0; i < len(this.SimpleKeys[db].ListKeys[baseKey].Members); i++ {
						if actDelCnt == delCnt {
							break
						}
						oldVal := this.SimpleKeys[db].ListKeys[baseKey].Members[i]
						if oldVal != matchVal {
							newMember = append(newMember, oldVal)
						} else {
							actDelCnt++
						}
					}
				} else {
					for i := len(this.SimpleKeys[db].ListKeys[baseKey].Members) - 1; i > -1; i-- {
						if actDelCnt == delCnt {
							break
						}
						oldVal := this.SimpleKeys[db].ListKeys[baseKey].Members[i]
						if oldVal != matchVal {
							newMember = append(newMember, oldVal)
						} else {
							actDelCnt++
						}
					}
					for i, j := 0, len(newMember)-1; i < j; i, j = i+1, j-1 {
						newMember[i], newMember[j] = newMember[j], newMember[i]
					}
				}
				this.SimpleKeys[db].ListKeys[baseKey].Members = newMember
			}
			if baseCmd == "RPOP" {
				this.SimpleKeys[db].ListKeys[baseKey].Members = this.SimpleKeys[db].ListKeys[baseKey].Members[0 : oldMemberLen-2]
			}
			if baseCmd == "RPOPLPUSH" {
				this.SimpleKeys[db].ListKeys[baseKey].Members = this.SimpleKeys[db].ListKeys[baseKey].Members[0 : oldMemberLen-2]
				//TODO -- add to other list
			}
			if baseCmd == "LINSERT" {
				// TODO -- LINSERT key BEFORE|AFTER pivot value
			}
			if baseCmd == "LPUSH" {
				for _, v := range args {
					this.SimpleKeys[db].ListKeys[baseKey].Members = append(this.SimpleKeys[db].ListKeys[baseKey].Members, v)
				}
			}
			if baseCmd == "LSET" {
				//LSET KEY_NAME INDEX VALUE
				if len(args) == 2 {
					idx, _ := strconv.Atoi(args[0])
					val := args[1]
					if idx < oldMemberLen-1 {
						this.SimpleKeys[db].ListKeys[baseKey].Members[idx] = val
					}
				} else {
					fmt.Println(fmt.Sprintf("bad lset %s args : %#v", baseKey, args))
				}
			}
		} else {
			// ignore LREM,,,, only do insert op..
			if baseCmd == "LPUSH" || baseCmd == "RPUSH" {
				this.SimpleKeys[db].ListKeys[baseKey] = NewListMap(klen)
			}
		}

	} else {
		fmt.Println(fmt.Sprintf("unkown db : %s , cmds : %s %s %#v", db, baseCmd, baseKey, args))
	}
}

func (this *AOFContext) cmdDoSetType(db string, baseCmd string, baseKey string, args []string) {
	klen := int64(len(baseKey))
	//SADD,   SDIFFSTORE <> SINTERSTORE,SUNIONSTORE,SMOVE s d k,SPOP k,SREM k m1,m2,,,,
	if _, ok := this.SimpleKeys[db]; ok {
		if _, ok := this.SimpleKeys[db].SetKeys[baseKey]; ok {
			if baseCmd == "SADD" {
				for _, v := range args {
					this.SimpleKeys[db].SetKeys[baseKey].Members = append(this.SimpleKeys[db].SetKeys[baseKey].Members, v)
				}
			} else if baseCmd == "SREM" {
				newMembers := make([]string, 10)
				for _, v := range args {
					for _, bv := range this.SimpleKeys[db].SetKeys[baseKey].Members {
						if bv == v {
							continue
						} else {
							newMembers = append(newMembers, bv)
						}
					}
				}
				this.SimpleKeys[db].SetKeys[baseKey].Members = newMembers
			} else if baseCmd == "SPOP" {
				this.SimpleKeys[db].SetKeys[baseKey].Members = this.SimpleKeys[db].SetKeys[baseKey].Members[1:]
			} else if baseCmd == "SMOVE" {
				//move k from s to d
				if len(args) == 2 {
					sourceSet := baseKey
					destSet := args[0]
					baseKey := args[1]
					if _, ok := this.SimpleKeys[db].SetKeys[destSet]; !ok {
						this.SimpleKeys[db].SetKeys[destSet] = NewSetMap(int64(len(destSet)))
					}
					newMember := make([]string, 10)
					for _, m := range this.SimpleKeys[db].SetKeys[sourceSet].Members {
						if m == baseKey {
							continue
						} else {
							newMember = append(newMember, m)
						}
					}
					this.SimpleKeys[db].SetKeys[sourceSet].Members = newMember
					this.SimpleKeys[db].SetKeys[destSet].Members = append(this.SimpleKeys[db].SetKeys[destSet].Members, baseKey)
				}
			} else {
				fmt.Println(fmt.Sprintf("unsupport this command : %s %s %#v", baseCmd, baseKey, args))
			}
		} else {
			if baseCmd == "SADD" {
				this.SimpleKeys[db].SetKeys[baseKey] = NewSetMap(klen)
				for _, v := range args {
					this.SimpleKeys[db].SetKeys[baseKey].Members = append(this.SimpleKeys[db].SetKeys[baseKey].Members, v)
				}
			} else {
				fmt.Println(fmt.Sprintf("ignore this command : %s %s %#v", baseCmd, baseKey, args))
			}
		}
	} else {
		fmt.Println(fmt.Sprintf("unkonw db : %s,for %s %s %#v", db, baseCmd, baseKey, args))
	}
}

func (this *AOFContext) cmdDoZSetType(db string, baseCmd string, baseKey string, args []string) {
	if _, ok := this.SimpleKeys[db]; ok {
		//TODO
		fmt.Println(fmt.Sprintf("unsupport ZSET : %s,for %s %s %#v", db, baseCmd, baseKey, args))
	} else {
		fmt.Println(fmt.Sprintf("unkonw db : %s,for %s %s %#v", db, baseCmd, baseKey, args))
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
