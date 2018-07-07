package aof

import (
	"fmt"
	"strconv"
)

/**
 *
 * Author  :  xiean
 * EMail   :  xiepaup@163.com 
 * Date    :  2018-07-07
 * Project :  OPS-Tools
 **/


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

