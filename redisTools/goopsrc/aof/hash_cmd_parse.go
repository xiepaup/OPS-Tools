package aof

import "fmt"

/**
 *
 * Author  :  xiean
 * EMail   :  xiepaup@163.com 
 * Date    :  2018-07-07
 * Project :  OPS-Tools
 **/



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