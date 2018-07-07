package aof

import (
	"strconv"
	"fmt"
)

/**
 *
 * Author  :  xiean
 * EMail   :  xiepaup@163.com 
 * Date    :  2018-07-07
 * Project :  OPS-Tools
 **/




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