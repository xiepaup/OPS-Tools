package aof

import "fmt"

/**
 *
 * Author  :  xiean
 * EMail   :  xiepaup@163.com 
 * Date    :  2018-07-07
 * Project :  OPS-Tools
 **/


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
