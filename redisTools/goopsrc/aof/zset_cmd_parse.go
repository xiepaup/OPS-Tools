package aof

import "fmt"

/**
 *
 * Author  :  xiean
 * EMail   :  xiepaup@163.com 
 * Date    :  2018-07-07
 * Project :  OPS-Tools
 **/


func (this *AOFContext) cmdDoZSetType(db string, baseCmd string, baseKey string, args []string) {
	if _, ok := this.SimpleKeys[db]; ok {
		//TODO
		fmt.Println(fmt.Sprintf("unsupport ZSET : %s,for %s %s %#v", db, baseCmd, baseKey, args))
	} else {
		fmt.Println(fmt.Sprintf("unkonw db : %s,for %s %s %#v", db, baseCmd, baseKey, args))
	}
}