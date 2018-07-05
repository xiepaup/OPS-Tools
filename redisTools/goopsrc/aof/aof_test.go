package aof

import (
	"testing"
	"fmt"
)

/**
 *
 * Author  :  xiepaup
 * EMail   :  xiepaup@163.com 
 * Date    :  2018-06-02
 * Project :  redisTools
 **/


func TestAOFContext_GetBigestTopKey(t *testing.T) {
	fmt.Println("this is unit test for aof of top n keys ...")
	aof,err := NewAOFContext("../test/redis-appendonly.aof")
	if err != nil{
		panic(fmt.Sprintf("init error : %s",err))
	}
	aof.SetStatTypeBySize(true)
	aof.GetBiggestTopKeys(10)
	//aof.GetBiggestTopKeys(10)
}