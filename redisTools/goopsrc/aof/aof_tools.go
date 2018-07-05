package aof


import (
	"strings"
)
/**
 *
 * Author  :  xiean
 * EMail   :  xiepaup@163.com 
 * Date    :  2018-06-02
 * Project :  redisTools
 **/






func CommandsOpsAllTypes(cmd string) bool{
	REDIS_UNIVERS_COMMANDS := [...]string{"DEL"}
	for _,c := range REDIS_UNIVERS_COMMANDS {
		if strings.Compare(c,cmd) == 0 {
			return true
		}
	}
	return false
}

func CommandOpsStringType(cmd string) bool {
	REDIS_STRING_COMMANDS  := [...]string{"SET", "GETSET",
		"SETBIT", "SETEX", "SETNX", "SETRANGE",
		"MSET", "MSETNX", "PSETEX", "INCR", "INCRBY", "INCRBYFOLAT", "DECR", "DECRBY", "APPEND"}
	for _,c := range REDIS_STRING_COMMANDS {
		if strings.Compare(c,cmd) == 0 {
			return true
		}
	}
	return false
}

func CommandOpsListType(cmd string) bool {
	REDIS_LIST_COMMANDS := [...]string{"LPUSH", "RPUSH", "LPOP", "RPOP", "LREM", "LSET",
		"LTRIM", "RPOPLPUSH", "LINSERT", "LPUSHX", "RPUSHX"}

	for _,c := range REDIS_LIST_COMMANDS {
		if strings.Compare(c,cmd) == 0 {
			return true
		}
	}
	return false
}

func CommandOpsHashType(cmd string) bool {
	REDIS_HASH_COMMANDS := [...]string{"HDEL", "HSET", "HICRBY", "HINCRBYFLOAT", "HMSET", "HSETNX"}
	for _,c := range REDIS_HASH_COMMANDS {
		if strings.Compare(c,cmd) == 0 {
			return true
		}
	}
	return false
}

func CommandOpsSetType(cmd string) bool {
	REDIS_SET_COMMANDS := [...]string{"SADD", "SREM", "SDIFFSTROE", "SINTERSTROE", "SUNIONSTROE", "SPOP", "SMOVE", "SPOP"}
	for _,c := range REDIS_SET_COMMANDS {
		if strings.Compare(c,cmd) == 0 {
			return true
		}
	}
	return false
}

func CommandOpsZSetType(cmd string) bool {
	REDIS_ZSET_COMMANDS  := [...]string{"ZADD", "ZINCRBY", "ZREM", "ZREMRANGEBYLEX", "ZREMRANGEBYRANK", "ZREMRANGEBYSCORE", "ZINTERSTORE"}

	for _,c := range REDIS_ZSET_COMMANDS {
		if strings.Compare(c,cmd) == 0 {
			return true
		}
	}
	return false
}