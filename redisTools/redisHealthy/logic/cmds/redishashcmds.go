package cmds

import "strings"

/**
 * Author  :  xiean
 * EMail   :  xiepaup@163.com 
 * GitHub  :  https://github.com/xiepaup
 * Date    :  2018-08-05 12:06
 * Project :  dbatools
 **/

var supportRedisHashCmds []string
var supportRedisHashReadCmds [9]string
var supportRedisHashWriteCmds [6]string

func init() {

	//supportRedisHashCmds = []string{"HKEYS","HEXISTS","HGET","HGETALL","HLEN","HMGET","HVALS","HSCAN","HSTRLEN","HSET","HMSET","HDEL","HINCRBY","HINCRBYFLOAT","HSETNX"}
	supportRedisHashReadCmds = [...]string{"HKEYS", "HEXISTS", "HGET", "HGETALL", "HLEN", "HMGET", "HVALS", "HSCAN", "HSTRLEN"}
	supportRedisHashWriteCmds = [...]string{"HSET", "HMSET", "HDEL", "HINCRBY", "HINCRBYFLOAT", "HSETNX"}

	supportRedisHashCmds = supportRedisHashReadCmds[0:]
	for _, c := range supportRedisHashWriteCmds {
		supportRedisHashCmds = append(supportRedisHashCmds, c)
	}

}

func GetRedisHashCmds() ([]string) {
	return supportRedisHashCmds
}

func IsHashCmd(c string) (bool) {
	c = strings.ToUpper(c)
	for _, hc := range supportRedisHashCmds {
		if c == hc {
			return true
		}
	}
	return false
}

func GetHashCmdType(c string) (int) {
	if IsHashCmd(c) {
		if isHashReadCmd(c) {
			return HASH_CMD_TYPE_READ
		}

		if isHashWriteCmd(c) {
			return HASH_CMD_TYPE_WRITE
		}
	}
	return UNKOWN_CMD_TYPE
}

func isHashReadCmd(c string) bool {
	c = strings.ToUpper(c)
	for _, hc := range supportRedisHashReadCmds {
		if c == hc {
			return true
		}
	}
	return false
}

func isHashWriteCmd(c string) bool {
	c = strings.ToUpper(c)
	for _, hc := range supportRedisHashWriteCmds {
		if c == hc {
			return true
		}
	}
	return false
}
