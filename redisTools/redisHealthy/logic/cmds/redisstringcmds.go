package cmds

import "strings"

/**
 * Author  :  xiean
 * EMail   :  xiepaup@163.com 
 * GitHub  :  https://github.com/xiepaup
 * Date    :  2018-08-05 12:06
 * Project :  dbatools
 **/

var supportRedisStringCmds []string
var supportRedisStringReadCmds [9]string
var supportRedisStringWriteCmds [13]string

//GETSET ===> treat as write .
func init() {

	supportRedisStringReadCmds = [...]string{"APPEND", "BITCOUNT", "BITOP", "BITFIELD", "GET", "GETBIT", "GETRANGE", "MGET", "STRLEN"}
	supportRedisStringWriteCmds = [...]string{"DECRBY", "DECR", "GETSET", "INCR", "INCRBY", "INCRBYFLOAT", "MSET", "MSETNX", "PSETEX", "SET", "SETEX", "SETNX", "SETRANGE"}

	supportRedisStringCmds = supportRedisStringReadCmds[0:]
	for _, c := range supportRedisStringWriteCmds {
		supportRedisStringCmds = append(supportRedisStringCmds, c)
	}

}

func IsStringCmd(c string) (bool) {
	c = strings.ToUpper(c)
	for _, sc := range supportRedisStringCmds {
		if c == sc {
			return true
		}
	}
	return false
}

func GetStringCmdType(c string) (int) {

	if IsStringCmd(c) {
		if isStringReadCmd(c) {
			return STR_CMD_TYPE_READ
		}
		if isStringWriteCmd(c) {
			return STR_CMD_TYPE_WRITE
		}
	}
	return UNKOWN_CMD_TYPE
}

func isStringReadCmd(c string) (bool) {
	c = strings.ToUpper(c)

	for _, sc := range supportRedisStringReadCmds {
		if c == sc {
			return true
		}
	}

	return false
}

func isStringWriteCmd(c string) (bool) {
	c = strings.ToUpper(c)

	for _, sc := range supportRedisStringWriteCmds {
		if c == sc {
			return true
		}
	}
	return false
}
