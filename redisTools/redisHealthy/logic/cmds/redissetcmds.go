package cmds

import "strings"

/**
 * Author  :  xiean
 * EMail   :  xiepaup@163.com 
 * GitHub  :  https://github.com/xiepaup
 * Date    :  2018-08-05 12:58
 * Project :  dbatools
 **/

var supportRedisSETCmds []string
var supportRedisSETReadCmds [10]string
var supportRedisSETWriteCmds [4]string


func init() {
	supportRedisSETReadCmds = [...]string{"SCARD", "SDIFF", "SDIFFSTORE","SINTER","SINTERSTORE","SISMEMBER","SRANDMEMBER","SUNION","SUNIONSTORE","SSCAN"}
	supportRedisSETWriteCmds = [...]string{"SADD", "SMOVE", "SPOP", "SREM"}

	supportRedisSETCmds = supportRedisListReadCmds[0:]

	for _, c := range supportRedisSETWriteCmds {
		supportRedisSETCmds = append(supportRedisSETCmds, c)
	}

}

func IsSetCmd(c string) (bool) {
	c = strings.ToUpper(c)

	for _, lc := range supportRedisSETCmds {
		if c == lc {
			return true
		}
	}
	return false
}

func GetSetCmdType(c string) (int) {
	c = strings.ToUpper(c)
	if IsSetCmd(c) {
		if isSetReadCmd(c) {
			return SET_CMD_TYPE_READ
		}
		if isSetWriteCmd(c) {
			return SET_CMD_TYPE_WRITE
		}
	}
	return UNKOWN_CMD_TYPE
}

func isSetReadCmd(c string) (bool) {
	c = strings.ToUpper(c)

	for _, lc := range supportRedisSETReadCmds {
		if c == lc {
			return true
		}
	}
	return false
}

func isSetWriteCmd(c string) (bool) {
	c = strings.ToUpper(c)

	for _, lc := range supportRedisSETWriteCmds {
		if c == lc {
			return true
		}
	}
	return false
}
