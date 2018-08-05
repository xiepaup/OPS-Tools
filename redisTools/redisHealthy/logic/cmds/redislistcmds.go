package cmds

import "strings"

/**
 * Author  :  xiean
 * EMail   :  xiepaup@163.com 
 * GitHub  :  https://github.com/xiepaup
 * Date    :  2018-08-05 12:06
 * Project :  dbatools
 **/

var supportRedisListCmds []string
var supportRedisListReadCmds [3]string
var supportRedisListWriteCmds [14]string


func init() {
	supportRedisListReadCmds = [...]string{"LINDEX", "LLEN", "LRANGE"}
	supportRedisListWriteCmds = [...]string{"BLPOP", "BRPOP", "BRPOPLPUSH", "LINSERT", "LPOP", "LPUSH", "LPUSHX", "LREM", "LSET", "LTRIM", "RPOP", "RPOPLPUSH", "RPUSH", "RPUSHX"}

	supportRedisListCmds = supportRedisListReadCmds[0:]

	for _, c := range supportRedisListWriteCmds {
		supportRedisListCmds = append(supportRedisListCmds, c)
	}

}

func IsListCmd(c string) (bool) {
	c = strings.ToUpper(c)

	for _, lc := range supportRedisListCmds {
		if c == lc {
			return true
		}
	}
	return false
}

func GetListCmdType(c string) (int) {
	c = strings.ToUpper(c)
	if IsListCmd(c) {
		if isListReadCmd(c) {
			return LIST_CMD_TYPE_READ
		}
		if isListWriteCmd(c) {
			return LIST_CMD_TYPE_WRITE
		}
	}
	return UNKOWN_CMD_TYPE
}

func isListReadCmd(c string) (bool) {
	c = strings.ToUpper(c)

	for _, lc := range supportRedisListReadCmds {
		if c == lc {
			return true
		}
	}
	return false
}

func isListWriteCmd(c string) (bool) {
	c = strings.ToUpper(c)

	for _, lc := range supportRedisListWriteCmds {
		if c == lc {
			return true
		}
	}
	return false
}
