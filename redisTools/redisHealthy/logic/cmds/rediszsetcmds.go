package cmds

import "strings"

/**
 * Author  :  xiean
 * EMail   :  xiepaup@163.com 
 * GitHub  :  https://github.com/xiepaup
 * Date    :  2018-08-05 12:58
 * Project :  dbatools
 **/



var supportRedisZSetCmds []string
var supportRedisZSetReadCmds [16]string
var supportRedisZSetWriteCmds [3]string


func init() {
	supportRedisZSetReadCmds = [...]string{"ZCARD","ZCOUNT","ZRANGE","ZRANGEBYSCORE","ZRANK","ZREMRANGEBYRANK","ZREMRANGEBYSCORE",
	"ZREVRANGE","ZREVRANGEBYSCORE","ZREVRANK","ZSCORE","ZUNIONSTORE","ZSCAN","ZRANGEBYLEX","ZLEXCOUNT","ZREMRANGEBYLEX"}
	supportRedisZSetWriteCmds = [...]string{"ZINCRBY","ZREM","ZADD"}

	supportRedisZSetCmds = supportRedisZSetReadCmds[0:]

	for _, c := range supportRedisZSetWriteCmds {
		supportRedisZSetCmds = append(supportRedisZSetCmds, c)
	}

}

func IsZSetCmd(c string) (bool) {
	c = strings.ToUpper(c)

	for _, lc := range supportRedisZSetCmds {
		if c == lc {
			return true
		}
	}
	return false
}

func GetZSetCmdType(c string) (int) {
	c = strings.ToUpper(c)
	if IsZSetCmd(c) {
		if isZSetReadCmd(c) {
			return ZSET_CMD_TYPE_READ
		}
		if isZSetWriteCmd(c) {
			return ZSET_CMD_YTPE_WRITE
		}
	}
	return UNKOWN_CMD_TYPE
}

func isZSetReadCmd(c string) (bool) {
	c = strings.ToUpper(c)

	for _, lc := range supportRedisZSetReadCmds {
		if c == lc {
			return true
		}
	}
	return false
}

func isZSetWriteCmd(c string) (bool) {
	c = strings.ToUpper(c)

	for _, lc := range supportRedisZSetWriteCmds {
		if c == lc {
			return true
		}
	}
	return false
}
