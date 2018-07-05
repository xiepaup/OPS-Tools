package aof


/**
 *
 * Author  :  xiean
 * EMail   :  xiepaup@163.com
 * Date    :  2018-06-02
 * Project :  redisTools
 **/

type RedisSimpleKeys struct {
	Db         int
	StringKeys map[string]*StringMap
	ListKeys   map[string]*ListMap
	HashKeys   map[string]*HashMap
	SetKeys    map[string]*SetMap
	ZSetMap    map[string]*ZSetMap
}

type KeyInfo struct {
	Db        string
	KeyType   string
	KeyName   string
	KeySize   int
	ValueSize int
	ValueNumb int
}

func NewKeyInfo() (*KeyInfo) {
	return &KeyInfo{
		KeySize:0,
		ValueSize:0,
		ValueNumb:0,
	}
}

type TopBigestKeys struct {
	StringTopKeys map[int]*KeyInfo
	ListTopKeys   map[int]*KeyInfo
	HashTopKeys   map[int]*KeyInfo
	SetTopKeys    map[int]*KeyInfo
	ZsetTopKeys   map[int]*KeyInfo
}

func NewTopBiggestKeys(top int) (*TopBigestKeys) {
	biggestKeys := &TopBigestKeys{
		StringTopKeys:make(map[int]*KeyInfo, top),
		ListTopKeys:make(map[int]*KeyInfo, top),
		HashTopKeys:make(map[int]*KeyInfo, top),
		SetTopKeys:make(map[int]*KeyInfo, top),
		ZsetTopKeys:make(map[int]*KeyInfo, top),
	}
	for i:=1;i<=top;i++{
		biggestKeys.StringTopKeys[i] = NewKeyInfo()
		biggestKeys.ListTopKeys[i] = NewKeyInfo()
		biggestKeys.HashTopKeys[i] = NewKeyInfo()
		biggestKeys.SetTopKeys[i] = NewKeyInfo()
		biggestKeys.ZsetTopKeys[i] = NewKeyInfo()
	}

	return biggestKeys
}

const (
	REDIS_PROTO_SPLIT_WORD = "\r\n"
)

// SET key value
type StringMap struct {
	KeyName     string
	KeyLen      int64
	ValueLen    int64
	ExpireSeted bool
}

func NewStringMap(klen, vlen int64) (*StringMap) {

	return &StringMap{
		KeyLen:klen,
		ValueLen:vlen,
	}
}

// RPUSH key value1 [value2]
type ListMap struct {
	ListKeyLen  int64
	//ListKeyName string
	Members     []string
	ExpireSeted bool
}

func NewListMap(klen int64) (*ListMap) {
	return &ListMap{
		ListKeyLen:klen,
		Members:make([]string, 10),
	}
}

// HMSET key field1 value1 [field2 value2 ]
type HashMap struct {
	HashKeyLen  int64
	//HashKeyName string
	Members     map[string]*StringMap
	ExpireSeted bool
}

func NewHashMap(klen int64) (*HashMap) {
	return &HashMap{
		HashKeyLen:klen,
		Members:make(map[string]*StringMap),
	}
}

//  SADD key member1 [member2]
type SetMap struct {
	SetKeyLen   int64
	//SetKeyName  string
	Members     []string
	ExpireSeted bool
}

func NewSetMap(klen int64) (*SetMap) {
	return &SetMap{
		SetKeyLen:klen,
		Members:make([]string, 10),
	}
}

type Member struct {
	Score     float64
	ValueName string
}

//  ZADD key score1 member1 [score2 member2]
type ZSetMap struct {
	ZSetKeyLen  int64
	//ZSetKeyName string
	Members     []Member
	ExpireSeted bool
}

func NewRedisSimpleKeys() *RedisSimpleKeys {
	return &RedisSimpleKeys{
		StringKeys: make(map[string]*StringMap),
		ListKeys:   make(map[string]*ListMap),
		HashKeys:   make(map[string]*HashMap),
		SetKeys:    make(map[string]*SetMap),
		ZSetMap:    make(map[string]*ZSetMap),
	}
}
