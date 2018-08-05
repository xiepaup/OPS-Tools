package macro

import (
	"sync"
	"fmt"
)

/**
 *
 * Author  :  xiean
 * EMail   :  xiepaup@163.com
 * GitHub  :  https://github.com/xiepaup
 * Date    :  2018-08-03
 * Project :  dbatools
 **/

type GlobalWatch struct {
	mutx sync.Mutex
	
	WriteBytes int64
	ReadBytes int64
	
	ReadOpsCnt int64
	WriteOpsCnt int64 
	
	HashOpsCnt int64 
	StrOpsCnt int64
	ListOpsCnt int64
	SetOpsCnt  int64
	ZSetOpsCnt int64 
	
	AllOpsCnt  int64 
}

func NewGlobalWatch() (*GlobalWatch) {
	return &GlobalWatch{}
}

func (this *GlobalWatch) Incr()  {
	
}



func (this *GlobalWatch) ToString() {
	fmt.Println(fmt.Sprintf("%#v",this))
}