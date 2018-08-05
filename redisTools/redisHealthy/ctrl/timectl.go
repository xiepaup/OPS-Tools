package ctrl

import "time"

/**
 *
 * Author  :  xiean
 * EMail   :  xiepaup@163.com
 * GitHub  :  https://github.com/xiepaup
 * Date    :  2018-08-03
 * Project :  dbatools
 **/

type TimeCtl struct {
	StartTime time.Time
	RunSeconds int
	DelaySeconds int

}

func NewTimeCtl(r,d,int2 int) (*TimeCtl) {
	return &TimeCtl{
		StartTime:time.Now(),
		RunSeconds:r,
		DelaySeconds:d,
	}
}



func (this *TimeCtl) GetRealStartTime()  {

}