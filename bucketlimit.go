package main

import (
	"fmt"
	"time"
)

func LeakBucketLimit(MaxQps int, ticket chan struct{}) {

	nticket := int(float64(MaxQps) / 1000) // mileSeconds
	if nticket <= 0 {
		nticket = 1
	}

	fmt.Println("qps for per millseconds : ", nticket)
	tkChan := time.NewTicker(time.Millisecond)

	c := 0
	for {
		if _, ok := <-tkChan.C; ok {
			for i := 0; i < nticket; i++ {
				select {
				case ticket <- struct{}{}:
					continue
					//fmt.Println("make ticket : ", i)
				}
				c++
			}
			//fmt.Println("make ticket ok ", c)
		} else {
			fmt.Println("exit make ticket")
			break
		}
	}
}

func main(){
	ticket := make(chan struct{}, 100)
	go LeakBucketLimit(limit, ticket)


}
