package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"time"
)

/**
 *
 * Author  :  xiean
 * EMail   :  xiepaup@163.com
 * GitHub  :  https://github.com/xiepaup
 * Date    :  2018-08-03
 * Project :  dbatools
 **/

const MAX_QUEUE_LEN = 100

var (
	RunSeconds      int
	DelayRunSeconds int
	Addr            string
	Paswd           string

	monitorChan   chan interface{}
	stopChan      chan struct{}
	terminateChan chan struct{}
)

func init() {

	stopChan = make(chan struct{}, 0)
	monitorChan = make(chan interface{}, MAX_QUEUE_LEN)
	terminateChan = make(chan struct{}, 0)

	flag.IntVar(&RunSeconds, "runsec", 60, "how long will be monitor")
	flag.IntVar(&DelayRunSeconds, "delaysec", 0, "if not 0, monitor will run N seconds later ")
	flag.StringVar(&Addr, "addr", "127.0.0.1:6379", "redis listen address")
	flag.StringVar(&Paswd, "password", "", "redis auth ,usually will be requirepass config item")
	flag.Parse()
}

func terminalProgram(terminalChan chan struct{}) {
	c := make(chan os.Signal, 0)
	signal.Notify(c)

	<-c

	terminalChan <- struct{}{}
}

func main() {

	terminalProgram(terminateChan)

	//go time  ctl
	//go redis ctl
	//

	mainLoop()

}



func mainLoop() {
	for {
		fmt.Println("todo Main Loop ..")

		select {
		case <-terminateChan:
			break
		case <-stopChan:
			//TODO  --> send stop chan to monitor , and consumer
		default:
			//TODO  ---> run programs .. and sleep some time
			time.Sleep(time.Microsecond * 10)

		}

		time.Sleep(time.Millisecond * 10)
	}
}
