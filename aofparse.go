package aof

import (
	"os"
	"errors"
	"bufio"
	"io"
	"fmt"
	"strconv"
)

/*
* PROJECT NAME : redisTools
* AUTH : vitoxie
* DATE : 2018/6/1 15:19
* VERSION :  1.1.1
* EMAIL : xiepaup@163.com
*/

type AOFContext struct {
	AofHandler  *os.File
	AOFFileName string
	TopNKeys    int
}


func NewAOFContext(f string) (*AOFContext, error) {
	_, err := os.Stat(f)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, errors.New("File Do Not Exist")
		}
		return nil, err
	} else {
		fh, err := os.Open(f)
		if err != nil {
			return nil, errors.New("Open File Error")
		}
		return &AOFContext{
			AofHandler:fh,
			AOFFileName:f,
		}, nil
	}
}


func (this *AOFContext) setTopN(n int) {
	if n < 1 {
		n = 1
	}
	this.TopNKeys = n
}


func (this *AOFContext) GetBigestTopKey(top int) () {
	this.setTopN(top)
	bufReader := bufio.NewReader(this.AofHandler)
	defer this.AofHandler.Close()
	this.loadAof2Memory(bufReader)
}


func (this *AOFContext) ParseAOF2Command()  {
	bufReader := bufio.NewReader(this.AofHandler)
	defer this.AofHandler.Close()
	this.loadAof2Memory(bufReader)
}


func (this *AOFContext) loadAof2Memory(bufReader *bufio.Reader) ([]string) {
	for {
		var cmdStr string
		line,_, err := bufReader.ReadLine()
		if err != nil {
			if err == io.EOF {
				break
			} else {
				panic(fmt.Sprintf("read file %s error : %v", this.AOFFileName, err))
			}
		}

		if line[0] == '*' {
			lineNextCnt, err := strconv.Atoi(string(line[1:]))
			for i := 0; i < lineNextCnt * 2; i++ {
				line,_, err = bufReader.ReadLine()
				if err != nil {
					if err != io.EOF {
						panic(fmt.Sprintf("read file %s error : %v", this.AOFFileName, err))
					}
				}
				if line[0] != '$' {
					cmdStr = fmt.Sprintf("%s %s", cmdStr, string(line))
				}
			}
			fmt.Println(cmdStr)
			//
		}
	}
}


func (this *AOFContext) PrintTopNkeys() {

}
