package main

import (
	"flag"
	"fmt"
	"github.com/go-redis/redis"
	"github.com/gocolly/colly"
	"strconv"
	"time"
)

var (
	watchList  = []string{"bj", "sh", "sz", "cd", "cq", "xa"}
	watchDict  = map[string]int{}
	cli        *redis.Client
	addr, pswd string
)

func init() {
	flag.StringVar(&addr, "addr", "127.0.0.1:33800", "redis addr")
	flag.StringVar(&pswd, "password", "testx1", "redis password")

	flag.Parse()
	cli = redis.NewClient(&redis.Options{
		Addr:     addr,
		Password: pswd, // no password set
		DB:       0,    // use default DB
	})
}

func main() {
	c := colly.NewCollector()

	var city string
	// Find and visit all links
	c.OnHTML(".content .resultDes .total", func(e *colly.HTMLElement) {
		str := e.ChildText("span")
		cnt, err := strconv.Atoi(str)
		if err != nil {
			fmt.Println("convert ", str, "to int failed ")
		}
		watchDict[city] = cnt
	})

	//c.OnHTML("a[href]", func(e *colly.HTMLElement) {
	//	//c.Visit(e.Request.AbsoluteURL(link))
	//	e.Request.Visit(e.Attr("href"))
	//})

	//c.OnRequest(func(r *colly.Request) {
	//	fmt.Println("Visiting", r.URL)
	//})

	for _, city = range watchList {
		url := fmt.Sprintf("https://%s.lianjia.com/ershoufang", city)
		c.Visit(url)
	}
	showResultAndSave()
}

func showResultAndSave() {
	now := time.Now().Format("20060102")
	for c, cnt := range watchDict {
		hk := fmt.Sprintf("house|ershou|%s", c)
		cli.HSet(hk, now, cnt)
		fmt.Println(hk, now, cnt)
	}
	fmt.Println("Job Done")
}
