package main

import (
	"crypto/md5"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"sync"
	"time"

	"io"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"net/url"
	"strings"

	"flag"

	"labix.org/v2/mgo"
	"labix.org/v2/mgo/bson"
)

const (
	//enumerator of proxy state
	proxyFail = true
	proxyOK   = false

	threadsCount = 10
	dbname       = "parser"
	timeout      = 60 //second, proxy timeout
	dummy_text   = "Sorry, your IP was blocked"
)

var (
	wg      sync.WaitGroup
	r       *rand.Rand
	session *mgo.Session
	links   *mgo.Collection
	proxies *mgo.Collection
	threads int
)

type LinkItem struct {
	Url        string  `bson:"url"`
	Done       bool    `bson:"done"`
	Random_url float32 `bson: "random_url"`
}

type ProxyItem struct {
	Url     string  `bson:"url"`
	Calls   int     `bson:"calls"`
	Fail    int     `bson:"fail"`
	Random  float32 `bson:"random"`
	Deleted bool    `bson:"boolean"`
}

// timeout for proxy request
func dialTimeout(network, addr string) (net.Conn, error) {
	return net.DialTimeout(network, addr, time.Duration(timeout*time.Second))
}

func getProxy() (ProxyItem, error) {
	var res []ProxyItem
	rnd := getRand()
	filter := bson.M{"deleted": false, "random": bson.M{"$lte": rnd}}
	err := proxies.Find(filter).All(&res)
	if err != nil {
		return ProxyItem{}, err
	}
	if len(res) == 0 {
		filter := bson.M{"deleted": false, "random": bson.M{"$gte": rnd}}
		err := proxies.Find(filter).All(&res)
		if err != nil {
			return ProxyItem{}, err
		}
	}
	return res[int(float32(len(res))*getRand())], nil
}

func deleteProxy(item ProxyItem) {
	err := proxies.Update(bson.M{"url": item.Url}, bson.M{"url": item.Url, "calls": item.Calls, "fail": item.Fail, "random": item.Random, "deleted": true})
	if err != nil {
		log.Printf("deleteProxy: %s\n", err.Error())
	}
	n, err := proxies.Find(bson.M{"deleted": false}).Count()
	if n < threads {
		log.Println("Count of proxies is not enought for normally work. Please, adding addresses!")
		os.Exit(0)
	}
}

func hitProxy(item ProxyItem, state bool) {
	value := 0
	if state == proxyFail {
		value = 1
	}
	// delete unstable proxy
	if (item.Calls > 10) && (float32(item.Fail+value)/float32(item.Calls) > 0.5) {
		deleteProxy(item)
	} else {
		err := proxies.Update(bson.M{"url": item.Url}, bson.M{"url": item.Url, "calls": item.Calls + 1, "fail": item.Fail + value, "random": item.Random, "deleted": item.Deleted})
		if err != nil {
			log.Printf("hitProxy: %s\n", err.Error())
		}
	}
}

func getPage(_url string) ([]byte, error) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("Panic in %s\n%v\n", _url, r)
		}
	}()
	proxy, err := getProxy()
	if err != nil {
		log.Printf(err.Error())
		hitProxy(proxy, proxyFail)
		return nil, err
	} else {
		fmt.Printf("proxy: %s\n", proxy.Url)
	}
	proxyUrl, err := url.Parse(proxy.Url)
	if err != nil {
		log.Printf(err.Error())
		hitProxy(proxy, proxyFail)
		return nil, err
	}
	transport := &http.Transport{Dial: dialTimeout, Proxy: http.ProxyURL(proxyUrl)}
	transport.CloseIdleConnections()
	myClient := &http.Client{Transport: transport}

	var res *http.Response
	done := make(chan bool)
	go func(done chan bool) {
		res, err = myClient.Get(_url)
		done <- true
	}(done)
	select {
	case <-done:
		//all OK
	case <-time.After(timeout * time.Second):
		err = errors.New("Timeout")
	}
	if err != nil {
		log.Println(err.Error())
		hitProxy(proxy, proxyFail)
		return nil, err
	}
	defer res.Body.Close()
	body, err := ioutil.ReadAll(res.Body)
	if err != nil {
		log.Printf(err.Error())
		hitProxy(proxy, proxyFail)
		return nil, err
	}
	if strings.Contains(string(body), dummy_text) {
		err = errors.New("Proxy is blocked")
		deleteProxy(proxy)
		return nil, err
	}
	return body, nil
}

func init() {
	flag.IntVar(&threads, "threads", threadsCount, "count of threads")
	flag.Parse()
	var t = time.Now()
	r = rand.New(rand.NewSource(t.UnixNano()))
}

func getRand() float32 {
	return r.Float32()
}

func dispatch(x chan LinkItem) {
	in, out := make(chan LinkItem, threads), make(chan bool, threads)
	for i := 0; i < threads; i++ {
		in <- <-x
	}
	for {
		select {
		case d := <-in:
			go wget(d, out)
		case <-out:
			in <- <-x
		}
	}
}

func filename(_url string) string {
	hash := md5.New()
	io.WriteString(hash, _url)
	return fmt.Sprintf("%x", hash.Sum(nil))
}

// save page body to tmp directory
func save(_url string, body []byte) {
	file, _ := os.Create("./tmp/" + filename(_url))
	defer file.Close()
	file.Write(body)
}

// checked item as downloaded
func checkOK(item LinkItem) {
	err := links.Update(bson.M{"url": item.Url}, bson.M{"url": item.Url, "done": true, "random_url": item.Random_url})
	if err != nil {
		log.Printf("checkOK: %s\n", err.Error())
	}
}

func wget(item LinkItem, done chan bool) {
	body, err := getPage(item.Url)

	if err != nil {
		log.Printf("FAIL: %s\n", item.Url)
	} else {
		save(item.Url, body)
		checkOK(item)
		time.Sleep(time.Duration(1000+getRand()) * time.Millisecond)
		log.Printf("OK: %s\n", item.Url)
	}
	wg.Done()
	done <- true
}

func initMongo() {
	var err error
	session, err = mgo.Dial("localhost")
	if err != nil {
		panic(err.Error())
	}
	links = session.DB(dbname).C("links")
	proxies = session.DB(dbname).C("proxies")
}

func main() {
	ch := make(chan LinkItem, threads)
	go dispatch(ch)

	initMongo()
	defer session.Close()

	var l []LinkItem
	err := links.Find(bson.M{"done": false}).All(&l)
	if err != nil {
		log.Printf("Error get data from mongo: %s\n", err.Error())
		os.Exit(0)
	}
	for _, v := range l {
		wg.Add(1)
		ch <- v
	}
	wg.Wait()
}
