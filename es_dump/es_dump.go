package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/hidu/es-tools/internal"
	"io/ioutil"
	"log"
	"os"
	"sync"
)

type IndexInfo struct {
	Host    *internal.Host    `json:"host"`
	DocType *internal.DocType `json:"type"`
}

func (i *IndexInfo) IndexUri() string {
	return fmt.Sprintf("%s%s", i.Host.AddressUrl, i.DocType.Uri())
}

type Config struct {
	OriginIndex   *IndexInfo             `json:"origin_index"`
	ScanQuery     *internal.Query        `json:"scan_query"`
	ScanTime      string                 `json:"scan_time"`
}

func (c *Config) String() string {
	bf, _ := json.Marshal(c)
	return string(bf)
}

var conf_name = flag.String("conf", "es_dump.json", "config file name")

func main() {
	flag.Parse()
	
	conf, err := readConf(*conf_name)
	if err != nil {
		fmt.Println("parser config failed:", err)
		os.Exit(2)
	}
	
	scrollResultChan := make(chan *internal.ScrollResult, 10)
	
	var wg sync.WaitGroup

//	for i := 0; i < *bulk_worker; i++ {
		wg.Add(1)
		go func() {
			for job := range scrollResultChan {
				dumpToFile(conf, job)
			}
			wg.Done()
		}()
//	}
	
	scroll := internal.NewScroll(conf.OriginIndex.Host, conf.OriginIndex.DocType, conf.ScanQuery)
	for {
		sr, err := scroll.Next()
		checkErr("scroll_next", err)

		scrollResultChan <- sr
		if !sr.HasMore() {
			break
		}
	}
	close(scrollResultChan)
	wg.Wait()
}

func readConf(conf_name string) (*Config, error) {
	bs, err := ioutil.ReadFile(conf_name)
	if err != nil {
		return nil, err
	}
	var conf *Config
	dec := json.NewDecoder(bytes.NewReader(bs))
	dec.UseNumber()
	err = dec.Decode(&conf)
	if err != nil {
		return nil, err
	}
	if conf.ScanTime == "" {
		conf.ScanTime = "120s"
	}

	if conf.OriginIndex == nil {
		log.Fatalln("origin_index is empty")
	}

	err = conf.OriginIndex.Host.Init()
	checkErr("parse origin index", err)
		if conf.ScanQuery == nil {
		conf.ScanQuery = internal.NewQuery()
	}
		return conf,nil
}

func checkErr(msg string, err error) {
	if err != nil {
		log.Fatalln(msg, err)
	}
}

func dumpToFile(conf *Config, scrollResult *internal.ScrollResult){
	for _, item := range scrollResult.Hits.Hits {
		fmt.Println(item)
	}
}

