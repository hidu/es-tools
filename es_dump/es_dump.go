/*
 * Copyright(C) 2020 github.com/hidu  All Rights Reserved.
 * Author: hidu (duv123+git@baidu.com)
 * Date: 2020/5/19
 */

package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"sync"

	"github.com/hidu/es-tools/internal"
)

// IndexInfo 索引信息
type IndexInfo struct {
	Host    *internal.Host    `json:"host"`
	DocType *internal.DocType `json:"type"`
}

// IndexURL 所有的url
func (i *IndexInfo) IndexURL() string {
	return fmt.Sprintf("%s%s", i.Host.Address, i.DocType.URI())
}

// Config 配置
type Config struct {
	// OriginIndex dump的索引
	OriginIndex *IndexInfo      `json:"origin_index"`
	ScanQuery   *internal.Query `json:"scan_query"`
	ScanTime    string          `json:"scan_time"`
}

// String 序列化
func (c *Config) String() string {
	bf, err := json.Marshal(c)
	if err != nil {
		return err.Error()
	}
	return string(bf)
}

var conf = flag.String("conf", "es_dump.json", "config file name")

func main() {
	flag.Parse()
	log.SetFlags(log.Ltime | log.Ltime)

	conf, err := readConf(*conf)
	if err != nil {
		fmt.Println("parser config failed:", err)
		os.Exit(2)
	}

	scrollResultChan := make(chan *internal.ScrollResponse, 10)

	var wg sync.WaitGroup

	// 	for i := 0; i < *bulk_worker; i++ {
	wg.Add(1)
	go func() {
		for job := range scrollResultChan {
			dumpToFile(conf, job)
		}
		wg.Done()
	}()
	// 	}

	scroll := internal.NewScroll(conf.OriginIndex.Host, conf.OriginIndex.DocType, conf.ScanQuery)
	for {
		sr, err := scroll.Next()
		checkErr("scroll_next, err=", err)

		scrollResultChan <- sr
		if !sr.HasMore() {
			break
		}
	}
	close(scrollResultChan)
	wg.Wait()

	log.Println("dump finish")
}

func readConf(confName string) (*Config, error) {
	bs, err := ioutil.ReadFile(confName)
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
	return conf, nil
}

func checkErr(msg string, err error) {
	if err != nil {
		log.Fatalln(msg, err)
	}
}

func dumpToFile(conf *Config, scrollResult *internal.ScrollResponse) {
	for _, item := range scrollResult.Hits.Hits {
		fmt.Println(item)
	}
}
