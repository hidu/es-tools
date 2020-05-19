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
	"path"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hidu/goutils/time_util"

	"github.com/hidu/es-tools/internal"
)

// IndexInfo 待处理的索引信息
type IndexInfo struct {
	Host    *internal.Host    `json:"host"`
	DocType *internal.DocType `json:"type"`
}

// IndexURI 索引的uri
func (i *IndexInfo) IndexURI() string {
	return fmt.Sprintf("%s%s", i.Host.Address, i.DocType.URI())
}

// Config 配置信息
type Config struct {
	OriginIndex   *IndexInfo             `json:"origin_index"`
	NewIndex      *IndexInfo             `json:"new_index"`
	ScanQuery     *internal.Query        `json:"scan_query"`
	ScanTime      string                 `json:"scan_time"`
	FieldsDefault map[string]interface{} `json:"fields_default"`
	DataFixCmd    string                 `json:"data_fix_cmd"`

	sameIndex bool
}

// String 序列化
func (c *Config) String() string {
	bf, _ := json.Marshal(c)
	return string(bf)
}

type CounterType struct {
	start     time.Time
	total     uint64 // scroll 的总数
	read      uint64 // 当前已读总数
	writeSkip uint64
	writeBulk uint64
	bulkC     uint64
}

func (c *CounterType) String() string {
	return fmt.Sprintf("counter[read=%d/%d skip=%d bulk_no=%d bulk_total=%d]", c.read, c.total, c.writeSkip, c.bulkC, c.writeBulk)
}
func (c *CounterType) PrintLog() {
	if c.total > 0 {
		finishRate := float64(c.read) / float64(c.total)
		used := time.Now().Sub(c.start)
		need := float64(c.total-c.read) / (float64(c.read) / used.Seconds())

		finishTime := time.Now().Add(time.Duration(need) * time.Second)

		log.Printf("%s rate=%.2f%% need=%.1fs finish_time=%s", c, 100*finishRate, need, finishTime.Format("2006-01-02 15:04:05"))
	} else {
		log.Println(c)
	}
}

var conf = flag.String("conf", "es_reindex.json", "reindex config file name")
var loopSleep = flag.Int64("loop_sleep", 0, "each loop sleep time")
var bulkWorker = flag.Int("bulk_worker", 3, "bulk worker num")
var isDebug = flag.Bool("debug", false, "debug and print")

var counter = &CounterType{
	start: time.Now(),
}

func main() {
	flag.Parse()
	config, err := readConf(*conf)
	if err != nil {
		fmt.Println("parser config failed:", err)
		os.Exit(2)
	}

	reIndex(config)
}

func readConf(confName string) (*Config, error) {
	bs, err := ioutil.ReadFile(confName)
	if err != nil {
		return nil, err
	}

	os.Chdir(path.Dir(confName))

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

	if conf.NewIndex == nil {
		err = internal.Clone(conf.OriginIndex, &conf.NewIndex)
		checkErr("clone new_index failed", err)
	}

	err = conf.NewIndex.Host.Init()
	checkErr("parse new index", err)

	if conf.OriginIndex.DocType == nil || conf.OriginIndex.DocType.Index == "" {
		return nil, fmt.Errorf("origin_index.type.index is empty")
	}

	if conf.NewIndex.DocType == nil {
		conf.NewIndex.DocType = &internal.DocType{}
	}

	if conf.NewIndex.DocType.Type != "" && conf.OriginIndex.DocType.Type == "" {
		return nil, fmt.Errorf("when origin_index.type.type is empty,new_index.type.type must empty")
	}

	if conf.ScanQuery == nil {
		conf.ScanQuery = internal.NewQuery()
	}

	if conf.FieldsDefault == nil {
		conf.FieldsDefault = make(map[string]interface{})
	}
	conf.DataFixCmd = strings.TrimSpace(conf.DataFixCmd)
	if strings.HasPrefix(conf.DataFixCmd, "#") {
		log.Println("ignore data fix cmd:", conf.DataFixCmd)
		conf.DataFixCmd = ""
	}

	conf.sameIndex = conf.OriginIndex.IndexURI() == conf.NewIndex.IndexURI()

	return conf, nil
}

func checkErr(msg string, err error) {
	if err != nil {
		log.Fatalln(msg, err, counter.String())
	}
}

func reIndex(conf *Config) {
	log.Println("[info] start re_index")
	scroll := internal.NewScroll(conf.OriginIndex.Host, conf.OriginIndex.DocType, conf.ScanQuery)

	scrollResultChan := make(chan *internal.ScrollResponse, *bulkWorker*2)
	var wg sync.WaitGroup

	for i := 0; i < *bulkWorker; i++ {
		wg.Add(1)
		go func(id int) {
			log.Printf("[info] bulk_worker_start id=[%d]\n", id)

			var fixer *internal.SubProcess
			if conf.DataFixCmd != "" {
				var _err error
				fixer, _err = internal.NewSubProcess(conf.DataFixCmd, fmt.Sprintf("%d", id))
				checkErr("create SubProcess faild", _err)
			}
			for job := range scrollResultChan {
				reBulk(conf, job, fixer)
			}
			wg.Done()

			if fixer != nil {
				fixer.Close()
			}
			log.Printf("[info] bulk_worker_finish id=[%d]", id)
		}(i)
	}

	time_util.SetInterval(counter.PrintLog, 5)

	log.Println("[info] started re_bulk worker,n=", *bulkWorker)

	for {
		sr, err := scroll.Next()
		checkErr("scroll_next", err)

		if counter.total == 0 {
			counter.total = scroll.Total()
		}

		counter.read += uint64(len(sr.Hits.Hits))

		scrollResultChan <- sr
		if !sr.HasMore() {
			break
		}
	}

	close(scrollResultChan)

	wg.Wait()

	log.Println("[info] bulkWorker all finished,stop re_index", counter.String())
}

func reBulk(conf *Config, scrollResult *internal.ScrollResponse, fixer *internal.SubProcess) {
	if *isDebug {
		fmt.Println("rebulk", scrollResult.String())
	}
	var datas []string
	datas_map := make(map[string]string)

	for _, item := range scrollResult.Hits.Hits {

		if conf.NewIndex.DocType.Index != "" {
			item.Index = conf.NewIndex.DocType.Index
		}

		if conf.NewIndex.DocType.Type != "" {
			item.Type = conf.NewIndex.DocType.Type
		}

		_hasChange := false
		for k, v := range conf.FieldsDefault {
			if _, has := item.Source[k]; !has {
				item.Source[k] = v
				_hasChange = true
			}
		}

		if fixer != nil {
			_itemRawStr := item.JSONString()
			var _res string
			var _err error
			_try := 0

			for {
				_try++
				_res, _err = fixer.Deal(_itemRawStr)
				if _err != nil {
					log.Println("[err] fixer_deal with error:", _err, "try_times=", _try, "input=", _itemRawStr)
					time.Sleep(1 * time.Second)
					continue
				}
				if _res == "" {
					break
				}
				_hasChange = _itemRawStr != _res

				newItem, _err := internal.NewDataItem(_res)
				if _err != nil {
					log.Println("[err] fixer_data with error:", _err, "try_times=", _try, "raw=", _itemRawStr, "new_str=", _res)
					time.Sleep(1 * time.Second)
					continue
				}
				if *isDebug {
					fmt.Println("fixer >>>" + strings.Repeat("=", 70))
					fmt.Println("raw:", _itemRawStr)
					fmt.Println("new:", _res)
				}
				item = newItem
				break
			}

			if _res == "" {
				atomic.AddUint64(&counter.writeSkip, 1)
				log.Println("[info] skip with empty resp:", item.UniqID())
				continue
			}
		}

		if !conf.sameIndex || _hasChange {
			str := item.String()
			datas_map[item.UniqID()] = str
			datas = append(datas, str)

			atomic.AddUint64(&counter.writeBulk, 1)
		} else {
			atomic.AddUint64(&counter.writeSkip, 1)
		}
	}

	if len(datas) < 1 {
		log.Println("[info] not change,skip reindex")
		return
	}

	var brt internal.BulkResponse

	err := conf.NewIndex.Host.BulkStream(strings.NewReader(strings.Join(datas, "\n")), &brt)
	checkErr("parse bulk resp failed:", err)

	// 	log.Println("bulk resp:", string(body))

	if brt.Errors {
		log.Println("[err] bulk resp has error")
	} else {
		log.Println("[info] bulk all success")
	}

	// 	t,_:=json.Marshal(br)
	// 	fmt.Println("br",string(t))
	for _, data := range brt.Items {
		atomic.AddUint64(&counter.bulkC, 1)
		if item, has := data["index"]; has {
			_id := item.UniqID()
			_raw, _ := datas_map[_id]
			if item.Error != "" {
				log.Printf("[err] bulk_err id=%s err=%s input=%s", _id, item.Error, strings.TrimSpace(_raw))
			} else {
				log.Printf("[info] bulk_suc id=%s s=%d", _id, item.Status)

			}
		}
	}
}
