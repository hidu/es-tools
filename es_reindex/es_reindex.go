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
	"path"
	"strings"
	"sync"
	"time"
)

func init() {
	ua := flag.Usage
	flag.Usage = func() {
		ua()
		fmt.Println("\n site: https://github.com/hidu/es-tools/")
		fmt.Println(" version:", "20171107 1.0")
	}
}

type IndexInfo struct {
	Host    *internal.Host    `json:"host"`
	DocType *internal.DocType `json:"type"`
}

func (i *IndexInfo) IndexUri() string {
	return fmt.Sprintf("%s%s", i.Host.AddressUrl, i.DocType.Uri())
}

type Config struct {
	OriginIndex   *IndexInfo             `json:"origin_index"`
	NewIndex      *IndexInfo             `json:"new_index"`
	ScanQuery     *internal.Query        `json:"scan_query"`
	ScanTime      string                 `json:"scan_time"`
	FieldsDefault map[string]interface{} `json:"fields_default"`
	DataFixCmd    string                 `json:"data_fix_cmd"`
	sameIndex     bool                   `json:"-"`
}

func (c *Config) String() string {
	bf, _ := json.Marshal(c)
	return string(bf)
}

var conf_name = flag.String("conf", "es_reindex.json", "reindex config file name")
var loop_sleep = flag.Int64("loop_sleep", 0, "each loop sleep time")
var bulk_worker = flag.Int("bulk_worker", 3, "bulk worker num")
var isDebug = flag.Bool("debug", false, "debug and print")

func main() {
	flag.Parse()
	config, err := readConf(*conf_name)
	if err != nil {
		fmt.Println("parser config failed:", err)
		os.Exit(2)
	}

	reIndex(config)
}

func readConf(conf_name string) (*Config, error) {
	bs, err := ioutil.ReadFile(conf_name)
	if err != nil {
		return nil, err
	}

	os.Chdir(path.Dir(conf_name))

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

	conf.sameIndex = conf.OriginIndex.IndexUri() == conf.NewIndex.IndexUri()

	return conf, nil
}

func checkErr(msg string, err error) {
	if err != nil {
		log.Fatalln(msg, err)
	}
}

func reIndex(conf *Config) {
	log.Println("start re_index")
	scroll := internal.NewScroll(conf.OriginIndex.Host, conf.OriginIndex.DocType, conf.ScanQuery)

	scrollResultChan := make(chan *internal.ScrollResult, *bulk_worker*2)
	var wg sync.WaitGroup

	for i := 0; i < *bulk_worker; i++ {
		wg.Add(1)
		go func(id int) {
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
		}(i)
	}

	log.Println("started re_bulk worker,n=", *bulk_worker)

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

	log.Println("stop re_index")
}

func reBulk(conf *Config, scrollResult *internal.ScrollResult, fixer *internal.SubProcess) {
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
			_itemRawStr := item.JsonString()
			var _res string
			var _err error
			_try := 0

			for {
				_try++
				_res, _err = fixer.Deal(_itemRawStr)
				if _err != nil {
					log.Println("fixer_deal with error:", _err, "try_times=", _try, "input=", _itemRawStr)
					time.Sleep(1 * time.Second)
					continue
				}
				if _res == "" {
					break
				}
				_hasChange = _itemRawStr != _res

				newItem, _err := internal.NewDataItem(_res)
				if _err != nil {
					log.Println("fixer_data with error:", _err, "try_times=", _try, "input=", _itemRawStr)
					time.Sleep(1 * time.Second)
					continue
				}
				item = newItem
				break
			}

			if _res == "" {
				log.Println("skip with empty resp:", item.UniqID())
				continue
			}
		}

		if !conf.sameIndex || _hasChange {
			str := item.String()
			datas_map[item.UniqID()] = str
			datas = append(datas, str)
		}
	}

	if len(datas) < 1 {
		log.Println("not change,skip reindex")
		return
	}

	var brt internal.BulkResult

	err := conf.NewIndex.Host.BulkStream(strings.NewReader(strings.Join(datas, "\n")), &brt)
	checkErr("parse bulk resp failed:", err)

	//	log.Println("bulk resp:", string(body))

	if brt.Errors {
		log.Println("buil resp has error")
	} else {
		log.Println("buil all success")
	}

	//	t,_:=json.Marshal(br)
	//	fmt.Println("br",string(t))
	for _, data := range brt.Items {
		if item, has := data["index"]; has {
			_id := item.UniqID()
			_raw, _ := datas_map[_id]
			if item.Error != "" {
				log.Println("bulk_err,", _id, item.Error, "raw:", strings.TrimSpace(_raw))

			} else {
				log.Println("bulk_suc,", _id, item.Status)

			}
		}
	}
}
