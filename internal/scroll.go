package internal

import (
	"encoding/json"
	"fmt"
	"log"
	"time"
)

// Scroll es scroll 操作
type Scroll struct {
	host      *Host
	doc       *DocType
	query     *Query
	scrollID  string
	second    int
	loopNo    uint64
	total     uint64
	scrollPos uint64
}

// NewScroll 创建一个scroll命令
func NewScroll(host *Host, doc *DocType, query *Query) *Scroll {
	return &Scroll{
		host:   host,
		doc:    doc,
		query:  query,
		second: 120,
	}
}

// SetScanTime 设置scan会话有效期
func (s *Scroll) SetScanTime(sec int) {
	s.second = sec
}

func (s *Scroll) scrollTime() string {
	return fmt.Sprintf("%ds", s.second)
}

// Next 获取下一页数据
func (s *Scroll) Next() (*ScrollResponse, error) {

	if s.scrollID == "" {
		for {
			sr, err := s.scan()

			for i := 0; i < 10; i++ {
				if err != nil {
					time.Sleep(time.Second)
					continue
				}
			}
			if err != nil {
				return nil, err
			}

			if sr != nil {
				if sr.IsError() {
					return nil, sr.Error()
				}
				s.scrollID = sr.ScrollID
				s.total = sr.Hits.Total
				break
			}
		}

	}
	s.loopNo++

	if s.scrollID == "" {
		return nil, fmt.Errorf("get scroll_id failed")
	}

	scanURI := "/_search/scroll?scroll=" + s.scrollTime()

	var srt *ScrollResponse

	for try := 0; try < 100; try++ {
		postData := map[string]string{
			"scroll":    s.scrollTime(),
			"scroll_id": s.scrollID,
		}

		bf, errJSON := json.Marshal(postData)
		if errJSON != nil {
			log.Fatalf("json.Marshal with error, data=%v, err=%v", postData, errJSON)
		}
		err := s.host.DoRequest("GET", scanURI, string(bf), &srt)
		if err != nil {
			log.Printf("[err] search_scroll failed, try=%d/100, error=%s\n", try, err.Error())
			time.Sleep(time.Second)
			continue
		}
		break
	}

	if srt.IsError() {
		s.host.speed.Fail("scroll_next", 1)
		return nil, srt.Error()
	}

	s.scrollPos += uint64(len(srt.Hits.Hits))

	s.scrollID = srt.ScrollID

	s.host.speed.Success("scroll_next", 1)
	s.host.speed.Success("scroll_result_items", len(srt.Hits.Hits))

	log.Printf("[info] scroll_next result, loopNo=%d, total=%d, scrollPos=%d\n", s.loopNo, s.total, s.scrollPos)

	return srt, nil
}

// https://www.elastic.co/guide/en/elasticsearch/reference/5.4/breaking_50_search_changes.html#_literal_search_type_scan_literal_removed
func (s *Scroll) scan() (*ScanResponse, error) {
	uri := s.doc.URI() + "/_search?scroll=" + s.scrollTime()
	if !s.host.Vs.Gt("5.0.0") {
		uri += "&search_type=scan"
	}
	var sr *ScanResponse
	qs := s.query.String()
	err := s.host.DoRequest("GET", uri, qs, &sr)
	log.Println("[info] scan, error=", err, ", result=", sr, ", uri=", uri, ", query=", qs)
	return sr, err
}

// Total 匹配的数据总条数
func (s *Scroll) Total() uint64 {
	return s.total
}
