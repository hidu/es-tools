package internal

import (
	"fmt"
	"log"
	"time"
)

type Scroll struct {
	host       *Host
	doc        *DocType
	query      *Query
	scroll_id  string
	second     int
	loop_no    uint64
	total      uint64
	scroll_pos uint64
}

func NewScroll(host *Host, doc *DocType, query *Query) *Scroll {
	return &Scroll{
		host:   host,
		doc:    doc,
		query:  query,
		second: 120,
	}
}

func (s *Scroll) SetScanTime(sec int) {
	s.second = sec
}

func (s *Scroll) scrollTime() string {
	return fmt.Sprintf("%ds", s.second)
}

func (s *Scroll) Next() (*ScrollResult, error) {
	var srt *ScrollResult
	var err error

	s.loop_no++

	if s.scroll_id == "" { // 首次扫描或scroll_id为空的情况
		for {
			srt, err = s.scan()

			for i := 0; i < 3; i++ {
				if err != nil {
					time.Sleep(time.Second)
					continue
				}
			}
			if err != nil {
				return nil, err
			}

			if srt != nil {
				if srt.IsError() {
					return nil, srt.Error()
				}
				s.scroll_id = srt.ScrollID
				s.total = srt.Hits.Total
				break
			}
		}
	} else { // 非首次扫描或指定scroll_id
		scanPost := `{"scroll": "10m","scroll_id": "` + s.scroll_id + `"}`

		for i := 0; i < 20; i++ {
			err = s.host.DoRequest("GET", "/_search/scroll", scanPost, &srt)
			if err != nil {
				log.Printf("[err] search_scroll failed,try=%d/20,error=%s\n", i, err.Error())
				time.Sleep(time.Second)
				continue
			}
			break
		}
	}

	if s.scroll_id == "" {
		return nil, fmt.Errorf("get scroll_id failed")
	}

	if srt.IsError() {
		s.host.speed.Fail("scroll_next", 1)
		return nil, srt.Error()
	}

	s.scroll_pos += uint64(len(srt.Hits.Hits))

	s.scroll_id = srt.ScrollID

	s.host.speed.Success("scroll_next", 1)
	s.host.speed.Success("scroll_result_items", len(srt.Hits.Hits))

	log.Printf("[info] scroll_next result,loop_no=%d,total=%d,scroll_pos=%d\n", s.loop_no, s.total, s.scroll_pos)

	return srt, nil
}

// https://www.elastic.co/guide/en/elasticsearch/reference/5.4/breaking_50_search_changes.html#_literal_search_type_scan_literal_removed

func (s *Scroll) scan() (*ScanResult, error) {
	uri := s.doc.Uri() + "/_search?scroll=" + s.scrollTime()
	if !s.host.Vs.Gt("5.0.0") {
		uri += "&search_type=scan"
	}
	var sr *ScanResult
	qs := s.query.String()
	err := s.host.DoRequest("GET", uri, qs, &sr)
	log.Println("[info] scan,error=", err, ",result=", sr, ",uri=", uri, ",query=", qs)
	return sr, err
}

func (s *Scroll) Total() uint64 {
	return s.total
}
