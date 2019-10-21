package internal

import (
	"encoding/json"
	"fmt"
	"strings"
)

type ScrollResult struct {
	EsResp
	ScrollID string `json:"_scroll_id"`
	Token    int    `json:"took"`
	TimedOut bool   `json:"timed_out"`
	Hits     *struct {
		Total uint64      `json:"total"`
		Hits  []*DataItem `json:"hits"`
	} `json:"hits"`
}

type ScanResult = ScrollResult

func (srt *ScrollResult) HasMore() bool {
	return srt.Hits != nil && len(srt.Hits.Hits) > 0
}

func (c *ScrollResult) String() string {
	bf, _ := json.MarshalIndent(c, " ", "  ")
	return string(bf)
}

func NewDataItem(str string) (*DataItem, error) {
	var item *DataItem
	dec := json.NewDecoder(strings.NewReader(str))
	dec.UseNumber()
	err := dec.Decode(&item)
	//	err := json.Unmarshal([]byte(str), &item)

	if err != nil {
		return nil, err
	}
	if item.Index == "" || item.Type == "" || item.ID == "" {
		return nil, fmt.Errorf("_index,_type,_id is empty,input=%s", str)
	}

	if item.Source == nil {
		return nil, fmt.Errorf("_source is empty,input=%s", str)
	}

	return item, err
}

type DataItem struct {
	Index  string                 `json:"_index"`
	Type   string                 `json:"_type"`
	ID     string                 `json:"_id"`
	Source map[string]interface{} `json:"_source"`
}

func (item *DataItem) String() string {
	header := map[string]interface{}{
		"index": map[string]string{
			"_index": item.Index,
			"_type":  item.Type,
			"_id":    item.ID,
		},
	}
	hd, _ := json.Marshal(header)
	bd, _ := json.Marshal(item.Source)
	return string(hd) + "\n" + string(bd) + "\n"
}

func (item *DataItem) JsonString() string {
	s, _ := json.Marshal(item)
	return string(s)
}

func (item *DataItem) UniqID() string {
	return fmt.Sprintf("%s|%s|%s", item.Index, item.Type, item.ID)
}

type BulkResult struct {
	EsResp
	Took   uint64                       `json:"took"`
	Errors bool                         `json:"errors"`
	Items  []map[string]*BulkResultItem `json:"items"`
}

type BulkResultItem struct {
	Index   string `json:"_index"`
	Type    string `json:"_type"`
	Id      string `json:"_id"`
	Version uint64 `json:"_version"`
	Status  int    `json:"status"`
	Error   string `json:"error"`
}

func (bri *BulkResultItem) UniqID() string {
	return fmt.Sprintf("%s|%s|%s", bri.Index, bri.Type, bri.Id)
}

func (bri *BulkResult) JsonString() string {
	bs, _ := json.Marshal(bri)
	return string(bs)
}
