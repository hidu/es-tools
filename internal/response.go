package internal

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
)

// EsResult es 结果接口定义
type EsResult interface {
	// 	IsError()(b bool)
	// 	Error()(error)
}

// ResponseBase 所有response的基类
type ResponseBase struct {
	ErrorStr string `json:"error"`
	Raw      string `json:"-"` // 原始的resp
}

// IsError 是否有错
func (e *ResponseBase) IsError() bool {
	return e.ErrorStr != ""
}

// Error 返回错误
func (e *ResponseBase) Error() error {
	return errors.New(e.ErrorStr)
}

// RawResp 原始的response内容
func (e *ResponseBase) RawResp() string {
	return e.Raw
}

// ScanResponse scan 命令的结果
type ScanResponse struct {
	ResponseBase
	ScrollID string `json:"_scroll_id"`
	Token    int    `json:"took"`
	TimedOut bool   `json:"timed_out"`
	Hits     struct {
		Total uint64 `json:"total"`
	}
}

func (sr *ScanResponse) String() string {
	bf, _ := json.MarshalIndent(sr, " ", "  ")
	return string(bf)
}

// ScrollResponse scroll的返回结果
type ScrollResponse struct {
	ResponseBase
	ScrollID string `json:"_scroll_id"`
	Token    int    `json:"took"`
	TimedOut bool   `json:"timed_out"`
	Hits     *struct {
		Total uint64      `json:"total"`
		Hits  []*DataItem `json:"hits"`
	} `json:"hits"`
}

// HasMore 是否有更多
func (sr *ScrollResponse) HasMore() bool {
	return sr.Hits != nil && len(sr.Hits.Hits) > 0
}

func (sr *ScrollResponse) String() string {
	bf, _ := json.MarshalIndent(sr, " ", "  ")
	return string(bf)
}

// NewDataItem 创建一条结果数据
func NewDataItem(str string) (*DataItem, error) {
	var item *DataItem
	dec := json.NewDecoder(strings.NewReader(str))
	dec.UseNumber()
	err := dec.Decode(&item)

	if err != nil {
		return nil, err
	}
	if item.Index == "" || item.Type == "" || item.ID == "" {
		return nil, fmt.Errorf("_index, _type, _id is empty, input=%q", str)
	}

	if item.Source == nil {
		return nil, fmt.Errorf("_source is empty, input=%q", str)
	}

	return item, err
}

// DataItem es 查询结果的一条数据
type DataItem struct {
	Index  string                 `json:"_index"`
	Type   string                 `json:"_type"`
	ID     string                 `json:"_id"`
	Source map[string]interface{} `json:"_source"`
}

// String 序列化
func (item *DataItem) String() string {
	return string(item.JSONBytes())
}

// BulkString 输出为用于bulk命令的字符串
func (item *DataItem) BulkString() string {
	header := map[string]interface{}{
		"index": map[string]string{
			"_index": item.Index,
			"_type":  item.Type,
			"_id":    item.ID,
		},
	}
	hd, _ := json.Marshal(header)
	bd, _ := json.Marshal(item.Source)

	var builder strings.Builder
	builder.Write(hd)
	builder.WriteByte('\n')
	builder.Write(bd)
	builder.WriteByte('\n')
	return builder.String()
}

// JSONString 序列化为json
func (item *DataItem) JSONBytes() []byte {
	s, err := json.Marshal(item)
	if err != nil {
		return []byte(err.Error())
	}
	return s
}

// UniqID 返回数据唯一id
func (item *DataItem) UniqID() string {
	return strings.Join([]string{
		item.Index,
		item.Type,
		item.ID,
	}, "|")
}

// BulkResponse bulk命令的response
type BulkResponse struct {
	ResponseBase
	Took   uint64                       `json:"took"`
	Errors bool                         `json:"errors"`
	Items  []map[string]*BulkResultItem `json:"items"`
}

// BulkResultItem bulk命令的一条数据
type BulkResultItem struct {
	Index   string `json:"_index"`
	Type    string `json:"_type"`
	ID      string `json:"_id"`
	Version uint64 `json:"_version"`
	Status  int    `json:"status"`
	Error   string `json:"error"`
}

// UniqID 唯一id
func (bri *BulkResultItem) UniqID() string {
	return strings.Join([]string{
		bri.Index,
		bri.Type,
		bri.ID,
	}, "|")
}

func (bri *BulkResponse) String() string {
	bs, err := json.Marshal(bri)
	if err != nil {
		return err.Error()
	}
	return string(bs)
}
