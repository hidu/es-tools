package internal

import (
	"strings"
)

// DocType 一个索引
type DocType struct {
	Index string `json:"index"`
	Type  string `json:"type"`
}

// URI 索引的请求路径
func (d *DocType) URI() string {
	if d.Type == "" {
		return strings.Join([]string{
			"/",
			d.Index,
		}, "")
	}
	return strings.Join([]string{
		"/",
		d.Index,
		"/",
		d.Type,
	}, "")
}
