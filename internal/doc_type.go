package internal

import (
	"strings"
)

type DocType struct {
	Index string `json:"index"`
	Type  string `json:"type"`
}

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
