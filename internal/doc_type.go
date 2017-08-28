package internal

import (
	"fmt"
)

type DocType struct {
	Index string `json:"index"`
	Type  string `json:"type"`
}

func (d *DocType) Uri() string {
	if d.Type == "" {
		return fmt.Sprintf("/%s", d.Index)
	}
	return fmt.Sprintf("/%s/%s", d.Index, d.Type)
}
