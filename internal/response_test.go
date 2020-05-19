/*
 * Copyright(C) 2020 github.com/hidu  All Rights Reserved.
 * Author: hidu (duv123+git@baidu.com)
 * Date: 2020/5/19
 */

package internal

import (
	"testing"
)

func TestDataItem_UniqID(t *testing.T) {
	type fields struct {
		Index  string
		Type   string
		ID     string
		Source map[string]interface{}
	}
	tests := []struct {
		name   string
		fields fields
		want   string
	}{
		{
			name: "case 1",
			fields: fields{
				Index:  "index",
				Type:   "type",
				ID:     "id",
				Source: nil,
			},
			want: "index|type|id",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			item := &DataItem{
				Index:  tt.fields.Index,
				Type:   tt.fields.Type,
				ID:     tt.fields.ID,
				Source: tt.fields.Source,
			}
			if got := item.UniqID(); got != tt.want {
				t.Errorf("UniqID() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestBulkResultItem_UniqID(t *testing.T) {
	type fields struct {
		Index   string
		Type    string
		ID      string
		Version uint64
		Status  int
		Error   string
	}
	tests := []struct {
		name   string
		fields fields
		want   string
	}{
		{
			name: "case 1",
			fields: fields{
				Index:   "index",
				Type:    "type",
				ID:      "id",
				Version: 0,
				Status:  0,
				Error:   "",
			},
			want: "index|type|id",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bri := &BulkResultItem{
				Index:   tt.fields.Index,
				Type:    tt.fields.Type,
				ID:      tt.fields.ID,
				Version: tt.fields.Version,
				Status:  tt.fields.Status,
				Error:   tt.fields.Error,
			}
			if got := bri.UniqID(); got != tt.want {
				t.Errorf("UniqID() = %v, want %v", got, tt.want)
			}
		})
	}
}
