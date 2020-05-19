/*
 * Copyright(C) 2020 github.com/hidu  All Rights Reserved.
 * Author: hidu (duv123+git@baidu.com)
 * Date: 2020/5/19
 */

package main

import (
	"testing"

	"github.com/hidu/es-tools/internal"
)

func TestIndexInfo_IndexURI(t *testing.T) {
	type fields struct {
		Host    *internal.Host
		DocType *internal.DocType
	}
	tests := []struct {
		name   string
		fields fields
		want   string
	}{
		{
			name: "case 1",
			fields: fields{
				Host: &internal.Host{
					Address:  "http://127.0.0.1:8090",
					Header:   nil,
					User:     "",
					Password: "",
					Vs:       nil,
				},
				DocType: &internal.DocType{
					Index: "index",
					Type:  "type",
				},
			},
			want: "http://127.0.0.1:8090/index/type",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			i := &IndexInfo{
				Host:    tt.fields.Host,
				DocType: tt.fields.DocType,
			}
			if got := i.IndexURI(); got != tt.want {
				t.Errorf("IndexURI() = %v, want %v", got, tt.want)
			}
		})
	}
}
