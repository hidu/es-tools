/*
 * Copyright(C) 2020 github.com/hidu  All Rights Reserved.
 * Author: hidu (duv123+git@baidu.com)
 * Date: 2020/5/19
 */

package internal

import (
	"testing"
)

func TestDocType_Uri(t *testing.T) {
	type fields struct {
		Index string
		Type  string
	}
	tests := []struct {
		name   string
		fields fields
		want   string
	}{
		{
			name: "case 1",
			fields: fields{
				Index: "index",
				Type:  "type",
			},
			want: "/index/type",
		},
		{
			name: "case 1",
			fields: fields{
				Index: "index",
			},
			want: "/index",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := &DocType{
				Index: tt.fields.Index,
				Type:  tt.fields.Type,
			}
			if got := d.URI(); got != tt.want {
				t.Errorf("URI() = %v, want %v", got, tt.want)
			}
		})
	}
}
