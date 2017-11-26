package internal

import (
	"fmt"
)

type EsResult interface {
	//	IsError()(b bool)
	//	Error()(error)
}

type EsResp struct {
	ErrorStr string `json:"error"`
	Raw      string `json:"-"` //原始的resp
}

func (e *EsResp) IsError() bool {
	return e.ErrorStr != ""
}

func (e *EsResp) Error() error {
	return fmt.Errorf("%s", e.ErrorStr)
}
func (e *EsResp) RawResp() string {
	return e.Raw
}
