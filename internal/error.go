package internal

import (
	"fmt"
)

type EsError struct {
	ErrorStr string `json:"error"`
}

func (e *EsError) IsError() bool {
	return e.ErrorStr != ""
}

func (e *EsError) Error() error {
	return fmt.Errorf("%s", e.ErrorStr)
}
