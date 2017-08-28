package internal

import (
	"bytes"
	"encoding/json"
	"fmt"
)

func jsonDecode(bs []byte, ret interface{}) error {
	dec := json.NewDecoder(bytes.NewReader(bs))
	dec.UseNumber()
	err := dec.Decode(&ret)
	if err == nil {
		return nil
	}
	return fmt.Errorf("json decode failed,err=(%s),data=(%s)", err, string(bs))
}

func jsonEncode(item interface{}) (string, error) {
	bf, err := json.Marshal(item)
	return string(bf), err
}

func Clone(src interface{}, dest interface{}) error {
	s, err := jsonEncode(src)
	if err != nil {
		return err
	}
	return jsonDecode([]byte(s), dest)
}
