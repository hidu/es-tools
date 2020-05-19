package internal

import (
	"encoding/base64"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	// "reflect"
	"strconv"
	"strings"

	"github.com/hidu/go-speed"
)

// ResponseVersion 集群信息
type ResponseVersion struct {
	ResponseBase
	Name        string                 `json:"name"`
	ClusterName string                 `json:"cluster_name"`
	ClusterUUID string                 `json:"cluster_uuid"`
	VersionData map[string]interface{} `json:"version"`
	Tagline     string                 `json:"tagline"`
}

// Gt 比较版本大小
func (vs *ResponseVersion) Gt(version string) bool {
	number, has := vs.VersionData["number"].(string)
	if !has {
		return false
	}
	numberArr := strings.Split(number, ".")
	versionArr := strings.Split(version, ".")

	for i, vs := range versionArr {
		vsI, err0 := strconv.ParseInt(vs, 10, 64)
		if err0 != nil {
			return false
		}

		if len(numberArr) < i {
			return false
		}
		numI, err1 := strconv.ParseInt(numberArr[i], 10, 64)
		if err1 != nil {
			return false
		}

		if vsI != numI {
			return numI > vsI
		}
	}
	return false
}

func (vs *ResponseVersion) String() string {
	s, _ := jsonEncode(vs)
	return s
}

// Host es的host配置信息
type Host struct {
	// Address 主机host信息，eg：http://127.0.0.1:8080
	Address string `json:"addr"`

	// Header http header
	Header map[string]string `json:"header"`

	// User basic 认证的用户名
	User string `json:"user"`

	// Password basic 认证的密码
	Password string `json:"password"`

	client *http.Client
	speed  *speed.Speed

	Vs *ResponseVersion `json:"-"`
}

// Init 初始化
func (h *Host) Init() error {
	if h.speed != nil {
		return nil
	}
	u, err := url.Parse(h.Address)
	if err != nil {
		return err
	}
	if h.User == "" && u.User != nil && u.User.Username() != "" {
		h.User = u.User.Username()
		h.Password, _ = u.User.Password()
	}
	if h.Header == nil {
		h.Header = make(map[string]string)
	}

	if h.User != "" {
		ps := fmt.Sprintf("%s:%s", h.User, h.Password)
		h.Header["Authorization"] = fmt.Sprintf("Basic %s", base64.StdEncoding.EncodeToString([]byte(ps)))
	}

	if _, has := h.Header["User-Agent"]; !has {
		h.Header["User-Agent"] = "hidu_es-tools"
	}

	log.Println("Header:", h.Header)

	if h.client == nil {
		h.client = &http.Client{}
	}
	h.speed = speed.NewSpeed("es", 5, nil)

	h.Vs = &ResponseVersion{}
	err = h.DoRequest("GET", "/", "", &h.Vs)

	if err != nil {
		return err
	}
	log.Println("version_info:", h.Vs)

	if !h.Vs.Gt("1.0.0") {
		err = fmt.Errorf("wrong version < 1.0.0")
	}
	return err
}

// DoRequestStream 发送并获取解析结果
func (h *Host) DoRequestStream(method string, uri string, payload io.Reader, result EsResult) error {
	h.Init()

	urlStr := fmt.Sprintf("%s%s", h.Address, uri)
	req, err := http.NewRequest(method, urlStr, payload)
	if err != nil {
		return err
	}

	if h.Header != nil {
		for k, v := range h.Header {
			req.Header.Set(k, v)
		}
	}
	req.Header.Set("Content-Type", "application/json")

	// bf,_:=httputil.DumpRequest(req,true)
	// log.Println("request=",string(bf))

	resp, err := h.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	bd, err := ioutil.ReadAll(resp.Body)

	if err != nil {
		return err
	}

	e := jsonDecode(bd, &result)

	// 	if result != nil {
	// 		raw:=reflect.ValueOf(result).Elem()
	// 		fmt.Println(raw.Type())
	// 		f0:=raw.FieldByName("Raw")
	// 		if f0.IsValid(){
	// 			f0.SetString(string(bd))
	// 		}
	// 	}

	return e
}

// DoRequest 发送请求
func (h *Host) DoRequest(method string, uri string, payload string, result EsResult) error {
	return h.DoRequestStream(method, uri, strings.NewReader(payload), result)
}

// BulkStream 发送bulk请求
func (h *Host) BulkStream(stream io.Reader, result *BulkResponse) error {
	err := h.DoRequestStream("POST", "/_bulk", stream, &result)
	if err == nil {
		h.speed.Success("bulk_items", len(result.Items))
	}
	return err
}
