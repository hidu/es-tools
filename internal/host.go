package internal

import (
	"encoding/base64"
	"fmt"
	"github.com/hidu/go-speed"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"strconv"
	"strings"
)

type Version struct {
	Name        string                 `json:"name"`
	ClusterName string                 `json:"cluster_name"`
	ClusterUUID string                 `json:"cluster_uuid"`
	VersionData map[string]interface{} `json:"version"`
	Tagline     string                 `json:"tagline"`
}

func (vs *Version) Gt(version string) bool {
	number, has := vs.VersionData["number"].(string)
	if !has {
		return false
	}
	number_arr := strings.Split(number, ".")
	version_arr := strings.Split(version, ".")

	for i, vs := range version_arr {
		vs_i, err_0 := strconv.ParseInt(vs, 10, 64)
		if err_0 != nil {
			return false
		}

		if len(number_arr) < i {
			return false
		}
		num_i, err_1 := strconv.ParseInt(number_arr[i], 10, 64)
		if err_1 != nil {
			return false
		}

		if vs_i != num_i {
			return num_i > vs_i
		}
	}
	return false
}

func (vs *Version) String() string {
	s, _ := jsonEncode(vs)
	return s
}

type Host struct {
	AddressUrl string            `json:"addr"`
	Header     map[string]string `json:"header"`
	User       string            `json:"user"`
	Password   string            `json:"password"`
	client     *http.Client      `json:"-"`
	speed      *speed.Speed      `json:"-"`
	Vs         *Version          `json:"-"`
}

func (h *Host) Init() error {
	if h.speed != nil {
		return nil
	}
	u, err := url.Parse(h.AddressUrl)
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

	if h.client == nil {
		h.client = &http.Client{}
	}
	h.speed = speed.NewSpeed("es", 5, nil)

	h.Vs = &Version{}
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

func (h *Host) DoRequestStream(method string, uri string, playload io.Reader, result interface{}) error {
	h.Init()

	urlStr := fmt.Sprintf("%s%s", h.AddressUrl, uri)
	req, err := http.NewRequest(method, urlStr, playload)
	if err != nil {
		return err
	}

	if h.Header != nil {
		for k, v := range h.Header {
			req.Header.Set(k, v)
		}
	}
	resp, err := h.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	bd, err := ioutil.ReadAll(resp.Body)

	if err != nil {
		return err
	}
	return jsonDecode(bd, &result)
}
func (h *Host) DoRequest(method string, uri string, playload string, result interface{}) error {
	return h.DoRequestStream(method, uri, strings.NewReader(playload), &result)
}

func (h *Host) BulkStream(stream io.Reader, result *BulkResult) error {
	return h.DoRequestStream("POST", "/_bulk", stream, &result)
}
