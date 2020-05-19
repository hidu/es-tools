/*
 * Copyright(C) 2020 github.com/hidu  All Rights Reserved.
 * Author: hidu (duv123+git@baidu.com)
 * Date: 2020/5/19
 */

package internal

import (
	"flag"
	"fmt"
)

func init() {
	ua := flag.Usage
	flag.Usage = func() {
		ua()
		fmt.Println("\n site: https://github.com/hidu/es-tools/")
		fmt.Println(" version:", "20200519 1.2")
	}
}
