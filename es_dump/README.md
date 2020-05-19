# es_dump

将ES中的数据通过scroll方式查询导出。  

## 1.安装

```bash
go get -u github.com/hidu/es-tools/es_dump
```

## 2.配置
```json
{
    "origin_index":{
        "host":{
            "addr":"http://127.0.0.1:9200",
            "header":{"from": "es_dump"},
            "user":"",
            "password":""
        },
        "type":{
            "index":"test",
            "type":"type1"
        }
    },
    "scan_query":{
        "size":100
    },
    "scan_time":"180s"
}
```
说明：  
origin_index：待查询的索引信息   
host.user: baisc 认证的账号名   
host.password:  basic 认证的密码
host.header:  其他http header，若不是basic认证，可将认证信息放在这。  


scan_query：查询的语句。可以写更多查询条件。  

## 3.使用
```
 es_dump -conf dump.json
```
目前会将查询结果输出到stdout。  