es_reindex
===
重建es索引的工具


## install

```
go get -u github.com/hidu/es-tools/es_reindex
```

## useage

>es_reindex -conf test.json


`test.json` 配置文件
```json
{
    "origin_index":{
        "host":{
            "addr":"http://127.0.0.1:8666/",
            "header":{},
            "user":"",
            "password":""
        },
        "type":{
            "index":"mbd_relation_forb",
            "type":"可选字段"
        }
    },
     "new_index":{
        "host":{
            "addr":"http://127.0.0.1:8666/",
            "header":{},
            "user":"",
            "password":""
        },
        "type":{
            "index":"test1",
            "type":"可选字段"
        }
    },
    "scan_query":{
        "size":100
    },
    "scan_time":"180s",
    "data_fix_cmd":"php 1_data_fix.php"
}
```

说明：  
1. `origin_index`: 原始index的配置 (`origin_index.type.type` 是可选的)  
2. `new_index`： 可选，索引写入的host 配置 (`new_index.type` 为可选),若new_index 不存在，则还是写入`origin_index`
3. `scan_query`: 进行scan 时的查询条件
4. `scan_time`: scan的时间
5. `data_fix_cmd`: 可选，调用另外一个进程来对数据进行修正处理


1_data_fix.php 文件示例：

```php
<?php
// 每次读入一行，然后输出一行
$idx=0;
while(!feof(STDIN)){
    $line=fgets(STDIN);
    
    $arr=json_decode($line,true);
    $arr['_source']['data']=$idx++;
    if($arr['_source']['ts']<1){
        echo "\n"; // reindex 的时候跳过这条数据
    }else{
        echo json_encode($arr)."\n";
        // 输出处理后的数据，必须以一个回车符结尾
    }
}
```
