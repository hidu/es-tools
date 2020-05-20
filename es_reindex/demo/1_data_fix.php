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