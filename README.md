# OPS-Tools
About-MySQL/Linux/Oracle  Tools 

orzdba                         ----- Linux/MySQL Monitor Tools (opensource by AliBaBa)



xa-general-statistic.py        ----- 解析查询日志，获取SELECT、DELETE、UPDATE、INSERT 语句执行情况(opensource by xiepaup)

此工具目前简单粗暴：
使用目的：
1.能够获得一段时间内表上执行 SELECT、DELETE、UPDATE、INSERT 次数，以及DB使用情况
2.标红执行占比大于40% 的表，直观反应不合理请求。
3.能够反应整个业务是否分配均匀，是否存在不合理业务在刷DB

使用方法如下：
+    Usage : python xa-general-statistic.py general.log 10:10:10    
+    general.log ---> 需要解析的general log 日志文件                
+    10:10:10    ---> 文件结束时间（这个时间目前没有记录再文件里，只能手动输入囖）

使用效果如下：
[root@xxxxx-000 xa]# python scripts/xa-general-statistic.py general-log-2016-08-08.log 17:59:12
     -----------------------------------------------------------------
     -                                                               -
     - This Time Total Monitor        548 seconds                    -
     - As Follow is Top  15 Execute Table Statistic Info             -
     - General Log Execute Between 17:50:04 and 17:59:12             -
     -                                                               -
     -----------------------------------------------------------------
SELECT total Executed : 833532
--------------------------------------------------------------------------
| 序列号 |执行占比 | 每秒执行 | 总执行次数|           执行表名           |
--------------------------------------------------------------------------
|    1   |  75.32% |  2820.90 |     44332 |                   xxxxxxxxxxx|    －－－ 这种明显不合理咯
|    2   |   4.99% |    75.94 |     41614 |                xxxxxxxxxxxxxx|
|    3   |   4.96% |    75.52 |     41383 |                xxxxxxxxxxxxxx|
|    4   |   4.89% |    74.35 |     40746 |                   xxxxxxxxxxx|
--------------------------------------------------------------------------
INSERT total Executed : 18281
--------------------------------------------------------------------------
| 序列号 |执行占比 | 每秒执行 | 总执行次数|           执行表名           |
--------------------------------------------------------------------------
|    1   |   8.60% |     2.87 |      1572 |                   xxxxxxxxxxx|
|    2   |   8.57% |     2.86 |      1566 |                   xxxxxxxxxxx|
|    3   |   8.24% |     2.75 |      1506 |    xxxxxxxxxxxxxxxxxxxxxxxxxx|
--------------------------------------------------------------------------



binlog-rollbakc.pl      使用说明：
混滚误操作语句：update js_landing_page set goodsflowkey='zdy_cps_kai_si_sheng_yang',sort=5 where sort=6; 

1.首先拿到 这行这条语句的开始时间以及结束时间：
2.其次拿到 js_landing_page 这张表的表结构，在一个测试db 上建好一个空表
3.把对应的binlog scp 到具有 该回滚脚步的服务器上
4.执行脚步得到 反解后的结果
---说明：
    该脚本先是完全利用mysqlbinlog 工具解析出这个binlog 里边的内容
    然后再到脚本层面过滤掉 filter，得出反解结果




------------------------------------------
redis_key_distribution.py  统计redis key类型数据大小分布
原理：使用redis命令： scan、pipline、type 和 debug object  来得到 redis key 信息

----------------------------------------------------------------------------------------------------------------------------------------
统计时间:[Fri Jul 29 17:06:29 2016 ~ Fri Jul 29 17:06:29 2016]
Redis服务器[127.0.0.1:6388]
数据类型和数据大小分布情况如下:
----------------------------------------------------------------------------------------------------------------------------------------

----------------------------------------------------------------------------------------------------------------------------------------
|KEY TYPE | KEY COUNT | KEY 64(byte) | KEY 128 | Key 512 | Key 1024 | Key 2048 | Key 3072 | Key 4096 | Key 5120 | Key 6044 | Key large |
| String  |       2   |       2      |       0 |       0 |        0 |       0  |       0  |       0  |       0  |       0  |        0  |
|   LIST  |       1   |       1      |       0 |       0 |        0 |       0  |       0  |       0  |       0  |       0  |        0  |
|   HASH  |       2   |       2      |       0 |       0 |        0 |       0  |       0  |       0  |       0  |       0  |        0  |
|    SET  |       1   |       1      |       0 |       0 |        0 |       0  |       0  |       0  |       0  |       0  |        0  |
|   ZSET  |       1   |       1      |       0 |       0 |        0 |       0  |       0  |       0  |       0  |       0  |        0  |
----------------------------------------------------------------------------------------------------------------------------------------





Twemproxy 命令支持列表：
https://raw.githubusercontent.com/twitter/twemproxy/master/notes/redis.md




----Add Redis Tools ,modify at redis-cli , function --bigkeys

add an arguments , --bigkey-numb 

VITOXIE-MB1:src xiean$ ./redis-cli-new -p 2837 --bigkeys --bigkey-numb  3

Biggest string Key Top   1  found 'str.de' has 5 bytes
Biggest string Key Top   2  found 'str.ee' has 5 bytes
Biggest string Key Top   3  found 'str.d' has 4 bytes
Biggest   hash Key Top   1  found 'hash.a' has 2 fields
