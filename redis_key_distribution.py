#!/usr/bin/python
# -*- coding: utf-8 -*-
import getopt
import time
import sys
import redis
import os
reload(sys)
sys.setdefaultencoding('utf8')

__Version__ = "1.5.1"
__AUTHOR__  = "xiean"
__EMAIL__   = "xiepaup@163.com"
__DATE__    = "2016-07-29"


#LIST FORMAT
#0   1     2     3     4      5      6      7       8     9      10    11 
#ALL <=64 <=128 <=256 <=512 <=1024 <=2048 <=3072 <=4096 <=5120 <=6044 >6044
string_size_counter_list = [0,0,0,0,0,0,0,0,0,0,0,0]
list_size_counter_list   = [0,0,0,0,0,0,0,0,0,0,0,0]
hash_size_counter_list   = [0,0,0,0,0,0,0,0,0,0,0,0]
set_size_counter_list    = [0,0,0,0,0,0,0,0,0,0,0,0]
zset_size_counter_list   = [0,0,0,0,0,0,0,0,0,0,0,0]



def size_counter_do_count(keySizeCounterList,keySize):
   keySizeCounterList[0] += 1   #// Add all global counter
   if keySize <= 64:
      keySizeCounterList[1]  += 1
   elif keySize <= 128:
      keySizeCounterList[2] += 1
   elif keySize <= 512:
      keySizeCounterList[3] += 1
   elif keySize <= 1024:
      keySizeCounterList[4] += 1
   elif keySize <= 2048:
      keySizeCounterList[5] += 1
   elif keySize <= 3072:
      keySizeCounterList[6] += 1
   elif keySize <= 4096:
      keySizeCounterList[7] += 1
   elif keySize <= 5120:
      keySizeCounterList[8] += 1
   elif keySize <= 6044:
      keySizeCounterList[9] += 1
   else:
      keySizeCounterList[10] += 1
   return keySizeCounterList


def key_size_static(keyType,keySize):
   global string_size_counter_list
   global list_size_counter_list  
   global hash_size_counter_list  
   global set_size_counter_list   
   global zset_size_counter_list  

   if keyType == "string":
      string_size_counter_list = size_counter_do_count(string_size_counter_list,keySize)
   elif keyType == "hash":
      hash_size_counter_list   = size_counter_do_count(hash_size_counter_list,keySize)
   elif keyType == "list":
      list_size_counter_list   = size_counter_do_count(list_size_counter_list,keySize)
   elif keyType == "set":
      set_size_counter_list    = size_counter_do_count(set_size_counter_list,keySize)
   elif keyType == "zset":
      zset_size_counter_list   = size_counter_do_count(zset_size_counter_list,keySize)
   else:
      print "UNKONW KEY TYPE !!",keyType
   

'''Redis SCAN command got keys '''
def get_key(rdbConn,start):
   try:
      keys_list = rdbConn.scan(start,count=20)
      return keys_list
   except Exception,e:
      print e

''' Redis DEBUG OBJECT command got key info '''
def get_key_info(rdbConn,keyName):
    try:
       rpiple = rdbConn.pipeline()
       rpiple.type(keyName)
       rpiple.debug_object(keyName)
       key_info_list = rpiple.execute()
       return key_info_list
    except Exception,e:
       print "INFO : ",e


def redis_key_static(key_info_list):

    keyType = key_info_list[0]
    keySize = key_info_list[1]['serializedlength']
    key_size_static(keyType,keySize)



'''Print Key distrubution '''
def show_static_info(host,port,start_time,end_time):
   print u'''----------------------------------------------------------------------------------------------------------------------------------------
统计时间:[%s ~ %s]
Redis服务器[%s:%s]
数据类型和数据大小分布情况如下:
----------------------------------------------------------------------------------------------------------------------------------------

----------------------------------------------------------------------------------------------------------------------------------------
|KEY TYPE | KEY COUNT | KEY 64(byte) | KEY 128 | Key 512 | Key 1024 | Key 2048 | Key 3072 | Key 4096 | Key 5120 | Key 6044 | Key large |
|%7s  | %7s   | %7s      | %7s | %7s |  %7s | %7s  | %7s  | %7s  | %7s  | %7s  |  %7s  |
|%7s  | %7s   | %7s      | %7s | %7s |  %7s | %7s  | %7s  | %7s  | %7s  | %7s  |  %7s  |
|%7s  | %7s   | %7s      | %7s | %7s |  %7s | %7s  | %7s  | %7s  | %7s  | %7s  |  %7s  |
|%7s  | %7s   | %7s      | %7s | %7s |  %7s | %7s  | %7s  | %7s  | %7s  | %7s  |  %7s  |
|%7s  | %7s   | %7s      | %7s | %7s |  %7s | %7s  | %7s  | %7s  | %7s  | %7s  |  %7s  |
----------------------------------------------------------------------------------------------------------------------------------------
''' % (start_time,end_time,host,port,
       'String',string_size_counter_list[0],string_size_counter_list[1],string_size_counter_list[2],string_size_counter_list[3],string_size_counter_list[4],string_size_counter_list[5],string_size_counter_list[6],string_size_counter_list[7],string_size_counter_list[8],string_size_counter_list[9],string_size_counter_list[10],
       'LIST',list_size_counter_list[0],list_size_counter_list[1],list_size_counter_list[2],list_size_counter_list[3],list_size_counter_list[4],list_size_counter_list[5],list_size_counter_list[6],list_size_counter_list[7],list_size_counter_list[8],list_size_counter_list[9],list_size_counter_list[10],
        'HASH',hash_size_counter_list[0],hash_size_counter_list[1],hash_size_counter_list[2],hash_size_counter_list[3],hash_size_counter_list[4],hash_size_counter_list[5],hash_size_counter_list[6],hash_size_counter_list[7],hash_size_counter_list[8],hash_size_counter_list[9],hash_size_counter_list[10],
        'SET',set_size_counter_list[0],set_size_counter_list[1],set_size_counter_list[2],set_size_counter_list[3],set_size_counter_list[4],set_size_counter_list[5],set_size_counter_list[6],set_size_counter_list[7],set_size_counter_list[8],set_size_counter_list[9],set_size_counter_list[10],
        'ZSET',zset_size_counter_list[0],zset_size_counter_list[1],zset_size_counter_list[2],zset_size_counter_list[3],zset_size_counter_list[4],zset_size_counter_list[5],zset_size_counter_list[6],zset_size_counter_list[7],zset_size_counter_list[8],zset_size_counter_list[9],zset_size_counter_list[10])



''' Main Function '''
if __name__ == "__main__":
   host = sys.argv[1]
   port = sys.argv[2]
   start_time = time.ctime()
   rdbConn = redis.Redis(host=host,port=port)
   init_keys = get_key(rdbConn,0)
   while True :
       for key in init_keys[1]:
            key_info = get_key_info(rdbConn,key)
            redis_key_static(key_info) 
       init_keys = get_key(rdbConn,init_keys[0])
       if init_keys[0] == 0:
          break
   end_time = time.ctime()
   show_static_info(host,port,start_time,end_time)
