#!/usr/bin/python
# -*- coding: utf-8 -*-
import getopt
import time
import sys
import redis
import os
reload(sys)
sys.setdefaultencoding('utf8')

__Version__ = "1.8.1"
__AUTHOR__  = "xiean"
__DATE__    = "2016-07-28"
__EMAIL__   = "xiepaup@163.com"

'''
   Function :
            1.        Show All Key type
            2.        Show Key size distrute
            3.        Show Key key counter
            4.        Show Key with no ttl counter
'''


#LIST FORMAT
#0   1     2     3     4      5      6      7       8     9      10    11 
#ALL <=64 <=128 <=256 <=512 <=1024 <=2048 <=3072 <=4096 <=5120 <=6044 >6044
string_size_counter_list = [0,0,0,0,0,0,0,0,0,0,0,0]
list_size_counter_list   = [0,0,0,0,0,0,0,0,0,0,0,0]
hash_size_counter_list   = [0,0,0,0,0,0,0,0,0,0,0,0]
set_size_counter_list    = [0,0,0,0,0,0,0,0,0,0,0,0]
zset_size_counter_list   = [0,0,0,0,0,0,0,0,0,0,0,0]



def size_counter_do_count(keySizeCounterList,keySize,keyTtl):
   keySizeCounterList[0] += 1   #// Add all global counter
   if keyTtl < 0:
      keySizeCounterList[1] += 1
   if keySize <= 64:
      keySizeCounterList[2]  += 1
   elif keySize <= 128:
      keySizeCounterList[3] += 1
   elif keySize <= 512:
      keySizeCounterList[4] += 1
   elif keySize <= 1024:
      keySizeCounterList[5] += 1
   elif keySize <= 2048:
      keySizeCounterList[6] += 1
   elif keySize <= 3072:
      keySizeCounterList[7] += 1
   elif keySize <= 4096:
      keySizeCounterList[8] += 1
   elif keySize <= 5120:
      keySizeCounterList[9] += 1
   elif keySize <= 6044:
      keySizeCounterList[10] += 1
   else:
      keySizeCounterList[11] += 1
   return keySizeCounterList


def key_size_static(keyType,keySize,keyTtl):
   global string_size_counter_list
   global list_size_counter_list  
   global hash_size_counter_list  
   global set_size_counter_list   
   global zset_size_counter_list  

   if keyType == "string":
      string_size_counter_list = size_counter_do_count(string_size_counter_list,keySize,keyTtl)
   elif keyType == "hash":
      hash_size_counter_list   = size_counter_do_count(hash_size_counter_list,keySize,keyTtl)
   elif keyType == "list":
      list_size_counter_list   = size_counter_do_count(list_size_counter_list,keySize,keyTtl)
   elif keyType == "set":
      set_size_counter_list    = size_counter_do_count(set_size_counter_list,keySize,keyTtl)
   elif keyType == "zset":
      zset_size_counter_list   = size_counter_do_count(zset_size_counter_list,keySize,keyTtl)
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
       rpiple.ttl(keyName)
       key_info_list = rpiple.execute()
       return key_info_list
    except Exception,e:
       print "INFO : ",e

def redis_key_static(key_info_list):

    keyType = key_info_list[0]
    keySize = key_info_list[1]['serializedlength']
    keyTtl  = key_info_list[2]
    key_size_static(keyType,keySize,keyTtl)



'''Print Key distrubution '''
def show_static_info(host,port,start_time,end_time):
   print u'''----------------------------------------------------------------------------------------------------------------------------------------------------
统计时间:[%s ~ %s]
Redis服务器[%s:%s]
数据类型和数据大小分布情况如下:
----------------------------------------------------------------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------------------------------------------
|KEY TYPE | KEY COUNT | KEY No TTL | KEY 64(byte) | KEY 128 | Key 512 | Key 1024 | Key 2048 | Key 3072 | Key 4096 | Key 5120 | Key 6044 | Key large |
|%7s  |   %7s  | %7s   | %7s      | %7s | %7s |  %7s | %7s  | %7s  | %7s  | %7s  | %7s  |  %7s  |
|%7s  |   %7s  | %7s   | %7s      | %7s | %7s |  %7s | %7s  | %7s  | %7s  | %7s  | %7s  |  %7s  |
|%7s  |   %7s  | %7s   | %7s      | %7s | %7s |  %7s | %7s  | %7s  | %7s  | %7s  | %7s  |  %7s  |
|%7s  |   %7s  | %7s   | %7s      | %7s | %7s |  %7s | %7s  | %7s  | %7s  | %7s  | %7s  |  %7s  |
|%7s  |   %7s  | %7s   | %7s      | %7s | %7s |  %7s | %7s  | %7s  | %7s  | %7s  | %7s  |  %7s  |
----------------------------------------------------------------------------------------------------------------------------------------------------
''' % (start_time,end_time,host,port,
       'String',string_size_counter_list[0],string_size_counter_list[1],string_size_counter_list[2],string_size_counter_list[3],string_size_counter_list[4],string_size_counter_list[5],string_size_counter_list[6],string_size_counter_list[7],string_size_counter_list[8],string_size_counter_list[9],string_size_counter_list[10],string_size_counter_list[11],
       'LIST',list_size_counter_list[0],list_size_counter_list[1],list_size_counter_list[2],list_size_counter_list[3],list_size_counter_list[4],list_size_counter_list[5],list_size_counter_list[6],list_size_counter_list[7],list_size_counter_list[8],list_size_counter_list[9],list_size_counter_list[10],list_size_counter_list[11],
        'HASH',hash_size_counter_list[0],hash_size_counter_list[1],hash_size_counter_list[2],hash_size_counter_list[3],hash_size_counter_list[4],hash_size_counter_list[5],hash_size_counter_list[6],hash_size_counter_list[7],hash_size_counter_list[8],hash_size_counter_list[9],hash_size_counter_list[10],hash_size_counter_list[11],
        'SET',set_size_counter_list[0],set_size_counter_list[1],set_size_counter_list[2],set_size_counter_list[3],set_size_counter_list[4],set_size_counter_list[5],set_size_counter_list[6],set_size_counter_list[7],set_size_counter_list[8],set_size_counter_list[9],set_size_counter_list[10],set_size_counter_list[11],
        'ZSET',zset_size_counter_list[0],zset_size_counter_list[1],zset_size_counter_list[2],zset_size_counter_list[3],zset_size_counter_list[4],zset_size_counter_list[5],zset_size_counter_list[6],zset_size_counter_list[7],zset_size_counter_list[8],zset_size_counter_list[9],zset_size_counter_list[10],zset_size_counter_list[11])



def usage():
   print '''
This Scripts is used for statistic key distribution , Current is Version is Just show key type and key size Distrub!

--INPUT :
   -p,--password=          Author pass
   -P,--port=              Redis Port ,Default is 6379
   -h,--host=              Redis Host ,Default is 127.0.0.1
   -H,--help               show Scritps Usages !

--EXAMPLE:
    python redis_key_distribution.py -h 8.8.8.88 -P 7201

--Sample:
----------------------------------------------------------------------------------------------------------------------------------------------------
统计时间:[Thu Aug 18 20:36:42 2016 ~ Fri Aug 19 00:17:55 2016]
Redis服务器[redis-slave-cart-info-000.jp:5279]
数据类型和数据大小分布情况如下:
----------------------------------------------------------------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------------------------------------------
|KEY TYPE | KEY COUNT  | KEY No TTL| KEY 64(byte) | KEY 128 | Key 512 | Key 1024 | Key 2048 | Key 3072 | Key 4096 | Key 5120 | Key 6044 | Key large |
| String  | 36989458   | 23750890  | 33587210     | 1355252 |  268180 |  1181146 |  390903  |  110478  |   43892  |   21394  |   10357  |    20646  |
|   LIST  |        15  |      15   |       0      |       0 |       3 |        1 |       1  |       0  |       1  |       1  |       0  |        8  |
|   HASH  |      5454  |    1520   |     954      |     171 |     543 |      885 |    1431  |     673  |     275  |       5  |       2  |      515  |
|    SET  |         0  |       0   |       0      |       0 |       0 |        0 |       0  |       0  |       0  |       0  |       0  |        0  |
|   ZSET  |         1  |       1   |       0      |       0 |       0 |        0 |       0  |       0  |       0  |       0  |       0  |        1  |
----------------------------------------------------------------------------------------------------------------------------------------------------


   Above shows key type for goods-sort, as you can see most key type is : ZSET; for ZSET: there are 26489 keys size between 512 ~ 1024 byte !

   '''
   sys.exit()


def parse_args(sys_argvs):
   passwd    = ''
   host      = '127.0.0.1'
   port      = 6379
   try:
      opts,args = getopt.getopt(sys_argvs,"HP:h:",["help","password=","port=","host="])
      for op,value in opts:
         if op in ("-p","--password"):
             password = value
         elif op in ("-h","--host"):
             host = value
         elif op in ("-P","--port"):
             port = int(value)
         elif op in ("-H","--help"):
             usage()
   except Exception,e:
        print "Parse Args Error ,%s" % (e)
   return {'passwd':passwd,'host':host,'port':port,'outputdir':outputdir}



''' Main Function '''
def main():
   if len(sys.argv) < 2:
      usage()
   input_args = parse_args(sys.argv[1:])

   host = input_args['host']
   port = input_args['port']
   start_time = time.ctime()

   rdbConn = redis.Redis(host=host,port=port)
   init_keys = get_key(rdbConn,0)

   #keysFile = open(host.replace('.','_')+'_'+str(port)+'.redis','w')
   keys_without_expire_time_handle = open(host.replace('.','_')+'_'+str(port)+'_without_expiretime.redis','w')

   while True :
       for key in init_keys[1] :
            key_info = get_key_info(rdbConn,key)
            if key_info:
               redis_key_static(key_info)
               #keysFile.write('''"%s"\n''' % (key+','+key_info[0]+','+str(key_info[1]['serializedlength'])+','+str(key_info[2])))
               if key_info[2] < 0:
                  keys_without_expire_time_handle.write(key_info[0]+','+key+'\n')
       init_keys = get_key(rdbConn,init_keys[0])
       if init_keys[0] == 0:
          break

   end_time = time.ctime()
   #keysFile.close()
   keys_without_expire_time_handle.close()
   show_static_info(host,port,start_time,end_time)

if __name__ == "__main__":
   print ''' Please Wait For Seconds .... '''
   main()
