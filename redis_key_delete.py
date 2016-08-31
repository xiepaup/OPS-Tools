import redis
import time
import sys

def del_keys(rdbConn,keys_list):
   rdbPiple = rdbConn.pipeline()
   for key in keys_list:
      rdbPiple.delete(key)
   rdbPiple.execute()


def main(fileName,rdbConn,delCounter):
   keys_file   = open(fileName,'r')
   key_counter = 0
   key_list    = []
   for line in keys_file:
      line = line.strip()
      if line != '':
         key = line.split(',')[1]
         key_counter += 1
         key_list.append(key)
      if (key_counter % delCounter) == 0 :
         del_keys(rdbConn,key_list)
         key_list = []
         print "%s : Deleted %12s Keys" % (time.ctime(),key_counter)
         time.sleep(0.1)

if __name__ == "__main__":
   if len(sys.argv) < 4:
       print "please input argvs "
   beginTime = time.ctime()
   fileName = sys.argv[1]
   host     = sys.argv[2]
   port     = sys.argv[3]
   rdbConn  = redis.Redis(host=host,port=port)
   main(fileName,rdbConn,300)
  
   endTime = time.ctime()
   print "Delete Key Done ! running through %s - %s " % (beginTime,endTime)
[root@MHA-Cobbler aaaaaaaaaa_cart-info-keys_del]# cat redis_key_delete.py.bak 
import redis
import time
import sys


''' Del String '''
def del_string_key(rdbConn,keys_list):
   rdbPiple = rdbConn.pipeline()
   for key in keys_list:
      rdbPiple.delete(key)
   rdbPiple.execute()

''' Del Hash '''
def del_hash_key(rdbConn,hashKey):
   hash_cur = 0
   while hash_cur != 0:
       hash_cur,hdata = rdbConn.hscan(hashKey,cur=hash_cur,count=300)
       rdbPiple = rdbConn.pipeline()
       for item in hdata.items():
          rdbPiple.hdel(hashKey,item[0])
       rdbPiple.execute()

''' Del Set '''
def del_set_key(rdbConn,setKey):
   set_cur = 0
   while set_cur != 0:
      set_cur,sdata = rdbConn.sscan(setKey,cur=set_cur,count=300)
      rdbPiple = rdbConn.pipeline()
      for item in sdata:
        rdbPiple.srem(setKey,item)
      rdbPiple.execute()

''' Del List '''
def del_list_key(rdbConn,listKey):
   while rdbConn.llen(listKey) > 0:
      rdbConn.ltrim(listKey,0,-50)

''' Del ZSET KEY '''
def del_zset_key(rdbConn,zsetKey):
   while rdbConn.zcard(zsetKey) > 0:
     rdbConn.zremrangebyrank(zsetKey,0,50)

''' DELETE KEY Entry ... '''
def del_key_entry(rdbConn,keyType,keyName):
   if keyType == 'string':
      del_string_key(rdbConn,keyName)
   elif keyType == 'hash':
      del_hash_key(rdbConn,keyName)
   elif keyType == 'set':
      del_set_key(rdbConn,keyName)
   elif keyType == 'zset':
      del_zset_key(rdbConn,keyName)
   elif keyType == 'list':
      del_list_key(rdbConn,keyName)
   else:
      print "UnKnow Key ... %s,%s" %s (keyType,keyName)


def main(fileName,rdbConn,delCounter):
   keys_file   = open(fileName,'r')
   key_counter = 0
   key_list    = []
   for line in keys_file:
      line = line.strip()
      if line != '':

         type_key = line.split(',')
         keyType = type_key[0]
         keyName = type_key[1]

         key_counter += 1
         if keyType == 'string':
            key_list.append(keyName)
         if len(key_list) == 1000:
            del_key_entry(rdbConn,keyType,key_list)
            key_list = []
         del_key_entry(rdbConn,keyType,keyName)

      if (key_counter % delCounter) == 0 :
         print "%s : Deleted %12s Keys" % (time.ctime(),key_counter)
         time.sleep(0.1)

if __name__ == "__main__":
   if len(sys.argv) < 4:
       print "please input argvs "
   beginTime = time.ctime()
   fileName = sys.argv[1]
   host     = sys.argv[2]
   port     = sys.argv[3]
   rdbConn  = redis.Redis(host=host,port=port)
   main(fileName,rdbConn,900)
  
   endTime = time.ctime()
   print "Delete Key Done ! running through %s - %s " % (beginTime,endTime)
