import redis
import time
import sys
'''
This Scripts is for Delete keys which miss used by dev 
                               
Created by  xiean@2016-04-20 Email xiepaup@163.com
'''



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
         #key = line.split(',')[1]
         key = line
         key_counter += 1
         key_list.append(key)
      if (key_counter % delCounter) == 0 :
         del_keys(rdbConn,key_list)
         key_list = []
         print "%s : Deleted %12s Keys" % (time.ctime(),key_counter)
         time.sleep(0.1)

def usage():
    print '''
Function: This Scripts is used to delete keys which miss uesed by dev !
Args    :
          -H,--help          show usage
          -F,--file          input keys will be deleted !
          -h,--host          redis ip or domain
          -p,--port          redis port
'''

if __name__ == "__main__":
   if len(sys.argv) < 4:
       print "please input argvs "
       usage()
       exit()
   fileName = ""
   host     = ""
   port     = ""

   try:
      opts,args = getopt.getopt(sys.argv[1:],"Hf:h:p:",["help","file=","host=","port="])
      for op,value in opts:
         if op in ("-f","--file"):
             fileName = value
         elif op in ("-h","--host"):
             host = value
         elif op in ("-P","--port"):
             port = int(value)
         elif op in ("-H","--help"):
             usage()
   except Exception,e:
        print "Parse Args Error ,%s" % (e)

   beginTime = time.ctime()
   rdbConn  = redis.Redis(host=host,port=port)
   main(fileName,rdbConn,300)
  
   endTime = time.ctime()
   print "Delete Key Done ! running through %s - %s " % (beginTime,endTime)
