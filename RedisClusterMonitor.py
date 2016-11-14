import sys
import time
import redis

###############################
####
#### Redis Cluster Nodes monitor !!
####Created : xiean
####Date    : 2016-10-31
####Mail    : xiepaup@163.com
####
###############################


nodes_conn  = {}
nodes_info = {
                 '19-7210':'10.205.142.19:7210',
                 '20-7210':'10.205.142.21:7210',
                 '20-6379':'10.205.142.20:6379',
                 '21-6380':'10.205.142.21:6380',
                 '21-6379':'10.205.142.21:6379',
                 '21-7210':'10.205.142.21:7210',
                 '31-6379':'10.205.142.31:6379',
                 '31-7210':'10.205.142.31:7210',
                 '31-8210':'10.205.142.31:8210',
                 '22-7210':'10.205.142.22:7210',
                 '22-6379':'10.205.142.22:6379'}
def create_connection():
   global nodes_conn,nodes_info
   for (key,val) in nodes_info.items():
      nodes_conn[key] = redis.Redis(host=val.split(':')[0],port=val.split(':')[1])
   print "%s : Created All Connection ! " % (time.ctime())


def show_message(msg):
   global log_handle
   show_msg = "%s : %s" %(time.ctime()[11:19],msg)
   print show_msg
   log_handle.write(show_msg+"\n")


def get_client_list(_rcKey):
    global nodes_conn,nodes_info
    clientFileName = './logs/%s-clientList-%s.log' % (nodes_info[_rcKey].replace(':','_'),time.ctime()[11:19].replace(':',''))
    print clientFileName
    clist = nodes_conn[_rcKey].client_list()
    clientFileHandle = open(clientFileName,'w')
    for cc in clist:
       cc = str(cc).replace("'","").replace('{','').replace('}','')
       clientFileHandle.write(cc+'\n')
    clientFileHandle.close()


def add_color(intKey):
   if intKey   <= 10:
      return intKey
   elif intKey <= 200:
      return "\033[32m%s\033[0m" % intKey
   elif intKey <= 600:
      return "\033[34m%s\033[0m" % intKey
   elif intKey <= 1200:
      return "\033[33m%s\033[0m" % intKey
   else:
      return "\033[31m%s\033[0m" % intKey


def get_client_list(_rcKey):
    global nodes_info,nodes_conn
    clientFileName = './logs/%s-clientList-%s.log' % (nodes_info[_rcKey].replace(':','_'),time.ctime()[11:19].replace(':',''))
    print clientFileName
    clist = nodes_conn[_rcKey].client_list()
    cinfo = nodes_conn[_rcKey].info()
    clientFileHandle = open(clientFileName,'w')
    for cc in clist:
       cc = str(cc).replace("'","").replace('{','').replace('}','')
       clientFileHandle.write(cc+'\n')
    for ci in cinfo:
       ci = "%-50s:%100s" % (ci,cinfo[ci])
       clientFileHandle.write(ci+'\n')
    clientFileHandle.close() 


def monitor_info():
   global nodes_conn
   conn_client = {}
   _nodeWriteLog = {}
   _nodeWriteSleepTime = {}

   for (k,v) in nodes_conn.items():
      _nodeWriteLog[k] = True
      _nodeWriteSleepTime[k] = 5

   while True:
      _total_cc = 0
      _total_bc = 0
      _totla_ops = 0

      for (_rcKey,_rConn) in nodes_conn.items():
         _info = _rConn.info()
         cc = _info['connected_clients']
         bc = _info['blocked_clients']
         ops = _info['instantaneous_ops_per_sec']
         _total_cc = _total_cc + cc
         _total_bc = _total_bc + bc
         _totla_ops = _totla_ops + ops
         conn_client[_rcKey]  = "%4s:%-6s" % (cc,ops)

         if cc > 1000 or bc > 2:
            if _nodeWriteLog[_rcKey]:
               get_client_list(_rcKey)
               _nodeWriteLog[_rcKey] = False
            else:
               if _nodeWriteSleepTime[_rcKey] > 55:
                  _nodeWriteSleepTime[_rcKey] = 0
                  _nodeWriteLog[_rcKey] = True
            _nodeWriteSleepTime[_rcKey] = _nodeWriteSleepTime[_rcKey] + 1


      conn_client['ALL'] = "%5s:%-6s" % (_total_cc,_totla_ops)
      msg = str(conn_client).replace("'","").replace('{','').replace('}','').replace(',','|')
      show_message(msg)
      time.sleep(1)

if __name__ == "__main__":
   thisTime = time.ctime()[4:19].replace(' ','_').replace(':','')
   fileName = './logs/goods_detail-%s.log' %(thisTime)
   log_handle = open(fileName,'w')
   create_connection()
   monitor_info()
   log_handle.close()
