#!/usr/bin/python
# -*- coding: utf-8 -*-
import MySQLdb
import MySQLdb.cursors
import getopt
import time
import sys
reload(sys)
sys.setdefaultencoding('utf8')
'''
This Scripts is for Show MySQL TPS;QPS;Thread_connected;Threads_running;
                               Max_conn;Max_user_conn;Conn_timeout;Interactive_timeout;wait_timeout;long_query_time;read_only
Created by  xiean@2016-04-20 Email xiepaup@163.com
Modified by xiean@2016-04-23 Email xiepaup@163.com
Modify Log:
         Change QPS;TPS statistic way ( from Questions to caclute Com_xxxx)
Version : 1.0.1
'''

def get_my_conn(host,port,user,pwd):
   try:
       return MySQLdb.connect(host=host,port=port,user=user,
                          passwd=pwd,charset="utf8",
                          cursorclass = MySQLdb.cursors.DictCursor)
   except Exception,e:
       print "Connect Error %s:%s - %s" % (host,port,e)
       return None

def get_mysql_process(conn):
   try:
      sql_proc = '''select user,substring_index(host,':',1) host ,count(*) from information_schema.PROCESSLIST group by user,substring_index(host,':',1)'''
      cur = conn.cursor()
      cur.execute(sql_proc)
      proc_info = cur.fetchall()
      cur.close()
   except Exception,e:
     print "Get Processlist Error , %s" % (e) 

def get_mysql_status(conn):
   try:
      #sql_status='''show global status where variable_name in ('Threads_connected', 'Threads_running', 'Slow_queries', 'Uptime', 'questions', 'com_commit', 'com_rollback')'''
      sql_status='''show global status where variable_name in ('Threads_connected', 'Threads_running', 'Slow_queries', 'Com_select','Com_insert','Com_delete','Com_update','Uptime')'''
      cur = conn.cursor()
      cur.execute(sql_status)
      status_info1 = cur.fetchall()
      time.sleep(1)
      cur.execute(sql_status)
      status_info2 = cur.fetchall()
      cur.close()
      status_dict1 = {}
      status_dict2 = {}
      #({'Value': u'44232815', 'Variable_name': u'Com_commit'}, 
      #  {'Value': u'0', 'Variable_name': u'Com_rollback'}, 
      #  {'Value': u'119337', 'Variable_name': u'Questions'}, 
      #  {'Value': u'0', 'Variable_name': u'Slow_queries'}, 
      for key_vlaue in status_info1:
          status_dict1[key_vlaue['Variable_name']] = int(key_vlaue['Value'])
      for key_vlaue in status_info2:
          status_dict2[key_vlaue['Variable_name']] = int(key_vlaue['Value'])
      return {'Threads_running':status_dict2['Threads_running'],'Threads_connected':status_dict2['Threads_connected'],
              'QPS':(status_dict2['Com_select'] - status_dict1['Com_select']),
              'TPS':(status_dict2['Com_insert'] - status_dict1['Com_insert'] +
                     status_dict2['Com_delete'] - status_dict1['Com_delete'] +
                     status_dict2['Com_update'] - status_dict1['Com_update'])}
   except Exception,e:
      print "Get Status Error , %s" % (e)
      return None

def get_mysql_var(conn):
   try:
      sql_var = '''show global variables where variable_name in ('max_connections','max_user_connections','connect_timeout','interactive_timeout','wait_timeout','long_query_time','read_only')'''
      cur     = conn.cursor()
      cur.execute(sql_var)
      var_info = cur.fetchall()
      var_dict = {}
      for key_value in var_info:
          var_dict[key_value['Variable_name']] = key_value['Value']
      return var_dict
   except Exception,e:
      print "Get Variables Error , %s" % (e)
      return None

def exec_command(host,port,name,user,password,sql):
    try:
       conn = get_my_conn(host,port,user,password)
       if conn :
          cur = conn.cursor()
          cur.execute(sql)
          sql_info = cur.fetchall()
          print "Execute On : [%s==%s:%s] ---: " % (name,host,port)
          for info in sql_info:
              print str(info)
          print "--------------------------------"
    except Exception,e:
       print "Execute SQL Error , [%s:%s:%s:%s]%s" % (host,port,user,password,e)
       
def show_table_index(host,port,user,password):
    sql_indx='''SELECT tb.name,idx.name,group_concat(field.name) 
                FROM information_schema.INNODB_SYS_TABLES tb , 
                     information_schema.INNODB_SYS_INDEXES idx,
                     information_schema.INNODB_SYS_FIELDS field 
                WHERE tb.TABLE_ID = idx.TABLE_ID and idx.INDEX_ID = field.INDEX_ID and tb.name not like 'mysql/%'  
                GROUP BY tb.name,idx.name'''

def show_mysql_status(host,port,user,password):
    conn = get_my_conn(host,port,user,password)
    #print "%s,%s,%s,%s" % (host,port,user,password)
    if conn :
       pages = 50
       i     = 49
       while True:
          status_info = get_mysql_status(conn)
          i += 1
          current_time = time.strftime('%X',time.localtime(time.time()))
          if status_info:
              if  i == pages:
                  i = 0
                  print "---------------------------------------------------"
                  print "----------Stat From %15s:%4s" % (host,port)
                  print "---------------------------------------------------"
                  print " Time    |  Running |  Connected |    QPS  |    TPS"
                  print "---------------------------------------------------"
              else:
                  print "%8s |  %7s | %9s  | %7s | %6s" % (current_time,status_info['Threads_running'],status_info['Threads_connected'],
                                                   status_info['QPS'],status_info['TPS'])
                  #print "%7s | %9s  | %7s | %6s" % (status_info['Threads_running'],status_info['Threads_connected'],
                  #                         status_info['Questions']/status_info['Uptime'],
                  #                         (status_info['Com_commit']+status_info['Com_rollback'])/+status_info['Uptime'])
          try:
             time.sleep(inteval)
          except Exception,e:
             pass

def show_mysql_vars(host,port,name,user,password):
    conn = get_my_conn(host,port,user,password)
    if conn :
        var_info = get_mysql_var(conn)
        if var_info :
           print '''%-10s >> %s:%s ||  %6s  |    %6s |       %6s |              %6s |       %6s |   %6s | %6s ''' % (name,host,port,var_info['max_connections'],var_info['max_user_connections'],
                                                                   var_info['connect_timeout'],var_info['interactive_timeout'],var_info['wait_timeout'],
                                                                   var_info['long_query_time'],var_info['read_only'])

def do_work(host,port,name,user,password):
    if status :
       show_mysql_status(host,port,user,password)
    elif var:
       ###print "-------------------------------------------------------------------------------------------------------------------------------------"
       ###print "                                    Max_conn | User_conn | Conn_timeout | interactive_timeout | wait_timeout | long_query | read_only "
       ###print "-------------------------------------------------------------------------------------------------------------------------------------"
       show_mysql_vars(host,port,name,user,password)
    elif sql:
         exec_command(host,port,name,user,password,sql) 

def usage():
   print "----------------------------------------------------"
   print "Args  Input:                                "
   print "Args  Input: -H,--help   show usage         "
   print "Args  Input: -f,--file   host;port format file"
   print "Args  Input: -u,--user=  usernmae"
   print "Args  Input: -p,--password= password"
   print "Args  Input: -h,--host=     hostname or ip "
   print "Args  Input: -P,--port=     mysql port"
   print "Args  Input: -i,--interval=   monitor interval "
   print "Args  Input: -s,--status      show status info"
   print "Args  Input: -v,--var         show variables info"
   print "Args  Input: -e,--sql         execute sql command"
   print "----------------------------------------------------"

if __name__ == "__main__":
   if len(sys.argv) < 2:
      print usage()
      sys.exit()
   file     =  ""
   user     =  "root"
   password =  "jproot"
   host     =  "127.0.0.1"
   port     =  3306
   inteval  =  2.0
   status   = False
   var      = False
   sql      = ""
   try:
      opts,args = getopt.getopt(sys.argv[1:],"Hf:u:p:h:P:isve:",["help","file=","user=","password=","port=","host=","interval=","status","var","sql="])
      for op,value in opts:
         if op in ("-f","--file"):
             file = value
         elif op in ("-u","--user"):
             user = value
         elif op in ("-p","--password"):
             password = value
         elif op in ("-h","--host"):
             host = value
         elif op in ("-P","--port"):
             port = int(value)
         elif op in ("-i","--interval"):
             inteval = float(value)
         elif op in ("-s","--status"):
             status = True
         elif op in ("-v","--var"):
             var = True
         elif op in ("-e","--sql"):
             sql = value
         elif op in ("-H","--help"):
             print usage()
   except Exception,e:
        print "Parse Args Error ,%s" % (e)

   if var and user and  password:
      print "-------------------------------------------------------------------------------------------------------------------------------------"
      print "                                    Max_conn | User_conn | Conn_timeout | interactive_timeout | wait_timeout | long_query | read_only "
      print "-------------------------------------------------------------------------------------------------------------------------------------"
  
   if file !="":
       for line in open(file,'r'):
           if line.strip() != "":
               host = line.strip().split(':')[0]
               port = int(line.strip().split(':')[1])
               name = line.strip().split(':')[2]
           do_work(host,port,name,user,password)
           ### if var :
           ###    print "-------------------------------------------------------------------------------------------------------------------------------------"
           ###    print "                                    Max_conn | User_conn | Conn_timeout | interactive_timeout | wait_timeout | long_query | read_only "
           ###    print "-------------------------------------------------------------------------------------------------------------------------------------"
           ###    show_mysql_vars(host,port,name,user,password)
           ### elif sql:
           ###    exec_command(host,port,user,password,sql)
           ### elif status :
           ###    show_mysql_status(host,port,user,password)
   else:
      do_work(host,port,'',user,password)
      ### if status :
      ###    show_mysql_status(host,port,user,password)
      ### elif var:
      ###    print "-------------------------------------------------------------------------------------------------------------------------------------"
      ###    print "                                    Max_conn | User_conn | Conn_timeout | interactive_timeout | wait_timeout | long_query | read_only "
      ###    print "-------------------------------------------------------------------------------------------------------------------------------------"
      ###    show_mysql_vars(host,port,'',user,password)
      ### elif sql:
      ###    exec_command(host,port,user,password,sql)
