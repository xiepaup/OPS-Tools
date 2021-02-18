#!/usr/bin/python
# -*- coding: utf-8 -*-
import MySQLdb
import MySQLdb.cursors
import getopt
import sys
reload(sys)
sys.setdefaultencoding('utf8')

'''
This Scripts is for show subordinate status && start subordinate && stop subordinate
Created by xiean @2016-04-20 Email: xiepaup@163.com
Modified by xiean @2016-04-25
Log1: Add subordinate Debug Info to see truely if sync is ok !
'''
_HOST_ = ""
_PORT_ = ""
_USER_ = ""
_PWD_  = ""

def stop_subordinate(conn):
    sql_stop_subordinate = '''stop subordinate'''
    cur = conn.cursor()
    cur.execute(sql_stop_subordinate)
    cur.close()
    
def start_subordinate(conn):
    sql_start = '''start subordinate'''
    cur = conn.cursor()
    cur.execute(sql_start)
    cur.close()

def get_dict_value(dict_name,key_name):
    try:
       if dict_name.has_key(key_name):
          return dict_name[key_name]
       else:
          return "_ERROR_"
    except Exception,e:
       print "Get Key from dict Error [%s,%s],%s" % (dict_name,key_name,e)

def set_read_only(conn,switch):
    sql_read = '''set global read_only=%s''' % (switch)
    cur = conn.cursor()
    cur.execute(sql_read)
    cur.close()

def get_variable(conn,variable):
    sql_var = ''' show variables like "%s"''' % (variable)
    cur = conn.cursor()
    cur.execute(sql_var)
    var_value = cur.fetchall()
    cur.close()
    return var_value

def get_my_conn(host,port,user,pwd):
   conn = ""
   try:
      conn =  MySQLdb.connect(host=host,port=port,user=user,
                          passwd=pwd,charset="utf8",cursorclass = MySQLdb.cursors.DictCursor)
   except Exception,e:
       print "Get MySQL conn Error [%s/%s@%s:%s],e" % (user,pwd,host,port,e)
       traceback.print_exc()
   return conn

##  
def get_main_binlog_info(main_host,main_port):
    try:
       main_conn = get_my_conn(main_host,main_port,_USER_,_PWD_)
       main_cur = main_conn.cursor()
       main_cur.execute("show main status")
       main_info = main_cur.fetchone()
       main_file = get_dict_value(main_info,"File")
       main_post = get_dict_value(main_info,"Position")
       main_cur.close()
       main_conn.close()
       return {"main_file":main_file,"main_pos":main_post}
    except Exception,e:
       print "Get Main Info Error [%s:%s],%s " % ( str(main_host),str(main_port),str(e))

def get_binlog_postition(conn,debug):
    try:
       sql_main_status = '''show main status'''
       cur = conn.cursor()
       cur.execute(sql_main_status)
       main_info = cur.fetchone()
       return {"subordinate_file":main_info['File'],"subordinate_pos":main_info['Position']}
    except Exception,e:
       print "Get Subordinate binlog position Error ,%s" % (e)

def change_main_to(conn,subordinate_dict,pos_dict,debug):
    try:
       sql_change_main = '''CHANGE MASTER TO
      MASTER_HOST='%s',
      MASTER_USER='replic',
      MASTER_PASSWORD='replic',
      MASTER_PORT=%s,
      MASTER_LOG_FILE='%s',
      MASTER_LOG_POS=%s,
      MASTER_CONNECT_RETRY=10''' % (pos_dict['host'],int(pos_dict['port']),pos_dict['file'],pos_dict['pos'])
       if debug:
          print '''-------------------------------------
%s:%s : Execute Change Main Command:
MySQL-binLog File     : %s
MySQL-binLog Position : %s
%s
-------------------------------''' % (subordinate_dict['main_host'],subordinate_dict['main_port'],subordinate_dict['main_file'],subordinate_dict['main_pos'],sql_change_main)
       else:
          cur = conn.cursor()
          cur.execute(sql_change_main)
          cur.close()
    except Exception,e:
       print "Change Main Error ,[input:%s:%s <--> output:%s:%s] : %s" % (pos_dict['host'],int(pos_dict['port']),subordinate_dict['main_host'],subordinate_dict['main_port'],e)

#entry 
def main_to_main(host,port,name,user,pwd,debug):
    '''
      current connect to subordinate,To made main to subordinate, execute this function
      so conn is subordinate connection .
    '''
    try:
        conn = get_my_conn(host,port,user,pwd)
        binlog_pos_dict = get_binlog_postition(conn,debug)  # get subordinate position
        subordinate_main_dict = get_subordinate_info(conn,debug)      # get subordinate of main 
        main_host  = subordinate_main_dict['main_host']
        main_port  = subordinate_main_dict['main_port']
        main_conn = get_my_conn(main_host,main_port,_USER_,_PWD_)
        change_main_to(main_conn,{"main_host":main_host,"main_port":main_port,"main_file":binlog_pos_dict['subordinate_file'],"main_pos":binlog_pos_dict['subordinate_pos']},
                         {'host':_HOST_,'port':_PORT_,'file':binlog_pos_dict['subordinate_file'],'pos':binlog_pos_dict['subordinate_pos']},debug)
        conn.close()
    except Exception,e:
        print "Set M-M Modle Error ,%s" % (e)
  
def get_subordinate_info(conn,debug):
    try:
       sql_subordinate_info = '''show subordinate status'''
       sql_read_only  = """show global variables like 'read_only'"""
       cur = conn.cursor()
       cur.execute(sql_subordinate_info)
       subordinate_info = cur.fetchall()
       subordinate_sql_thread = get_dict_value(subordinate_info[0],"Subordinate_SQL_Running")
       subordinate_io_thread  = get_dict_value(subordinate_info[0],"Subordinate_IO_Running")
       subordinate_behind_sed = get_dict_value(subordinate_info[0],"Seconds_Behind_Main")
       subordinate_main_ip  = get_dict_value(subordinate_info[0],"Main_Host")
       subordinate_main_port= get_dict_value(subordinate_info[0],"Main_Port")
       execute_main_file= get_dict_value(subordinate_info[0],"Relay_Main_Log_File")
       execute_main_pos = get_dict_value(subordinate_info[0],"Exec_Main_Log_Pos")
       cur.execute(sql_read_only)
       is_read_only = cur.fetchone()['Value']
       cur.close()
       if debug:
          main_binlog_info = get_main_binlog_info(subordinate_main_ip,subordinate_main_port)
          return ({"sql_thread":subordinate_sql_thread,
                "io_thread":subordinate_io_thread,
                "behind_seconds":subordinate_behind_sed,
                "main_host":subordinate_main_ip,
                "main_port":subordinate_main_port,
                "read_only":is_read_only,
                "exec_mfile":execute_main_file,
                "exec_mpos":execute_main_pos,
                "main_file":main_binlog_info['main_file'],
                "main_pos":main_binlog_info['main_pos']})
       else:
          return ({"sql_thread":subordinate_sql_thread,
                "io_thread":subordinate_io_thread,
                "behind_seconds":subordinate_behind_sed,
                "main_host":subordinate_main_ip,
                "main_port":subordinate_main_port,
                "read_only":is_read_only})
    except Exception,e:
      print "Get Subordinate info Error , %s " % (e)
      pass
 
def check_subordinate_delay(subordinate_info):
    if get_dict_value(subordinate_info,'exec_mfile') == get_dict_value(subordinate_info,'main_file') and get_dict_value(subordinate_info,'exec_mpos') == get_dict_value(subordinate_info,'main_pos'):
       return "~OK~"
    elif get_dict_value(subordinate_info,'behind_seconds') == 0:
       return "~Wait~"
    else:
       return "~Error~"

def show_subordinate_info(host,port,name,subordinate_info,debug):
   if not debug:
      print "%-16s >> %s:%s | %s  %s %s %s" % (name,host,port,get_dict_value(subordinate_info,'sql_thread'),
                                           get_dict_value(subordinate_info,'io_thread'),
                                           get_dict_value(subordinate_info,'behind_seconds'),
                                           get_dict_value(subordinate_info,'read_only'))
                                          # get_dict_value(subordinate_info,'main_host'),
                                          # get_dict_value(subordinate_info,'main_port'))
   else:
      print "%-16s || %s:%s -sync from -> %s:%s |--> %4s %4s %4s %3s --debug:[subordinate=%s:%s  main=%s:%s]  %8s" % (name,host,port,
                                           get_dict_value(subordinate_info,'main_host'),get_dict_value(subordinate_info,'main_port'),
                                           get_dict_value(subordinate_info,'io_thread'),get_dict_value(subordinate_info,'sql_thread'),get_dict_value(subordinate_info,'behind_seconds'),get_dict_value(subordinate_info,'read_only'),
                                           get_dict_value(subordinate_info,'exec_mfile'),get_dict_value(subordinate_info,'exec_mpos'),
                                           get_dict_value(subordinate_info,'main_file'),get_dict_value(subordinate_info,'main_pos'),check_subordinate_delay(subordinate_info))
                                           


def do_stop_subordinate(host,port,name,user,pwd):
   conn = get_my_conn(host,port,user,pwd)
   #subordinate_info1 = get_subordinate_info(conn,False)
   #show_subordinate_info(host,port,subordinate_info1,False)
   stop_subordinate(conn)
   subordinate_info2 = get_subordinate_info(conn,False)
   show_subordinate_info(host,port,name,subordinate_info2,False)
   conn.close()

def do_start_subordinate(host,port,name,user,pwd):
   conn = get_my_conn(host,port,user,pwd)
   start_subordinate(conn)
   subordinate_info2 = get_subordinate_info(conn,False)
   show_subordinate_info(host,port,name,subordinate_info2,False)
   conn.close()

def do_show_subordinate(host,port,name,user,pwd,debug):
   try:
      conn = get_my_conn(host,port,user,pwd)
      subordinate_info = get_subordinate_info(conn,debug)
      #subordinate_info = get_subordinate_info_debug(host,port,user,pwd,debug)
      if subordinate_info:
         show_subordinate_info(host,port,name,subordinate_info,debug)
      else:
         print "%-10s >> %s:%s | No More Info " %(name,host,port)
      get_variable(conn,'read_only')[0]["Value"]
      conn.close()
   except Exception,e:
       print "Show Subordinate Info Error :[%s:%s] %s" % (host,port,e)


def do_set_read_only(host,port,user,pwd):
   pass

#####After Main Function ..
def do_work(host,port,name,user,pwd,debug):
    if stop:
       do_stop_subordinate(host,port,name,user,pwd)
    if start:
       do_start_subordinate(host,port,name,user,pwd)
    if status:
       do_show_subordinate(host,port,name,user,pwd,debug)
    if MMmodle:
       main_to_main(host,port,name,user,pwd,debug)

def usage():
   print '''
subordinate_manage Used for subordinate manage for: 
                    start subordinate;stop subordinate; 
                    set read_only=1;  set read_only=0;
                    show subordinate status; show subordinate and Main binlog positioin

Args:  Input0:      -f,--file        file name
Args:  Input1:      -H,--help        show usage
ARgs:  Input2:      -u,--user        username   
Args:  Input3:      -p,--password    password
Args:  Input4:      -h,--host        host or ip
Args:  Input5;      -P,--port        port
Args:  Input6:      -r               (running)   start subordinate;
Args:  Input7:      -s               (status)    show subordinate status;
Args:  Input8:      -t               (terminate) stop  subordinate;
Args:  Input9:      -d,--debug       show more Info for debug
Args:  Input10:     --read           set global read_only=1; 
Args:  Input11:     --write          set global read_only=0;
Args:  Input12:     --double-main  connect to subordinate ,and then make main to subordinate too

attention:
  file format:
  10.10.10.10:5331:dbname1
  10.10.10.11:5331:dbname2

require:
  Python Version 2.7.1 +
  Python MySQLdb Modle .

example:
  python subordinate_manage.py -uxiean -pxxxx -h10.10.10.10 -P5331 -s           [show subordinate status]
  python subordinate_manage.py -uxiean -pxxxx -h10.10.10.10 -P5331 -s --debug   [show subordinate status for external infomation]
  python subordinate_manage.py -uxiean -pxxxx -h10.10.10.10 -P5331 -r           [start subordinate]
  python subordinate_manage.py -uxiean -pxxxx -h10.10.10.10 -P5331 -t           [stop  subordinate]
'''
   sys.exit()

if __name__ == "__main__":
   if len(sys.argv) < 1:
      sys.exit()
   try:
      opts,args = getopt.getopt(sys.argv[1:],"Hf:u:p:h:Prstd",["help","file=","user=","host=","password=","port=","debug","write","read","double-main"])
      file = ""
      user = "root"
      pwd  = "jproot"
      host = "127.0.0.1"
      name = "----"
      role = "subordinate"
      port = 3306
      debug = False
      stop = False
      start = False
      status = False
      writeable = False
      MMmodle  = False
      for op,value in opts:
         if op in ("-f","--file"):
              file = value
         elif op in ("-H","--help"):
              usage()
         elif op in ("-h","--host"):
              _HOST_ = value
         elif op in ("-P","--port"):
              _PORT_ = int(value)
         elif op in ("-p","--password"):
              _PWD_ = value
         elif op in ("-u","--user"):
              _USER_ = value
         elif op == "-t":
              stop = True
         elif op == "-s":
              status = True
         elif op == "-r":
              start = True
         elif op in ("-d","--debug"):
              debug = True
         elif op in ("write"):
              writeable = True
         elif op in ("read"):
              writeable = False
         elif op in ("--double-main"):
              MMmodle = True
      if file != "":
         subordinate_list = open(file,'r')
         for host_port in subordinate_list:
            _HOST_ = host_port.strip().split(":")[0]
            _PORT_ = int(host_port.strip().split(":")[1])
            name = host_port.strip().split(":")[2].decode('UTF-8')
            do_work(_HOST_,_PORT_,name,_USER_,_PWD_,debug)
      else:
         do_work(_HOST_,_PORT_,name,_USER_,PWD_,debug)
   except Exception,e:
      print "Parse Args Error [%s:%s],%s" % (_HOST_,_PORT_,e)    
      pass
