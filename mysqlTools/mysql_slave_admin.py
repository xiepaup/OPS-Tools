#!/usr/bin/python
# -*- coding: utf-8 -*-
import MySQLdb
import MySQLdb.cursors
import getopt
import sys
reload(sys)
sys.setdefaultencoding('utf8')

'''
This Scripts is for show slave status && start slave && stop slave
Created by xiean @2016-04-20 Email: xiepaup@163.com
Modified by xiean @2016-04-25
Log1: Add slave Debug Info to see truely if sync is ok !
'''
_HOST_ = ""
_PORT_ = ""
_USER_ = ""
_PWD_  = ""

def stop_slave(conn):
    sql_stop_slave = '''stop slave'''
    cur = conn.cursor()
    cur.execute(sql_stop_slave)
    cur.close()
    
def start_slave(conn):
    sql_start = '''start slave'''
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
def get_master_binlog_info(master_host,master_port):
    try:
       master_conn = get_my_conn(master_host,master_port,_USER_,_PWD_)
       master_cur = master_conn.cursor()
       master_cur.execute("show master status")
       master_info = master_cur.fetchone()
       master_file = get_dict_value(master_info,"File")
       master_post = get_dict_value(master_info,"Position")
       master_cur.close()
       master_conn.close()
       return {"master_file":master_file,"master_pos":master_post}
    except Exception,e:
       print "Get Master Info Error [%s:%s],%s " % ( str(master_host),str(master_port),str(e))

def get_binlog_postition(conn,debug):
    try:
       sql_master_status = '''show master status'''
       cur = conn.cursor()
       cur.execute(sql_master_status)
       master_info = cur.fetchone()
       return {"slave_file":master_info['File'],"slave_pos":master_info['Position']}
    except Exception,e:
       print "Get Slave binlog position Error ,%s" % (e)

def change_master_to(conn,slave_dict,pos_dict,debug):
    try:
       sql_change_master = '''CHANGE MASTER TO
      MASTER_HOST='%s',
      MASTER_USER='replic',
      MASTER_PASSWORD='replic',
      MASTER_PORT=%s,
      MASTER_LOG_FILE='%s',
      MASTER_LOG_POS=%s,
      MASTER_CONNECT_RETRY=10''' % (pos_dict['host'],int(pos_dict['port']),pos_dict['file'],pos_dict['pos'])
       if debug:
          print '''-------------------------------------
%s:%s : Execute Change Master Command:
MySQL-binLog File     : %s
MySQL-binLog Position : %s
%s
-------------------------------''' % (slave_dict['master_host'],slave_dict['master_port'],slave_dict['master_file'],slave_dict['master_pos'],sql_change_master)
       else:
          cur = conn.cursor()
          cur.execute(sql_change_master)
          cur.close()
    except Exception,e:
       print "Change Master Error ,[input:%s:%s <--> output:%s:%s] : %s" % (pos_dict['host'],int(pos_dict['port']),slave_dict['master_host'],slave_dict['master_port'],e)

#entry 
def master_to_master(host,port,name,user,pwd,debug):
    '''
      current connect to slave,To made master to slave, execute this function
      so conn is slave connection .
    '''
    try:
        conn = get_my_conn(host,port,user,pwd)
        binlog_pos_dict = get_binlog_postition(conn,debug)  # get slave position
        slave_master_dict = get_slave_info(conn,debug)      # get slave of master 
        master_host  = slave_master_dict['master_host']
        master_port  = slave_master_dict['master_port']
        master_conn = get_my_conn(master_host,master_port,_USER_,_PWD_)
        change_master_to(master_conn,{"master_host":master_host,"master_port":master_port,"master_file":binlog_pos_dict['slave_file'],"master_pos":binlog_pos_dict['slave_pos']},
                         {'host':_HOST_,'port':_PORT_,'file':binlog_pos_dict['slave_file'],'pos':binlog_pos_dict['slave_pos']},debug)
        conn.close()
    except Exception,e:
        print "Set M-M Modle Error ,%s" % (e)
  
def get_slave_info(conn,debug):
    try:
       sql_slave_info = '''show slave status'''
       sql_read_only  = """show global variables like 'read_only'"""
       cur = conn.cursor()
       cur.execute(sql_slave_info)
       slave_info = cur.fetchall()
       slave_sql_thread = get_dict_value(slave_info[0],"Slave_SQL_Running")
       slave_io_thread  = get_dict_value(slave_info[0],"Slave_IO_Running")
       slave_behind_sed = get_dict_value(slave_info[0],"Seconds_Behind_Master")
       slave_master_ip  = get_dict_value(slave_info[0],"Master_Host")
       slave_master_port= get_dict_value(slave_info[0],"Master_Port")
       execute_master_file= get_dict_value(slave_info[0],"Relay_Master_Log_File")
       execute_master_pos = get_dict_value(slave_info[0],"Exec_Master_Log_Pos")
       cur.execute(sql_read_only)
       is_read_only = cur.fetchone()['Value']
       cur.close()
       if debug:
          master_binlog_info = get_master_binlog_info(slave_master_ip,slave_master_port)
          return ({"sql_thread":slave_sql_thread,
                "io_thread":slave_io_thread,
                "behind_seconds":slave_behind_sed,
                "master_host":slave_master_ip,
                "master_port":slave_master_port,
                "read_only":is_read_only,
                "exec_mfile":execute_master_file,
                "exec_mpos":execute_master_pos,
                "master_file":master_binlog_info['master_file'],
                "master_pos":master_binlog_info['master_pos']})
       else:
          return ({"sql_thread":slave_sql_thread,
                "io_thread":slave_io_thread,
                "behind_seconds":slave_behind_sed,
                "master_host":slave_master_ip,
                "master_port":slave_master_port,
                "read_only":is_read_only})
    except Exception,e:
      print "Get Slave info Error , %s " % (e)
      pass
 
def check_slave_delay(slave_info):
    if get_dict_value(slave_info,'exec_mfile') == get_dict_value(slave_info,'master_file') and get_dict_value(slave_info,'exec_mpos') == get_dict_value(slave_info,'master_pos'):
       return "~OK~"
    elif get_dict_value(slave_info,'behind_seconds') == 0:
       return "~Wait~"
    else:
       return "~Error~"

def show_slave_info(host,port,name,slave_info,debug):
   if not debug:
      print "%-16s >> %s:%s | %s  %s %s %s" % (name,host,port,get_dict_value(slave_info,'sql_thread'),
                                           get_dict_value(slave_info,'io_thread'),
                                           get_dict_value(slave_info,'behind_seconds'),
                                           get_dict_value(slave_info,'read_only'))
                                          # get_dict_value(slave_info,'master_host'),
                                          # get_dict_value(slave_info,'master_port'))
   else:
      print "%-16s || %s:%s -sync from -> %s:%s |--> %4s %4s %4s %3s --debug:[slave=%s:%s  master=%s:%s]  %8s" % (name,host,port,
                                           get_dict_value(slave_info,'master_host'),get_dict_value(slave_info,'master_port'),
                                           get_dict_value(slave_info,'io_thread'),get_dict_value(slave_info,'sql_thread'),get_dict_value(slave_info,'behind_seconds'),get_dict_value(slave_info,'read_only'),
                                           get_dict_value(slave_info,'exec_mfile'),get_dict_value(slave_info,'exec_mpos'),
                                           get_dict_value(slave_info,'master_file'),get_dict_value(slave_info,'master_pos'),check_slave_delay(slave_info))
                                           


def do_stop_slave(host,port,name,user,pwd):
   conn = get_my_conn(host,port,user,pwd)
   #slave_info1 = get_slave_info(conn,False)
   #show_slave_info(host,port,slave_info1,False)
   stop_slave(conn)
   slave_info2 = get_slave_info(conn,False)
   show_slave_info(host,port,name,slave_info2,False)
   conn.close()

def do_start_slave(host,port,name,user,pwd):
   conn = get_my_conn(host,port,user,pwd)
   start_slave(conn)
   slave_info2 = get_slave_info(conn,False)
   show_slave_info(host,port,name,slave_info2,False)
   conn.close()

def do_show_slave(host,port,name,user,pwd,debug):
   try:
      conn = get_my_conn(host,port,user,pwd)
      slave_info = get_slave_info(conn,debug)
      #slave_info = get_slave_info_debug(host,port,user,pwd,debug)
      if slave_info:
         show_slave_info(host,port,name,slave_info,debug)
      else:
         print "%-10s >> %s:%s | No More Info " %(name,host,port)
      get_variable(conn,'read_only')[0]["Value"]
      conn.close()
   except Exception,e:
       print "Show Slave Info Error :[%s:%s] %s" % (host,port,e)


def do_set_read_only(host,port,user,pwd):
   pass

#####After Main Function ..
def do_work(host,port,name,user,pwd,debug):
    if stop:
       do_stop_slave(host,port,name,user,pwd)
    if start:
       do_start_slave(host,port,name,user,pwd)
    if status:
       do_show_slave(host,port,name,user,pwd,debug)
    if MMmodle:
       master_to_master(host,port,name,user,pwd,debug)

def usage():
   print '''
slave_manage Used for slave manage for: 
                    start slave;stop slave; 
                    set read_only=1;  set read_only=0;
                    show slave status; show slave and Master binlog positioin

Args:  Input0:      -f,--file        file name
Args:  Input1:      -H,--help        show usage
ARgs:  Input2:      -u,--user        username   
Args:  Input3:      -p,--password    password
Args:  Input4:      -h,--host        host or ip
Args:  Input5;      -P,--port        port
Args:  Input6:      -r               (running)   start slave;
Args:  Input7:      -s               (status)    show slave status;
Args:  Input8:      -t               (terminate) stop  slave;
Args:  Input9:      -d,--debug       show more Info for debug
Args:  Input10:     --read           set global read_only=1; 
Args:  Input11:     --write          set global read_only=0;
Args:  Input12:     --double-master  connect to slave ,and then make master to slave too

attention:
  file format:
  10.10.10.10:5331:dbname1
  10.10.10.11:5331:dbname2

require:
  Python Version 2.7.1 +
  Python MySQLdb Modle .

example:
  python slave_manage.py -uxiean -pxxxx -h10.10.10.10 -P5331 -s           [show slave status]
  python slave_manage.py -uxiean -pxxxx -h10.10.10.10 -P5331 -s --debug   [show slave status for external infomation]
  python slave_manage.py -uxiean -pxxxx -h10.10.10.10 -P5331 -r           [start slave]
  python slave_manage.py -uxiean -pxxxx -h10.10.10.10 -P5331 -t           [stop  slave]
'''
   sys.exit()

if __name__ == "__main__":
   if len(sys.argv) < 1:
      sys.exit()
   try:
      opts,args = getopt.getopt(sys.argv[1:],"Hf:u:p:h:Prstd",["help","file=","user=","host=","password=","port=","debug","write","read","double-master"])
      file = ""
      user = "root"
      pwd  = "jproot"
      host = "127.0.0.1"
      name = "----"
      role = "slave"
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
         elif op in ("--double-master"):
              MMmodle = True
      if file != "":
         slave_list = open(file,'r')
         for host_port in slave_list:
            _HOST_ = host_port.strip().split(":")[0]
            _PORT_ = int(host_port.strip().split(":")[1])
            name = host_port.strip().split(":")[2].decode('UTF-8')
            do_work(_HOST_,_PORT_,name,_USER_,_PWD_,debug)
      else:
         do_work(_HOST_,_PORT_,name,_USER_,PWD_,debug)
   except Exception,e:
      print "Parse Args Error [%s:%s],%s" % (_HOST_,_PORT_,e)    
      pass
