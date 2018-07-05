#!/bin/bash
###############################################
## Function: rotate slow_log ,interval everyday
## Created by xiean@2016-05-09 xiepaup@163.com
## Version : 1.2.2
## 10 0 * * * /bin/bash /root/xa/rotate_slow_log.sh 3306  >> /data/dbbak/ops_mysql.log 2>&1
###############################################

USER="dbbackup"
PWD="dbbackup"
DATE="`date "+%F"`"
DEL_DATE="`date "+%F" -d '12 days ago'`"
MYSQL="/usr/local/mysql/bin/mysql"

rotate_log(){
   ${MYSQL} -u${USER} -p${PWD} -S /data/mysql/${port}/mysql.sock information_schema -sss <<EOF
set global slow_query_log=0;
select VARIABLE_VALUE into @old_slow_log_file from GLOBAL_VARIABLES where VARIABLE_NAME='SLOW_QUERY_LOG_FILE';
set global slow_query_log_file='/data/mysql/${port}/mysql_slow_${DATE}.log';
set global slow_query_log=1;
select VARIABLE_VALUE into @new_slow_log_file from GLOBAL_VARIABLES where VARIABLE_NAME='SLOW_QUERY_LOG_FILE';

select concat('Change Slow log file From ',@old_slow_log_file,' To --> ',@new_slow_log_file);
EOF
  if [ $? -eq 0 ]; then
     echo "`date "+%F %T"` : Switch Log File Success !"
  else
     echo "`date "+%F %T"` : Switch Log File Falied !"
     exit -100
  fi
}


remove_log(){
   _FILE_DEL_="/data/mysql/${port}/mysql_slow_${DEL_DATE}.log"
   echo "`date "+%F %T"` : Execute command : rm -f ${_FILE_DEL_}"
   rm -f /data/mysql/${port}/mysql_slow_${DEL_DATE}.log >/dev/null 2>&1
   if [ $? -eq 0 ]; then
      echo "`date "+%F %T"` : Remove File[${_FILE_DEL_}] Success !"
   else
      echo "`date "+%F %T"` : Remove File[${_FILE_DEL_}] Failed !"
   fi
}

export port=$1
rotate_log
remove_log
