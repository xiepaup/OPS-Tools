#!/bin/bash

##############################################################
#####
#####Function 1   : backup to local directory
#####Function 2   : backup to remote directory
#####Function 3   : backup to remote directory(compress)
#####Author       : xiean
#####Email        : xiepup@163.com
#####Date         : 2016-04-15
#####Version      : 1.2.2
##############################################################

PROJECT="xxxx-db-3306-10-10"
BACK_USER="dbbackup"
BACK_PWD="dbbackup"
INNOBACKUP="/usr/bin/innobackupex"
XTRABACKUP="/usr/bin/xtrabackup"
REMOTE_STREAM="/usr/bin/xbstream"
MYSQL_BASE_DIR="/data/mysql"
BACKUP_BASE_DIR="/data/dbbak"
BACKUP_DATA_DIR="/data/dbbak/${PROJECT}_`date "+%F"`"
LOCAL_DEL_BAK="/data/dbbak/${PROJECT}_`date "+%F" -d '1 days ago'`"
REMOTE_DEL_BAK="/data/dbbak/${PROJECT}_`date "+%F" -d '2 days ago'`"
REMOTE_DEL_TAR_BAK="/data/dbbak/${PROJECT}_`date "+%F" -d '5 days ago'`"

back_mysql_local(){
   mkdir -p ${BACKUP_DATA_DIR}/logs 2>/dev/null
   rm -rf ${LOCAL_DEL_BAK}  >>/dev/null
   ${INNOBACKUP} --ibbackup=${XTRABACKUP} --defaults-file=${MYSQL_BASE_DIR}/${port}/my.cnf --user=${BACK_USER} --password=${BACK_PWD}  --slave-info ${BACKUP_DATA_DIR} > ${BACKUP_BASE_DIR}/logs/backup_${PROJECT}.log 2>&1
   if [ $? -eq 0 ]; then 
     _SUCCESS="`grep "innobackupex: completed OK!" ${BACKUP_BASE_DIR}/logs/backup_${PROJECT}.log | wc -l`"
     if [ ${_SUCCESS} -eq 1 ]; then
        echo "`date "+%F %T"` : Backup ${PROJECT} Success !!"
        BACK_REAL_DIR="`head -n26 ${BACKUP_BASE_DIR}/logs/backup_${PROJECT}.log |grep 'innobackupex: Created backup directory'|awk '{print $5}'`"
        cp ${MYSQL_BASE_DIR}/${port}/my.cnf ${BACK_REAL_DIR}/node-back-my.cnf
        touch ${BACK_REAL_DIR}/_SUCCESS
     else
       echo "`date "+%F %T"` : Backup ${PROJECT} Failed -1!!"
       touch ${BACK_REAL_DIR}/_FAILED
       exit -100
     fi
   else
       echo "`date "+%F %T"` : Backup ${PROJECT} Failed -2 !!"
       touch ${BACK_REAL_DIR}/_FAILED
       exit -101
   fi 
}

back_mysql_remote(){
   ssh root@${back_host} "rm -rf ${BACKUP_DATA_DIR} 2>/dev/null;mkdir -p ${BACKUP_DATA_DIR} 2>/dev/null"
   ${INNOBACKUP} --ibbackup=${XTRABACKUP} --defaults-file=${MYSQL_BASE_DIR}/${port}/my.cnf  --user=${BACK_USER} --password=${BACK_PWD}  --slave-info --stream=xbstream /tmp 2> ${BACKUP_BASE_DIR}/logs/backup_${PROJECT}.log | ssh root@${back_host} "${REMOTE_STREAM} -x -C ${BACKUP_DATA_DIR}"
   if [ $? -eq 0 ]; then
      echo "`date "+%F %T"` : Backup db ${PROJECT} Success !" >> ${BACKUP_BASE_DIR}/logs/backup_${PROJECT}.log 2>&1
      scp ${MYSQL_BASE_DIR}/${port}/my.cnf root@${back_host}:/${BACKUP_DATA_DIR}/node-back-my.cnf >/dev/null
      touch_remote "_SUCCESS"
      ssh root@${back_host} "rm -rf ${REMOTE_DEL_BAK}"
   else
     echo "`date "+%F %T"` : Backup db ${PROJECT} Failed !" >> ${BACKUP_BASE_DIR}/logs/backup_${PROJECT}.log 2>&1
     touch_remote "_FAILED"
   fi
}

back_mysql_tar(){
    ${INNOBACKUP} --ibbackup=${XTRABACKUP} --defaults-file=${MYSQL_BASE_DIR}/${port}/my.cnf  --user=${BACK_USER} --password=${BACK_PWD}  --slave-info --stream=tar /tmp 2> ${BACKUP_BASE_DIR}/logs/backup_${PROJECT}.log | ssh root@${back_host} "gzip > ${BACKUP_DATA_DIR}.tar.gz"
    if [ $? -eq 0 ]; then
      echo "`date "+%F %T"` : Backup db ${PROJECT} Success !" >> ${BACKUP_BASE_DIR}/logs/backup_${PROJECT}.log 2>&1
      scp ${MYSQL_BASE_DIR}/${port}/my.cnf root@${back_host}:/data/dbbak/${PROJECT}-back-my-`date "+%F"`.cnf >/dev/null
      if [ $? -eq 0 ]; then
         echo "`date "+%F %T"` : Backup ${PROJECT} to [${back_host}] Success !!"
         ssh root@${back_host} " rm -f ${REMOTE_DEL_TAR_BAK}.tar.gz"
      else
         echo "`date "+%F %T"` : Backup ${PROJECT} to [${back_host}] Success !!"
      fi
   else
     echo "`date "+%F %T"` : Backup db ${PROJECT} Failed !" >> ${BACKUP_BASE_DIR}/logs/backup_${PROJECT}.log 2>&1
     touch_remote "_FAILED"
   fi
}

touch_remote(){
   ssh root@${back_host} "touch ${BACKUP_DATA_DIR}/$1"
}



if [ $# -eq 1 ]; then
   export port=$1
   back_mysql_local
fi
if [ $# -eq 2 ]; then
   export back_host=$1
   export port=$2
   back_mysql_remote

fi
if [ $# -eq 3 ]; then
   export back_host=$1
   export port=$2
   back_mysql_tar
fi
