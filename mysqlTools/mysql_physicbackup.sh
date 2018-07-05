#!/bin/bash


##############################################################
#####
#####Function 1   : backup to local directory
#####Function 2   : backup to remote directory
#####Author       : xiean
#####Email        : xiepup@163.com
#####Date         : 2016-04-15
#####Version      : 1.0.1
##############################################################

PROJECT="test-3306-db-233"
BACK_USER="root"
BACK_PWD="jproot"
INNOBACKUP="/usr/bin/innobackupex"
XTRABACKUP="/usr/bin/xtrabackup"
REMOTE_STREAM="/usr/bin/xbstream"
REMOTE_DIR="/data/dbbak/${PROJECT}_`date "+%F"`"
REMOTE_DEL_DIR="/data/dbbak/${PROJECT}_`date "+%F" -d '2 weeks ago'`"
LOCAL_BAKDIR="/data/dbbak"

back_mysql_local(){
   mkdir -p ${REMOTE_DIR}/logs 2>/dev/null
   rm -rf /data1/dbbak/${PROJECT}_`date "+%F" -d '2 weeks ago'` >>/dev/null
   ${INNOBACKUP} --ibbackup=${XTRABACKUP} --defaults-file=/data/mysql/${port}/my.cnf --user=${BACK_USER} --password=${BACK_PWD}  --slave-info ${REMOTE_DIR} > ${REMOTE_DIR}/logs/backup_${PROJECT}.log 2>&1
   if [ $? -eq 0 ]; then 
     _SUCCESS="`grep "innobackupex: completed OK!" ${REMOTE_DIR}/logs/backup_${PROJECT}.log | wc -l`"
     if [ ${_SUCCESS} -eq 1 ]; then
        echo "`date "+%F %T"` : Backup ${PROJECT} Success !!"
        BACK_REAL_DIR="`head -n26 ${REMOTE_DIR}/logs/backup_${PROJECT}.log |grep 'innobackupex: Created backup directory'|awk '{print $5}'`"
        cp /data1/mysql/${port}/my.cnf ${BACK_REAL_DIR}/node-back-my.cnf
        touch ${REMOTE_DIR}/_SUCCESS
     else
       echo "`date "+%F %T"` : Backup ${PROJECT} Failed -1!!"
       touch ${REMOTE_DIR}/_FAILED
       exit -100
     fi
   else
       echo "`date "+%F %T"` : Backup ${PROJECT} Failed -2 !!"
       touch ${REMOTE_DIR}/_FAILED
       exit -101
   fi 
}

back_mysql_remote(){
   ssh root@${back_host} "rm -rf ${REMOTE_DIR} 2>/dev/null;mkdir -p ${REMOTE_DIR};rm -rf ${REMOTE_DEL_DIR} 2>/dev/null 2>/dev/null"
   ${INNOBACKUP} --ibbackup=${XTRABACKUP} --defaults-file=/data1/mysql/${port}/my.cnf  --user=${BACK_USER} --password=${BACK_PWD}  --slave-info --stream=xbstream /tmp 2> ${LOCAL_BAKDIR}/logs/backup_${PROJECT}.log | ssh root@${back_host} "${REMOTE_STREAM} -x -C ${REMOTE_DIR}"
   ##${INNOBACKUP} --ibbackup=${XTRABACKUP} --defaults-file=/data1/mysql/${port}/my.cnf  --user=${BACK_USER} --password=${BACK_PWD}  --slave-info --stream=tar /tmp 2> ${LOCAL_BAKDIR}/logs/backup_${PROJECT}.log | ssh root@${back_host} "gzip > ${REMOTE_DIR}.tar.gz"
   if [ $? -eq 0 ]; then
      echo "`date "+%F %T"` : Backup db ${PROJECT} Success !" >> ${LOCAL_BAKDIR}/logs/backup_${PROJECT}.log 2>&1
      scp /data1/mysql/${port}/my.cnf root@${back_host}:/${REMOTE_DIR}/node-back-my.cnf >/dev/null
      touch_remote "_SUCCESS"
      ssh root@${back_host} "rm -rf ${REMOTE_DEL_DIR}"
      ##echo "`date "+%F %T"` : Restore data1bases on ${back_host} ............"
      ##ssh root@${back_host} "bash /root/xa/restore_mysql.sh ${port} ${REMOTE_DIR}"
      ##if [ $? -eq 0 ]; then
      ##   echo "`date "+%F %T"` : Resotre MySQL on ${back_host} Done !"
      ##else
      ##   echo "`date "+%F %T"` : Resotre MySQL on ${back_host} Failed !"
      ##fi
   else
     echo "`date "+%F %T"` : Backup db ${PROJECT} Failed !" >> ${LOCAL_BAKDIR}/logs/backup_${PROJECT}.log 2>&1
     touch_remote "_FAILED"
   fi
}

back_mysql_tar(){
    ${INNOBACKUP} --ibbackup=${XTRABACKUP} --defaults-file=/data/mysql/${port}/my.cnf  --user=${BACK_USER} --password=${BACK_PWD}  --slave-info --stream=tar /tmp 2> ${LOCAL_BAKDIR}/logs/backup_${PROJECT}.log | ssh root@${back_host} "gzip > ${REMOTE_DIR}.tar.gz"
    if [ $? -eq 0 ]; then
      echo "`date "+%F %T"` : Backup db ${PROJECT} Success !" >> ${LOCAL_BAKDIR}/logs/backup_${PROJECT}.log 2>&1
      scp /data/mysql/${port}/my.cnf root@${back_host}:/data/dbbak/${PROJECT}-back-my-`date "+%F"`.cnf >/dev/null
      if [ $? -eq 0 ]; then
         echo "`date "+%F %T"` : Resotre MySQL on ${back_host} Done !"
         ssh root@${back_host} " rm -f ${REMOTE_DEL_DIR}.tar.gz"
      else
         echo "`date "+%F %T"` : Resotre MySQL on ${back_host} Failed !"
      fi
   else
     echo "`date "+%F %T"` : Backup db ${PROJECT} Failed !" >> ${LOCAL_BAKDIR}/logs/backup_${PROJECT}.log 2>&1
     touch_remote "_FAILED"
   fi
}

touch_remote(){
   ssh root@${back_host} "touch ${REMOTE_DIR}/$1"
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
