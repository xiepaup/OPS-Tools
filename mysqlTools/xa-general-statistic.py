#!/usr/bin/python
# -*- coding: utf-8 -*-
from __future__ import division
import re
import os
import sys
import commands

########################################################
####
####Function : parase generay log
####Author   : xiean 
####Date     : 2016-03-25
####Modify   : 2016-03-28  Log:Fix some bugs
####Modify   : 2016-03-30  Log:Add time caculate && color
####Mail     : xiepaup@163.com
########################################################

_TOP_TABLE_ = 15
_SELECT="SELECT"
_DELETE="DELETE"
_INSERT="INSERT"
_UPDATE="UPDATE"
_SHOW  ="SHOW"     #Not Use Yet !
_CREATE="CREATE"   #Not Use Yet !
_DROP="DROP"       #Not Use Yet !
_TOTAL_EXECUTE_TIME=""

KEY_WORDS=[_SELECT,_DELETE,_INSERT,_UPDATE,_SHOW]


sql_old   = ""
sql_begin = ""
tmp_line  = ""

COMMAND_COUNT_DICT = {}
TABLE_SELECT_DICT = {}
TABLE_UPDATE_DICT = {}
TABLE_INSERT_DICT = {}
TABLE_DELETE_DICT = {}


def do_increase_value(dict_name,key):
   if key not in dict_name.keys():
      dict_name[key]  = 1
   else:
      dict_name[key] += 1


def add_table_stat(type,table_name):
    if type == _SELECT:
       do_increase_value(TABLE_SELECT_DICT,table_name)
    if type == _DELETE:
       do_increase_value(TABLE_DELETE_DICT,table_name)
    if type == _UPDATE:
       do_increase_value(TABLE_UPDATE_DICT,table_name)
    if type == _INSERT:
       do_increase_value(TABLE_INSERT_DICT,table_name)



def sort_dict_by_value(dict_name):
    return sorted(dict_name.iteritems(), key = lambda xakey:xakey[1],reverse=True)


def get_type_execute_times(type):
   if type not in COMMAND_COUNT_DICT.keys():
       return 0
   else:
       if type == _SELECT:
          return COMMAND_COUNT_DICT[_SELECT]
       if type == _DELETE:
          return COMMAND_COUNT_DICT[_DELETE]
       if type == _UPDATE:
          return COMMAND_COUNT_DICT[_UPDATE]
       if type == _INSERT:
          return COMMAND_COUNT_DICT[_INSERT]

def get_dict_by_type(type):
    if type not in KEY_WORDS:
       return {}
    if type == _SELECT:
       return TABLE_SELECT_DICT
    if type == _DELETE:
       #print str(TABLE_DELETE_DICT)
       return TABLE_DELETE_DICT
    if type == _UPDATE:
       #print str(TABLE_UPDATE_DICT)
       return TABLE_UPDATE_DICT
    if type == _INSERT:
       #print str(TABLE_INSERT_DICT)
       return TABLE_INSERT_DICT



def print_sorted_content(type,top_num):
    counter = 0
    dict_name = get_dict_by_type(type)
    print u"--------------------------------------------------------------------------"
    print u"| 序列号 |执行占比 | 每秒执行 | 总执行次数|           执行表名           |"
    print u"--------------------------------------------------------------------------"
    for (key,value) in sort_dict_by_value(dict_name):
        counter += 1
        execute_times = get_type_execute_times(type)
        percent = value/execute_times*100
        if percent < 10:
           print "|%5d   |%7.2f%% |%9.2f |%10d |%30s|" % (counter,value/execute_times*100,value/_TOTAL_EXECUTE_TIME,value,key)
        if percent >= 10 and percent < 40:
           print "\033[1;33;40m|%5d   |%7.2f%% |%9.2f |%10d |%30s|\033[0m" % (counter,value/execute_times*100,value/_TOTAL_EXECUTE_TIME,value,key)
        if percent >= 40:
           print "\033[1;31;40m|%5d   |%7.2f%% |%9.2f |%10d |%30s|\033[0m" % (counter,value/execute_times*100,value/_TOTAL_EXECUTE_TIME,value,key)
        if counter >= top_num:
           break
    print u"--------------------------------------------------------------------------"


## main print func ...
def print_statistic_info(type,top_num):
    print "%s total Executed : %d " % (type,get_type_execute_times(type))
    print_sorted_content(type,top_num)


def print_dict_content():
    pass


def deal_sql_command(line):
    ##print "!!!!!!!!!!!!!!!!!----++===>"+line
    ## before do something ...
    line = line.replace('(',' ( ').replace('`',' ').replace('select','SELECT').replace('insert','INSERT').replace('delete','DELETE').replace('update','UPDATE')
    #                6194968654 Query        UPDATE `js_captcha` SET `cap_status`=1 WHERE ( cap_id = 22464157 )
    if "FROM" in line or "JOIN" in line or "UPDATE" in line or "INSERT" in line or "DELETE" in line:
        #print line.split("WHERE")[0]
        tmp_key = ""
        next_is_table = 0
        #print line.strip()
        #6119924097 Query	SELECT `bi_is_business_bail` FROM `js_business_info` 
        #4547036932 Query	SELECT DISTINCT(apd_si_id) as apd_si_id FROM `js_activity_product_detail` LEFT JOIN js_sgoods_info on js_sgoods_info.si_id = js_activity_product_detail.apd_si_id 
        ##for words in line.split("WHERE")[0].strip().split():
        for words in line.strip().split():
            #print words
            if words in KEY_WORDS:
                 begin=1
                 tmp_key = words
                 #print "-----------Add Key Words !" + tmp_key
                 ## Add Some Statistic info !
                 do_increase_value(COMMAND_COUNT_DICT,tmp_key)
            if next_is_table == 1:
                 next_is_table = 0
                 #print tmp_key.split('(')[-1]+"\t"+words
                 ## Add Some Statistic info !
                 if words == "(":
                    continue
                 add_table_stat(tmp_key,words)
            if words == "FROM":
                 next_is_table = 1
            if words == "JOIN":
                 next_is_table = 1
            if words == _UPDATE:
                 next_is_table = 1
            if words.upper() == "INTO" :
                 next_is_table = 1





def print_stat_info(begin_time,end_time):
    
    #print "===" + str(COMMAND_COUNT_DICT)
    print u"     -----------------------------------------------------------------"
    print u"     -                                                               -"
    print u"     - This Time Total Monitor \033[1;31;40m%10s\033[0m seconds                    -" %(_TOTAL_EXECUTE_TIME)
    print u"     - As Follow is Top \033[1;32;40m%3d\033[0m Execute Table Statistic Info             -" % (_TOP_TABLE_)
    print u"     - General Log Execute Between %8s and %8s             -" % (begin_time,end_time)
    print u"     -                                                               -"
    print u"     -----------------------------------------------------------------"
    print_statistic_info(_SELECT,_TOP_TABLE_)
    print_statistic_info(_UPDATE,_TOP_TABLE_)
    print_statistic_info(_DELETE,_TOP_TABLE_)
    print_statistic_info(_INSERT,_TOP_TABLE_)
    #print_sorted_content(TABLE_SELECT_DICT,15)

def get_begin_time(general_file):
    return commands.getoutput("head -n4 %s |tail -n1|awk '{print $2}'" % (general_file))

def caculate_time(begin_time,end_time):
   time_list_begin = begin_time.split(':')
   time_list_end   = end_time.split(':')
   if len(time_list_end) != 3 or len(time_list_begin) != 3 or begin_time == end_time:
      print "Input End Time Error ,Please reinput !\n"
      print "Usage: "
      usage()
      sys.exit()
   else:
      end_long_time   = int(time_list_end[0])*3600 + int(time_list_end[1])*60 + int(time_list_end[2])
      begin_long_time = int(time_list_begin[0])*3600 + int(time_list_begin[1])*60 + int(time_list_begin[2])

      return int( end_long_time - begin_long_time )

def usage():
   print "====================================================================="
   print "+            Wellcome to general log parase center                  +"
   print "+       Usage : python "+sys.argv[0] +" general.log 10:10:10 +"
   print "+       general.log ---> 需要解析的general log 日志文件             +"
   print "+       10:10:10    ---> 文件结束时间                               +"
   print "====================================================================="


if __name__ == "__main__":
   if len(sys.argv) != 3:
      usage()
      #caculate_time('10:10:10','13:20:20')
   else:
      try:
         general_file = sys.argv[1]
         end_time = sys.argv[2]
         filehandle = open(general_file,'r')
         next_output = 2
         begin_time = get_begin_time(general_file)
         _TOTAL_EXECUTE_TIME = caculate_time(begin_time,end_time)
         for line in filehandle:
            line = line.strip()
            #print "Current Line : ======="+line
            if re.match('''[0-9]{5,8}''',line):
                sql_begin = line
                #print "------->"+line
                next_output = 2
            else:
                if sql_old != "":
                   tmp_line = sql_old
                   sql_old  = ""
                   next_output = 1
                tmp_line += "  " + line
                #print "===>"+ tmp_line+"----"
                continue
            if tmp_line != "":
                """deal---new line"""
                deal_sql_command(tmp_line)
            sql_old = sql_begin
            if next_output == 1 or tmp_line == "":
               deal_sql_command(line)
            tmp_line = ""
         deal_sql_command(line)
         print_stat_info(begin_time,end_time)
         pass
      except Exception,e:
        print e
        raise
      finally:
        filehandle.close()

###----Get File Begin Time : head -n4 general_8.36_20160330-0955_1006.log |tail -n1|awk '{print $2}'
