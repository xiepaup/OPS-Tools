#!/usr/bin/perl -w

use strict;
use warnings;

use Class::Struct;
use Getopt::Long qw(:config no_ignore_case);					# GetOption
# register handler system signals
use sigtrap 'handler', \&sig_int, 'normal-signals';

# catch signal
sub sig_int(){
	my ($signals) = @_;
	print STDERR "# Caught SIG$signals.\n";
	exit 1;
}

my %opt;
my $srcfile;
my $host = '127.0.0.1';
my $port = 3306;
my ($user,$pwd);
my ($MYSQL, $MYSQLBINLOG, $ROLLBACK_DML);
my $outfile = '/dev/null';
my (%do_dbs,%do_tbs);

# tbname=>tbcol, tbcol: @n=>colname,type
my %tbcol_pos;

my $SPLITER_COL = ',';
my $SQLTYPE_IST = 'INSERT';
my $SQLTYPE_UPD = 'UPDATE';
my $SQLTYPE_DEL = 'DELETE';
my $SQLAREA_WHERE = 'WHERE';
my $SQLAREA_SET = 'SET';

my $PRE_FUNCT = '========================== ';

# =========================================================
# 基于row模式的binlog，生成DML(insert/update/delete)的rollback语句
# 通过mysqlbinlog -v 解析binlog生成可读的sql文件
# 提取需要处理的有效sql
# 	"### "开头的行.如果输入的start-position位于某个event group中间，则会导致"无法识别event"错误
#
# 将INSERT/UPDATE/DELETE 的sql反转,并且1个完整sql只能占1行
# 	INSERT: INSERT INTO => DELETE FROM, SET => WHERE
# 	UPDATE: WHERE => SET, SET => WHERE
# 	DELETE: DELETE FROM => INSERT INTO, WHERE => SET
# 用列名替换位置@{1,2,3}
# 	通过desc table获得列顺序及对应的列名
# 	特殊列类型value做特别处理
# 逆序
# 
# 注意:
# 	表结构与现在的表结构必须相同[谨记]
# 	由于row模式是幂等的，并且恢复是一次性，所以只提取sql，不提取BEGIN/COMMIT
# 	只能对INSERT/UPDATE/DELETE进行处理
# ========================================================
sub main{

	# get input option
	&get_options();

	# 
	&init_tbcol();

	#
	&do_binlog_rollback();
}

&main();


# ----------------------------------------------------------------------------------------
# Func : get options and set option flag 
# ----------------------------------------------------------------------------------------
sub get_options{
	#Get options info
	GetOptions(\%opt,
		'help',					# OUT : print help info   
		'f|srcfile=s',			# IN  : binlog file
		'o|outfile=s',			# out : output sql file
		'h|host=s',				# IN  :  host
		'u|user=s',             # IN  :  user
		'p|password=s',         # IN  :  password
		'P|port=i',				# IN  :  port
		'start-datetime=s',		# IN  :  start datetime
		'stop-datetime=s',		# IN  :  stop datetime
		'start-position=i',		# IN  :  start position
		'stop-position=i',		# IN  :  stop position
		'd|database=s',			# IN  :  database, split comma
		'T|table=s',			# IN  :  table, split comma
		'i|ignore',				# IN  :  ignore binlog check ddl and so on
		'debug',				# IN  :  print debug information
	  ) or print_usage();

	if (!scalar(%opt)) {
		&print_usage();
	}

	# Handle for options
	if ($opt{'f'}){
		$srcfile = $opt{'f'};
	}else{
		&merror("please input binlog file");
	}

	$opt{'h'} and $host = $opt{'h'};
	$opt{'u'} and $user = $opt{'u'};
	$opt{'p'} and $pwd = $opt{'p'};
	$opt{'P'} and $port = $opt{'P'};
	if ($opt{'o'}) {
		$outfile = $opt{'o'};
		# 清空 outfile
		`echo '' > $outfile`;
	}

	# 
	$MYSQL = qq{mysql -h$host -u$user -p'$pwd' -P$port};
	&mdebug("get_options::MYSQL\n\t$MYSQL");

	# 提取binlog,不需要显示列定义信息，用-v，而不用-vv
	$MYSQLBINLOG = qq{mysqlbinlog -v};
	$MYSQLBINLOG .= " --start-position=".$opt{'start-position'} if $opt{'start-position'};
	$MYSQLBINLOG .= " --stop-position=".$opt{'stop-position'} if $opt{'stop-postion'};
	$MYSQLBINLOG .= " --start-datetime='".$opt{'start-datetime'}."'" if $opt{'start-datetime'};
	$MYSQLBINLOG .= " --stop-datetime='$opt{'stop-datetime'}'" if $opt{'stop-datetime'};
	$MYSQLBINLOG .= " $srcfile";
	&mdebug("get_options::MYSQLBINLOG\n\t$MYSQLBINLOG");

	# 检查binlog中是否含有 ddl sql: CREATE|ALTER|DROP|RENAME
	&check_binlog() unless ($opt{'i'});

	# 不使用mysqlbinlog过滤，USE dbname;方式可能会漏掉某些sql，所以不在mysqlbinlog过滤
	# 指定数据库
	if ($opt{'d'}){
		my @dbs = split(/,/,$opt{'d'});
		foreach my $db (@dbs){
			$do_dbs{$db}=1;
		}
	}

	# 指定表
	if ($opt{'T'}){
		my @tbs = split(/,/,$opt{'T'});
		foreach my $tb (@tbs){
			$do_tbs{$tb}=1;
		}
	}

	# 提取有效DML SQL
	$ROLLBACK_DML = $MYSQLBINLOG." | grep '^### '";
	# 去掉注释: '### ' -> ''
	# 删除首尾空格
	$ROLLBACK_DML .= " | sed 's/###\\s*//g;s/\\s*\$//g'";
	&mdebug("rollback dml\n\t$ROLLBACK_DML");
	
	# 检查内容是否为空
	my $cmd = "$ROLLBACK_DML | wc -l";
	&mdebug("check contain dml sql\n\t$cmd");
	my $size = `$cmd`;
	chomp($size);
	unless ($size >0){
		&merror("binlog DML is empty:$ROLLBACK_DML");
	};

}	


# ----------------------------------------------------------------------------------------
# Func :  check binlog contain DDL
# ----------------------------------------------------------------------------------------
sub check_binlog{
	&mdebug("$PRE_FUNCT check_binlog");
	my $cmd = "$MYSQLBINLOG ";
	$cmd .= " | grep -E -i '^(CREATE|ALTER|DROP|RENAME)' ";
	&mdebug("check binlog has DDL cmd\n\t$cmd");
	my $ddlcnt = `$cmd`;
	chomp($ddlcnt);

	my $ddlnum = `$cmd | wc -l`;
	chomp($ddlnum);
	my $res = 0;
	if ($ddlnum>0){
		# 在ddl sql前面加上前缀<DDL>
		$ddlcnt = `echo '$ddlcnt' | sed 's/^/<DDL>/g'`;
		&merror("binlog contain $ddlnum DDL:$MYSQLBINLOG. ddl sql:\n$ddlcnt");
	}

	return $res;
}


# ----------------------------------------------------------------------------------------
# Func : init all table column order
#		if input --database --table params, only get set table column order
# ----------------------------------------------------------------------------------------
sub init_tbcol{
	&mdebug("$PRE_FUNCT init_tbcol");
	# 提取DML语句
	my $cmd .= "$ROLLBACK_DML | grep -E '^(INSERT|UPDATE|DELETE)'";
	# 提取表名，并去重
	#$cmd .= " | awk '{if (\$1 ~ \"^UPDATE\") {print \$2}else {print \$3}}' | uniq ";
	$cmd .= " | awk '{if (\$1 ~ \"^UPDATE\") {print \$2}else {print \$3}}' | sort | uniq ";
	&mdebug("get table name cmd\n\t$cmd");
	open ALLTABLE, "$cmd | " or die "can't open file:$cmd\n";

	while (my $tbname = <ALLTABLE>){
		chomp($tbname);
		#if (exists $tbcol_pos{$tbname}){
		#	next;
		#}
		&init_one_tbcol($tbname) unless (&ignore_tb($tbname));
		
	}
	close ALLTABLE or die "can't close file:$cmd\n";

	# init tb col
	foreach my $tb (keys %tbcol_pos){
		&mdebug("tbname->$tb");
		my %colpos = %{$tbcol_pos{$tb}};
		foreach my $pos (keys %colpos){
			my $col = $colpos{$pos};
			my ($cname,$ctype) = split(/$SPLITER_COL/, $col);
			&mdebug("\tpos->$pos,cname->$cname,ctype->$ctype");
		}
	}
};


# ----------------------------------------------------------------------------------------
# Func : init one table column order
# ----------------------------------------------------------------------------------------
sub init_one_tbcol{
	my $tbname = shift;
	&mdebug("$PRE_FUNCT init_one_tbcol");
	# 获取表结构及列顺序
	my $cmd = $MYSQL." --skip-column-names --silent -e 'desc $tbname'";
	# 提取列名，并拼接
	$cmd .= " | awk -F\'\\t\' \'{print NR\"$SPLITER_COL`\"\$1\"`$SPLITER_COL\"\$2}'";
	&mdebug("get table column infor cmd\n\t$cmd");
	open TBCOL,"$cmd | " or die "can't open desc $tbname;";

	my %colpos;
	while (my $line = <TBCOL>){
		chomp($line);
		my ($pos,$col,$coltype) = split(/$SPLITER_COL/,$line);
		&mdebug("linesss=$line\n\t\tpos=$pos\n\t\tcol=$col\n\t\ttype=$coltype");
		$colpos{$pos} = $col.$SPLITER_COL.$coltype;
	}
	close TBCOL or die "can't colse desc $tbname";

	$tbcol_pos{$tbname} = \%colpos;
}


# ----------------------------------------------------------------------------------------
# Func :  rollback sql:	INSERT/UPDATE/DELETE
# ----------------------------------------------------------------------------------------
sub do_binlog_rollback{
	my $binlogfile = "$ROLLBACK_DML ";
	&mdebug("$PRE_FUNCT do_binlog_rollback");

	# INSERT|UPDATE|DELETE
	my $sqltype;
	# WHERE|SET
	my $sqlarea;
	
	my ($tbname, $sqlstr) = ('', '');
	my ($notignore, $isareabegin) = (0,0);

	# output sql file
	open SQLFILE, ">> $outfile" or die "Can't open sql file:$outfile";

	# binlog file
	open BINLOG, "$binlogfile |" or die "Can't open file: $binlogfile";
	while (my $line = <BINLOG>){
		chomp($line);
		if ($line =~ /^(INSERT|UPDATE|DELETE)/){
			# export sql
			if ($sqlstr ne ''){
				$sqlstr .= ";\n";
				print SQLFILE $sqlstr;
				&mdebug("export sql\n\t".$sqlstr);
				$sqlstr = '';
			}

			if ($line =~ /^INSERT/){
				$sqltype = $SQLTYPE_IST;
				$tbname = `echo '$line' | awk '{print \$3}'`;
				chomp($tbname);
				$sqlstr = qq{DELETE FROM $tbname};
			}elsif ($line =~ /^UPDATE/){
				$sqltype = $SQLTYPE_UPD;
				$tbname = `echo '$line' | awk '{print \$2}'`;
				chomp($tbname);
				$sqlstr = qq{UPDATE $tbname};
			}elsif ($line =~ /^DELETE/){
				$sqltype = $SQLTYPE_DEL;	
				$tbname = `echo '$line' | awk '{print \$3}'`;
				chomp($tbname);
				$sqlstr = qq{INSERT INTO $tbname};
			}

			# check ignore table
			if(&ignore_tb($tbname)){
				$notignore = 0;
				&mdebug("<BINLOG>#IGNORE#:line:".$line);
				$sqlstr = '';
			}else{
				$notignore = 1;
				&mdebug("<BINLOG>#DO#:line:".$line);
			}
		}else {
			if($notignore){
				&merror("can't get tbname") unless (defined($tbname));
				if ($line =~ /^WHERE/){
					$sqlarea = $SQLAREA_WHERE;
					$sqlstr .= qq{ SET};
					$isareabegin = 1;
				}elsif ($line =~ /^SET/){
					$sqlarea = $SQLAREA_SET;
					$sqlstr .= qq{ WHERE};
					$isareabegin = 1;
				}elsif ($line =~ /^\@/){
					$sqlstr .= &deal_col_value($tbname, $sqltype, $sqlarea, $isareabegin, $line);
					$isareabegin = 0;
				}else{
					&mdebug("::unknown sql:".$line);
				}
			}
		}
	}
	# export last sql
	if ($sqlstr ne ''){
		$sqlstr .= ";\n";
		print SQLFILE $sqlstr;
		&mdebug("export sql\n\t".$sqlstr);
	}
	
	close BINLOG or die "Can't close binlog file: $binlogfile";

	close SQLFILE or die "Can't close out sql file: $outfile";

	# 逆序
	# 1!G: 只有第一行不执行G, 将hold space中的内容append回到pattern space
	# h: 将pattern space 拷贝到hold space
	# $!d: 除最后一行都删除
	my $invert = "sed -i '1!G;h;\$!d' $outfile";
	my $res = `$invert`;
	&mdebug("inverter order sqlfile :$invert");
}

# ----------------------------------------------------------------------------------------
# Func :  transfer column pos to name
#	deal column value
#
#  &deal_col_value($tbname, $sqltype, $sqlarea, $isareabegin, $line);
# ----------------------------------------------------------------------------------------
sub deal_col_value($$$$$){
	my ($tbname, $sqltype, $sqlarea, $isareabegin, $line) = @_;
	&mdebug("$PRE_FUNCT deal_col_value");
	&mdebug("input:tbname->$tbname,type->$sqltype,area->$sqlarea,areabegin->$isareabegin,line->$line");
	my @vals = split(/=/, $line);
	my $pos = substr($vals[0],1);
	my $valstartpos = length($pos)+2;
	my $val = substr($line,$valstartpos);
	my %tbcol = %{$tbcol_pos{$tbname}};
	my ($cname,$ctype) = split(/$SPLITER_COL/,$tbcol{$pos});
	&merror("can't get $tbname column $cname type") unless (defined($cname) || defined($ctype));
	&mdebug("column infor:cname->$cname,type->$ctype");

	# join str
	my $joinstr;
	if ($isareabegin){
		$joinstr = ' ';
	}else{
		# WHERE 被替换为 SET, 使用 ,  连接
		if ($sqlarea eq $SQLAREA_WHERE){
			$joinstr = ', ';
		# SET 被替换为 WHERE 使用 AND 连接
		}elsif ($sqlarea eq $SQLAREA_SET){
			$joinstr = ' AND ';
		}else{
			&merror("!!!!!!The scripts error");
		}
	}
	
	# 
	my $newline = $joinstr;

	# NULL value
	if (($val eq 'NULL') && ($sqlarea eq $SQLAREA_SET)){
		$newline .= qq{ $cname IS NULL};
	}else{
		# timestamp: record seconds
		if ($ctype eq 'timestamp'){
			$newline .= qq{$cname=from_unixtime($val)};
		# datetime: @n=yyyy-mm-dd hh::ii::ss
		}elsif ($ctype eq 'datetime'){
			$newline .= qq{$cname='$val'};
		}else{
			$newline .= qq{$cname=$val};
		}
	}
	&mdebug("\told>$line\n\tnew>$newline");
	
	return $newline;
}

# ----------------------------------------------------------------------------------------
# Func :  check is ignore table
# params: IN table full name #  format:`dbname`.`tbname`
# RETURN：
#		0 not ignore
#		1 ignore
# ----------------------------------------------------------------------------------------
sub ignore_tb($){
	my $fullname = shift;
	# 删除`
	$fullname =~ s/`//g;
	my ($dbname,$tbname) = split(/\./,$fullname);
	my $res = 0;
	
	# 指定了数据库
	if ($opt{'d'}){
		# 与指定库相同
		if ($do_dbs{$dbname}){
			# 指定表
			if ($opt{'T'}){
				# 与指定表不同
				unless ($do_tbs{$tbname}){
					$res = 1;
				}
			}
		# 与指定库不同
		}else{
			$res = 1;
		}
	}
	#&mdebug("Table check ignore:$fullname->$res");
	return $res;
}


# ----------------------------------------------------------------------------------------
# Func :  print debug msg
# ----------------------------------------------------------------------------------------
sub mdebug{
	my (@msg) = @_;
	print "<DEBUG>@msg\n" if ($opt{'debug'});
}


# ----------------------------------------------------------------------------------------
# Func :  print error msg and exit
# ----------------------------------------------------------------------------------------
sub merror{
	my (@msg) = @_;
	print "<Error>:@msg\n";
	&print_usage();
	exit(1);
}

# ----------------------------------------------------------------------------------------
# Func :  print usage
# ----------------------------------------------------------------------------------------
sub print_usage{
	print <<EOF;
==========================================================================================
Command line options :
	--help				# OUT : print help info   
	-f, --srcfile			# IN  : binlog file. [required]
	-o, --outfile			# OUT : output sql file. [required]
	-h, --host			# IN  : host. default '127.0.0.1'
	-u, --user			# IN  : user. [required]
	-p, --password			# IN  : password. [required] 
	-P, --port			# IN  : port. default '3306'
	--start-datetime		# IN  : start datetime
	--stop-datetime			# IN  : stop datetime
	--start-position		# IN  : start position
	--stop-position			# IN  : stop position
	-d, --database			# IN  : database, split comma
	-T, --table			# IN  : table, split comma. [required] set -d
	-i, --ignore			# IN  : ignore binlog check contain DDL(CREATE|ALTER|DROP|RENAME)
	--debug				# IN  :  print debug information

Sample :
   shell> perl binlog-rollback.pl -f 'mysql-bin.000001' -o '/tmp/t.sql' -u 'user' -p 'pwd' 
   shell> perl binlog-rollback.pl -f 'mysql-bin.000001' -o '/tmp/t.sql' -u 'user' -p 'pwd' -i
   shell> perl binlog-rollback.pl -f 'mysql-bin.000001' -o '/tmp/t.sql' -u 'user' -p 'pwd' --debug
   shell> perl binlog-rollback.pl -f 'mysql-bin.000001' -o '/tmp/t.sql' -h '192.168.1.2' -u 'user' -p 'pwd' -P 3307
   shell> perl binlog-rollback.pl -f 'mysql-bin.000001' -o '/tmp/t.sql' -u 'user' -p 'pwd' --start-position=107
   shell> perl binlog-rollback.pl -f 'mysql-bin.000001' -o '/tmp/t.sql' -u 'user' -p 'pwd' --start-position=107 --stop-position=10000
   shell> perl binlog-rollback.pl -f 'mysql-bin.000001' -o '/tmp/t.sql' -u 'user' -p 'pwd' -d 'db1,db2'
   shell> perl binlog-rollback.pl -f 'mysql-bin.0000*' -o '/tmp/t.sql' -u 'user' -p 'pwd' -d 'db1,db2' -T 'tb1,tb2'
==========================================================================================
EOF
	exit;   
}


1;
