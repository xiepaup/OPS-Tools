package goopsrc


/*
* DATE : 2018/5/21 10:17
* VERSION :  2.1.1
* EMAIL : xiepaup@163.com
*/


import (
	"database/sql"
	"fmt"
	"strconv"
	_ "github.com/go-sql-driver/mysql"
)

type MySQLContext struct {
	Host string
	Port string
	User string
	Password string
	Database string
	Addr string
	Conn *sql.DB
}

func NewMySQLContext(host,port,user,password string) (*MySQLContext) {
	myContext := &MySQLContext{
		Addr:fmt.Sprintf("%s:%s",host,port),
		Host:host,
		User:user,
		Port:port,
		Password:password,
	}
	myContext.InitConnection()
	return myContext
}



func (this *MySQLContext) GetConn()(*sql.DB)  {

	if this.Conn == nil{
		//fmt.Println("lost mysql connection ,recreate it !")
		this.InitConnection()
	}
	return  this.Conn
}

func (this *MySQLContext) InitConnection() {
	this.Addr = fmt.Sprintf("%s:%s",this.Host,this.Port)
	if this.Addr != "" {

		//"root:pswd@tcp(127.0.0.1:3306)/test?charset=utf8"
		tcpaddr := fmt.Sprintf("%s:%s@tcp(%s)/%s", this.User, this.Password, this.Addr,this.Database)
		db, err := sql.Open("mysql", tcpaddr)
		if err != nil {
			// TODO add connect to mysql error !,maybe this will report !
			fmt.Println("connect to mysql Error !,TODO ----,%s", err)
		}
		db.Ping()
		this.Conn = db
	}
}

func (this *MySQLContext) Query(format string,args ...interface{})  ([]map[string]string, error) {
	var reslut []map[string]string
	rows, err := this.GetConn().Query(fmt.Sprintf(format,args...))
	if err != nil {
		// TODO -- this output no need ?
		//fmt.Println("query data error : %s", err)
		return reslut, err
	}

	cols, _ := rows.Columns()
	vals := make([][]byte, len(cols))

	scans := make([]interface{}, len(cols))

	for k, _ := range vals {
		scans[k] = &vals[k]
	}
	for rows.Next() {
		record := make(map[string]string)
		err = rows.Scan(scans...)
		//row := make(map[string]string)
		for k, v := range vals {
			key := cols[k]
			record[key] = string(v)
			//fmt.Println("k:",k,"key:",key,"val:",string(v))
		}
		//fmt.Println(record)
		reslut = append(reslut, record)
	}

	return reslut, nil
}


func (this *MySQLContext) IsAlive() (bool,error) {
	_,err := this.Query("SELECT NOW()")
	if err != nil {
		return false,err
	}
	return true,nil
}

func (this *MySQLContext) SetHost (h string){
	if len(h) != 0 {
		this.Host = h
	}else {
		panic(fmt.Sprintf("none host inputed !"))
	}
}

func (this *MySQLContext) SetPort(p string) {
	if len(p) != 0{
		_ ,err := strconv.Atoi(p)
		if err != nil{
			panic(fmt.Sprintf("input port[%s] none numberic ",p))
		}
		this.Port = p
	}
}

func (this *MySQLContext) SetUser(u string) {
	if len(u) != 0{
		this.User = u
	}else {
		panic(fmt.Sprintf("none username inputed !"))
	}
}


func (this *MySQLContext) ToString() string {
	return fmt.Sprintf("%v",this)
}


func (this *MySQLContext)Close() {
	this.Conn.Close()
}
