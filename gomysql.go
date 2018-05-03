
func GetMySQLConn(addr, user, paswd string) *sql.DB {

	if addr != "" {
		if user == "" || paswd == "" {
			user = DEFALUT_USER_NAME
			paswd = DEFAULT_USER_PSWD
		}
		//"root:pswd@tcp(127.0.0.1:3306)/test?charset=utf8"
		tcpaddr := fmt.Sprintf("%s:%s@tcp(%s)/test", user, paswd, addr)
		db, err := sql.Open("mysql", tcpaddr)
		if err != nil {
			// TODO add connect to mysql error !,maybe this will report !
			fmt.Println("connect to mysql Error !,TODO ----,%s", err)
		}
		db.Ping()
		return db
	}
	return nil
}

func QueryMySQLDataBySQL(conn *sql.DB, sql string) ([]map[string]string, error) {
	var reslut []map[string]string
	rows, err := conn.Query(sql)
	if err != nil {
		fmt.Println("query data error : %s", err)
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
