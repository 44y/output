package rc_config

import (
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
)

var g_db_config struct {
	db_user string
	db_psw  string
	db_host string
	db_name string
	db_port int
}

func Connect_db() *sql.DB {

	fmt.Println("in Connect_db")

	var open_db string
	open_db = fmt.Sprintf("%s:%s@(%s:%d)/%s", g_db_config.db_user, g_db_config.db_psw, g_db_config.db_host, g_db_config.db_port, g_db_config.db_name)
	//fmt.Println("open_db is ", open_db)
	db, err := sql.Open("mysql", open_db)
	if err != nil {
		fmt.Println("err")
		return nil
	}

	return db
}

func Select_rows(db *sql.DB, sql string) *sql.Rows {
	if len(sql) != 0 {
		rows, q_err := db.Query(sql)
		if q_err != nil {
			LOG_ERR.Println(q_err)
			return nil
		}
		return rows
	}
	return nil
}

func Select_counts(db *sql.DB, sql string) int {
	if len(sql) != 0 {
		rows, q_err := db.Query(sql)
		if q_err != nil {
			LOG_ERR.Println(q_err)
			return 0
		}
		count := 0
		for rows.Next() {
			rows.Scan(&count)
		}
		return count
	}
	return 0
}

func Insert_data(db *sql.DB, sql string) bool {
	if len(sql) != 0 {
		ret, err := db.Exec(sql)
		if err != nil {
			LOG_ERR.Println("insert data error,", err)
			return false
		}
		LOG_TRAC.Println("insert data success,", ret)
		return true
	}
	return false
}

/*
func Table_count(db *sql.DB, table_name string) int {
	sql := fmt.Sprintf("select count(*) from %s", table_name)
	rows, q_err := db.Query(sql)
	if q_err != nil {
		LOG_ERR.Println("db.Query failed")
		return 0
	}
	count := 0
	for rows.Next() {
		rows.Scan(&count)
	}
	return count
}


func Select_org_code_by_limit(db *sql.DB, table_name string, limit_low, count int) *sql.Rows {
	return select_item_by_limit(db, table_name, "SECURITY_SOFTWARE_ORGCODE", limit_low, count)
}

func Select_netbar_code_by_limit(db *sql.DB, table_name string, limit_low, count int) *sql.Rows {
	return select_item_by_limit(db, table_name, "NETBAR_WACODE", limit_low, count)
}

func Select_ap_id_by_limit(db *sql.DB, table_name string, limit_low, count int) *sql.Rows {
	return select_item_by_limit(db, table_name, "COLLECTION_EQUIPMENT_ID", limit_low, count)
}

func select_item_by_limit(db *sql.DB, table_name, item string, limit_low, count int) *sql.Rows {
	sql := fmt.Sprintf("select %s from %s limit %d, %d", item, table_name, limit_low, count)
	rows, q_err := db.Query(sql)
	if q_err != nil {
		LOG_ERR.Println("db.Query failed")
		return nil
	}
	return rows
}

func Select_count_by_item(db *sql.DB, table_name, item_name, item_value string, time_start, time_end int) int {
	sql := fmt.Sprintf("select count(*) from %s where %s = \"%s\" and LOG_TIME>%d and LOG_TIME<%d", table_name, item_name, item_value, time_start, time_end)
	//LOG_TRAC.Println(sql)
	rows, q_err := db.Query(sql)
	if q_err != nil {
		LOG_ERR.Println("db.Query failed ", q_err)
		return 0
	}
	count := 0
	for rows.Next() {
		rows.Scan(&count)
	}
	return count
}

func Select_data_by_item_limit(db *sql.DB, table_name, item_name, item_value string, time_start, time_end int, limit_low, count int) *sql.Rows {
	sql := fmt.Sprintf("select * from %s where %s = \"%s\" and LOG_TIME>%d and LOG_TIME<%d limit %d %d", table_name, item_name, item_value, time_start, time_end, limit_low, count)
	rows, q_err := db.Query(sql)
	if q_err != nil {
		LOG_ERR.Println("db.Query failed")
		return nil
	}
	return rows
}
*/
