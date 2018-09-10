package data_proc

import (
	"database/sql"
	"fmt"
	rc "output/src/rc_config"
)

func Read_last_succ_log(db *sql.DB, data_type, code string, time_start, time_end int) (int, int) {
	sql := fmt.Sprintf("select last_index,last_time from output_log where data_type = \"%s\" and code =\"%s\" and last_time > %d and last_time <= %d order by last_time desc limit 1", data_type, code, time_start, time_end)
	rows := rc.Select_rows(db, sql)
	if rows == nil {
		rc.LOG_ERR.Println("Select_rows err")
		return 0, 0
	}
	var last_index, last_time int

	for rows.Next() {
		rows.Scan(&last_index, &last_time)
	}
	return last_index, last_time
}

func Insert_succ_log(db *sql.DB, code, data_type string) {
	sql := fmt.Sprintf("insert into output_log (code, data_type, start_time, start_index, last_time,last_index) values(\"%s\", \"%s\", %d, %d, %d, %d)", code, data_type, Last_trans_info.Start_time, Last_trans_info.Start_index, Last_trans_info.Last_time, Last_trans_info.Last_index)
	rc.LOG_INFO.Println(sql)
	rc.Insert_data(db, sql)
}
