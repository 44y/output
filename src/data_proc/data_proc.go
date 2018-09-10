package data_proc

import (
	"bytes"
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	rc "output/src/rc_config"
	"sync"
	"time"
)

const (
	WA_SOURCE_FJ_0001_IND = iota
	WA_SOURCE_FJ_0002_IND
	WA_BASIC_FJ_0003_1_IND
	WA_BASIC_FJ_0003_2_IND
	WA_BASIC_FJ_0001_IND //场所基础信息
	WA_BASIC_FJ_0002_IND //厂商基础信息
	MAX_IND
)

const (
	//select_round_max      = 100
	table_name_suffix     = "_log"
	day_seconds       int = 60 * 60 * 24
)

var (
	G_conf Json_config
	//g_db         *sql.DB
	G_orgcode    []string                  //SECURITY_SOFTWARE_ORGCODE
	G_wacode     = make(map[string]string) //[NETBAR_WACODE]SECURITY_SOFTWARE_ORGCODE
	G_ap_id      = make(map[string]string) //[COLLECTION_EQUIPMENT_ID]NETBAR_WACODE
	Working_mtx  sync.Mutex                //互斥锁，同时只能一个work任务进行
	RC_mtx       sync.Mutex                //互斥锁，处理信号时保护共享资源.. ftp，加密参数等
	G_work       = make(chan bool, 1)      //
	Working_flag bool                      // true:working, false:not working
	WAIT_GROUP   sync.WaitGroup            //wait for Data_process_handle()
)

type Conf struct {
	Output_time   string `json:"output_time"`
	Data_dst_code string `json:"data_dst_code"`
	Key           string `json:"key"`
	Iv            string `json:"iv"`
	Ftp           Ftp    `json:"ftp"`
}

type Ftp struct {
	Ftp_account string `json:"ftp_account"`
	Ftp_host    string `json:"ftp_host"`
	Ftp_port    string `json:"ftp_port"`
	Ftp_psd     string `json:"ftp_password"`
	Ftp_path    string `json:"ftp_path"`
}

type Json_config struct {
	Conf   Conf   `json:"conf"`
	Status string `json:"status"`
	Code   int    `json:"code"`
}

func Get_data_type() []string {
	return []string{
		"WA_SOURCE_FJ_0001",
		"WA_SOURCE_FJ_0002",
		"WA_BASIC_FJ_0003_1",
		"WA_BASIC_FJ_0003_2",
		"WA_BASIC_FJ_0001",
		"WA_BASIC_FJ_0002"}
}

func Data_process_handle() {

	Working_mtx.Lock()
	Working_flag = true
	WAIT_GROUP.Add(1)

	rc.LOG_TRAC.Println("i am in data_process_handle")
	db := rc.Connect_db()
	defer func() {
		Working_flag = false
		db.Close()
		Working_mtx.Unlock()
		rc.LOG_TRAC.Println("end of work")
		WAIT_GROUP.Done()
	}()

	for db == nil {
		rc.LOG_ERR.Println("rc.Connect_db fail,wait for 5 seconds")
		time.Sleep(5 * time.Second)
		db = rc.Connect_db()
	}

	conn_status := Db_conn_test(db)
	for conn_status == false {
		rc.LOG_ERR.Println("Db_conn_test fail,wait for 5 seconds")
		time.Sleep(5 * time.Second)
		conn_status = Db_conn_test(db)
	}

	if Init_status_tables(db, &G_orgcode, G_wacode, G_ap_id) == false {
		rc.LOG_INFO.Println("nothing to do today")
		return
	}

	rc.LOG_TRAC.Println(G_orgcode, G_wacode, G_ap_id)
	data_type := Get_data_type()
	rc.LOG_TRAC.Println(data_type)

	org_count := len(G_orgcode)
	//netbar_count := len(G_wacode)
	//dev_count := len(G_ap_id)

	//rc.LOG_TRAC.Println(time_now, time_now-time_dua)

	t := time.Now()
	time_now := int(t.Unix())
	time_dua := day_seconds

	/*上报安全厂商基础信息:WA_BASIC_FJ_0002 */
	for i := 0; i < org_count; i++ {
		if Working_flag {
			RC_mtx.Lock()
			Org_data_handle(db, G_orgcode[i], time_now, time_dua, data_type)
			RC_mtx.Unlock()
		}
	}
	/*上报场所基础信息：WA_BASIC_FJ_ 0001*/
	for wacode, _ := range G_wacode {
		if Working_flag {
			RC_mtx.Lock()
			Netbar_data_handle(db, wacode, time_now, time_dua, data_type)
			RC_mtx.Unlock()
		}
	}

	/*
		上报设备数据：
		设备基础信息：WA_BASIC_FJ_0003*
		终端上下线：WA_SOURCE_FJ_0001
		上网日志：WA_SOURCE_FJ_0002
	*/
	for apid, _ := range G_ap_id {
		/*loop for each data type*/
		if Working_flag {
			RC_mtx.Lock()
			Ap_data_handle(db, apid, time_now, time_dua, data_type)
			RC_mtx.Unlock()
		}
	}

}

/*init global tables : G_orgcode, G_wacode, G_ap_id*/
func Init_status_tables(db *sql.DB, orgcode_arg *([]string), wacode_arg, ap_id_arg map[string]string) bool {

	sql := fmt.Sprintf("select count(*) from %s", rc.G_table_name.Table_org_status)
	org_count := rc.Select_counts(db, sql)
	if org_count == 0 {
		rc.LOG_INFO.Println("no software org")
		return false
	}

	//netbar_count := rc.Table_count(db, rc.G_table_name.Table_netbar_status)
	sql = fmt.Sprintf("select count(*) from %s", rc.G_table_name.Table_netbar_status)
	netbar_count := rc.Select_counts(db, sql)
	if netbar_count == 0 {
		rc.LOG_INFO.Println("no netbar")
		return false
	}

	//dev_count := rc.Table_count(db, rc.G_table_name.Table_dev_status)
	sql = fmt.Sprintf("select count(*) from %s", rc.G_table_name.Table_dev_status)
	dev_count := rc.Select_counts(db, sql)
	if dev_count == 0 {
		rc.LOG_INFO.Println("no device")
		return false
	}

	limit_low := 0
	/*orgcode_arg*/
	*orgcode_arg = append((*orgcode_arg)[0:0]) //empty orgcode table
	for round := org_count/rc.MAX_BCP_RECORDS + 1; round > 0; round-- {
		sql := fmt.Sprintf("select SECURITY_SOFTWARE_ORGCODE from %s limit %d, %d", rc.G_table_name.Table_org_status, limit_low, rc.MAX_BCP_RECORDS)
		//rows := rc.Select_org_code_by_limit(db, rc.G_table_name.Table_org_status, limit_low, rc.MAX_BCP_RECORDS)
		rows := rc.Select_rows(db, sql)
		if rows == nil {
			rc.LOG_TRAC.Println("Select_by_limit org code err")
			break
		}
		org_code := ""
		for rows.Next() {
			rows.Scan(&org_code)
			//fmt.Println(netbar_wacode)
			*orgcode_arg = append(*orgcode_arg, org_code)
		}
		limit_low += rc.MAX_BCP_RECORDS
	}

	limit_low = 0
	/*wacode_arg*/
	for key, _ := range wacode_arg { //empty wacode map
		delete(wacode_arg, key)
	}
	for round := netbar_count/rc.MAX_BCP_RECORDS + 1; round > 0; round-- {
		//rows := rc.Select_netbar_code_by_limit(db, rc.G_table_name.Table_netbar_status, limit_low, rc.MAX_BCP_RECORDS)
		sql := fmt.Sprintf("select NETBAR_WACODE ,CODE_ALLOCATION_ORGANIZATION from %s limit %d, %d", rc.G_table_name.Table_netbar_status, limit_low, rc.MAX_BCP_RECORDS)
		rows := rc.Select_rows(db, sql)
		if rows == nil {
			rc.LOG_TRAC.Println("Select_by_limit wacode err")
			break
		}
		netbar_wacode := ""
		org_code := ""
		for rows.Next() {
			rows.Scan(&netbar_wacode, &org_code)
			//fmt.Println(netbar_wacode)
			/*场所表中的厂商编码要存在于厂商表中，否则不合法*/
			is_found := false
			for _, val := range *orgcode_arg {
				if val == org_code {
					is_found = true
					break
				}
			}
			if is_found {
				wacode_arg[netbar_wacode] = org_code
			}
		}
		limit_low += rc.MAX_BCP_RECORDS
	}

	limit_low = 0
	/*ap_id_arg*/
	for key, _ := range ap_id_arg { //empty ap_id map
		delete(ap_id_arg, key)
	}
	for round := dev_count/rc.MAX_BCP_RECORDS + 1; round > 0; round-- {
		//rows := rc.Select_ap_id_by_limit(db, rc.G_table_name.Table_dev_status, limit_low, rc.MAX_BCP_RECORDS)
		sql := fmt.Sprintf("select COLLECTION_EQUIPMENT_ID, NETBAR_WACODE from %s limit %d, %d", rc.G_table_name.Table_dev_status, limit_low, rc.MAX_BCP_RECORDS)
		rows := rc.Select_rows(db, sql)
		if rows == nil {
			rc.LOG_TRAC.Println("Select_by_limit ap id err")
			break
		}
		ap_id := ""
		wacode := ""
		for rows.Next() {
			rows.Scan(&ap_id, &wacode)
			//fmt.Println(netbar_wacode)
			/*设备表中的场所编码要存在于场所表中，否则不合法*/
			is_found := false
			for key, _ := range wacode_arg {
				if key == wacode {
					is_found = true
					break
				}
			}
			if is_found {
				ap_id_arg[ap_id] = wacode
			}
		}
		limit_low += rc.MAX_BCP_RECORDS
	}

	return true
}

func Org_data_handle(db *sql.DB, orgcode string, time_stamp int, time_dua int, data_type []string) {
	data_table_name := fmt.Sprintf("%s%s", data_type[WA_BASIC_FJ_0002_IND], table_name_suffix)

	/*读取outputlog中最后传输成功的time和index*/
	last_index, last_time := Read_last_succ_log(db, data_type[WA_BASIC_FJ_0002_IND], orgcode, time_stamp-time_dua, time_stamp)

	time_start := max_num(last_time, time_stamp-time_dua)

	sql := fmt.Sprintf("select count(*) from %s where SECURITY_SOFTWARE_ORGCODE = \"%s\" and LOG_TIME>%d and LOG_TIME<=%d and id>%d", data_table_name, orgcode, time_start, time_stamp, last_index)
	data_count := rc.Select_counts(db, sql)
	rc.LOG_TRAC.Println("厂商 ", orgcode, " 数据类型：", data_type[WA_BASIC_FJ_0002_IND], " 数 ", data_count)
	if data_count == 0 {
		//rc.LOG_WARNING.Println("device ", ap_id, " ", data_type[i], " no data")
		return
	}

	sql = fmt.Sprintf("select DATA_SRC_CODE from %s where SECURITY_SOFTWARE_ORGCODE = \"%s\" and LOG_TIME>%d and LOG_TIME <=%d and id > %d limit 1", data_table_name, orgcode, time_start, time_stamp, last_index)
	rc.LOG_TRAC.Println("sql = ", sql)
	rows := rc.Select_rows(db, sql)
	//rc.LOG_ERR.Println("ret rows = ", rows)
	if rows == nil {
		rc.LOG_ERR.Println("Select_rows err")
		return
	}
	data_src_code := ""
	for rows.Next() {
		rows.Scan(&data_src_code)
	}

	//limit_low := 0
	items_format := form_string_for_sql(WA_BASIC_FJ_0002_IND)
	//for round := data_count/rc.MAX_BCP_RECORDS + 1; round > 0; round-- {
	sql = fmt.Sprintf("select %s from %s where SECURITY_SOFTWARE_ORGCODE = \"%s\" and LOG_TIME>%d and LOG_TIME<=%d and id > %d limit 1", items_format, data_table_name, orgcode, time_start, time_stamp, last_index)
	rc.LOG_TRAC.Println("sql = ", sql)
	rows = rc.Select_rows(db, sql)
	if rows == nil {
		rc.LOG_ERR.Println("Select_rows err")
		return
	}
	/*获取本组数据的起始index和LOG_TIME*/
	if 0 == Last_trans_info.Start_time || 0 == Last_trans_info.Start_index {
		sql = fmt.Sprintf("select id, LOG_TIME from %s where SECURITY_SOFTWARE_ORGCODE = \"%s\" and LOG_TIME>%d and LOG_TIME<=%d and id > %d ORDER BY id limit 1", data_table_name, orgcode, time_start, time_stamp, last_index)
		rc.LOG_TRAC.Println("sql = ", sql)
		id_rows := rc.Select_rows(db, sql)
		if id_rows == nil {
			rc.LOG_ERR.Println("Select_rows err")
			return
		}
		for id_rows.Next() {
			id_rows.Scan(&Last_trans_info.Start_index, &Last_trans_info.Start_time)
		}
	}
	/*获取本组数据的结束index和LOG_TIME*/
	sql = fmt.Sprintf("select id, LOG_TIME from %s where SECURITY_SOFTWARE_ORGCODE = \"%s\" and LOG_TIME>%d and LOG_TIME<=%d and id > %d ORDER BY id desc limit 1", data_table_name, orgcode, time_start, time_stamp, last_index)
	rc.LOG_TRAC.Println("sql = ", sql)
	id_rows := rc.Select_rows(db, sql)
	if id_rows == nil {
		rc.LOG_ERR.Println("Select_rows err")
		return
	}
	for id_rows.Next() {
		id_rows.Scan(&Last_trans_info.Last_index, &Last_trans_info.Last_time)
	}
	Last_trans_info.Trans_status = NO_TRANS
	Write_bcp_file(rows, WA_BASIC_FJ_0002_IND, orgcode, data_src_code)
	/*数据传输成功，写入outputlog*/
	if Last_trans_info.Trans_status == TRANS_SUCC {
		Insert_succ_log(db, orgcode, data_type[WA_BASIC_FJ_0002_IND])
		Last_trans_info.Trans_status = NO_TRANS
		/*数据传输成功后清空起始参数*/
		Last_trans_info.Start_index = 0
		Last_trans_info.Start_time = 0
	} else if Last_trans_info.Trans_status == TRANS_FAIL {
		return
	}
	//limit_low += rc.MAX_BCP_RECORDS
	//	}
	if Files_for_zip.bcp_file_count > 0 {
		t := time.Now()
		t_stamp := int(t.Unix())
		Zip_handle(WA_BASIC_FJ_0002_IND, t_stamp, orgcode, data_src_code)
		/*数据传输成功，写入outputlog*/
		if Last_trans_info.Trans_status == TRANS_SUCC {
			Insert_succ_log(db, orgcode, data_type[WA_BASIC_FJ_0002_IND])
			Last_trans_info.Trans_status = NO_TRANS
			/*数据传输成功后清空起始参数*/
			Last_trans_info.Start_index = 0
			Last_trans_info.Start_time = 0
		} else if Last_trans_info.Trans_status == TRANS_FAIL {
			return
		}
	}
}

func Netbar_data_handle(db *sql.DB, wacode string, time_stamp int, time_dua int, data_type []string) {
	data_table_name := fmt.Sprintf("%s%s", data_type[WA_BASIC_FJ_0001_IND], table_name_suffix)

	/*读取outputlog中最后传输成功的time和index*/
	last_index, last_time := Read_last_succ_log(db, data_type[WA_BASIC_FJ_0001_IND], wacode, time_stamp-time_dua, time_stamp)

	time_start := max_num(last_time, time_stamp-time_dua)

	sql := fmt.Sprintf("select count(*) from %s where NETBAR_WACODE = \"%s\" and LOG_TIME>%d and LOG_TIME<=%d and id>%d", data_table_name, wacode, time_start, time_stamp, last_index)
	data_count := rc.Select_counts(db, sql)
	rc.LOG_TRAC.Println("场所 ", wacode, " 数据类型：", data_type[WA_BASIC_FJ_0001_IND], " 数 ", data_count)
	if data_count == 0 {
		//rc.LOG_WARNING.Println("device ", ap_id, " ", data_type[i], " no data")
		return
	}

	sql = fmt.Sprintf("select DATA_SRC_CODE from %s where NETBAR_WACODE = \"%s\" and LOG_TIME>%d and LOG_TIME<=%d and id >%d limit 1", data_table_name, wacode, time_start, time_stamp, last_index)
	rows := rc.Select_rows(db, sql)
	if rows == nil {
		rc.LOG_ERR.Println("Select_rows err")
		return
	}
	data_src_code := ""
	for rows.Next() {
		rows.Scan(&data_src_code)
	}

	//limit_low := 0
	items_format := form_string_for_sql(WA_BASIC_FJ_0001_IND)
	//for round := data_count/rc.MAX_BCP_RECORDS + 1; round > 0; round-- {
	sql = fmt.Sprintf("select %s from %s where NETBAR_WACODE = \"%s\" and LOG_TIME>%d and LOG_TIME<=%d and id>%d limit 1", items_format, data_table_name, wacode, time_start, time_stamp, last_index)
	rc.LOG_TRAC.Println("sql = ", sql)
	rows = rc.Select_rows(db, sql)
	if rows == nil {
		rc.LOG_ERR.Println("Select_rows err")
		return
	}
	/*获取本组数据的起始index和LOG_TIME*/
	if 0 == Last_trans_info.Start_time || 0 == Last_trans_info.Start_index {
		sql = fmt.Sprintf("select id, LOG_TIME from %s where NETBAR_WACODE = \"%s\" and LOG_TIME>%d and LOG_TIME<=%d and id > %d ORDER BY id limit 1", data_table_name, wacode, time_start, time_stamp, last_index)
		rc.LOG_TRAC.Println(sql)
		id_rows := rc.Select_rows(db, sql)
		if id_rows == nil {
			rc.LOG_ERR.Println("Select_rows err")
			return
		}
		for id_rows.Next() {
			id_rows.Scan(&Last_trans_info.Start_index, &Last_trans_info.Start_time)
		}
	}
	/*获取本组数据的结束index和LOG_TIME*/
	sql = fmt.Sprintf("select id, LOG_TIME from %s where NETBAR_WACODE = \"%s\" and LOG_TIME>%d and LOG_TIME<=%d and id > %d ORDER BY id desc limit 1", data_table_name, wacode, time_start, time_stamp, last_index)
	rc.LOG_TRAC.Println(sql)
	id_rows := rc.Select_rows(db, sql)
	if id_rows == nil {
		rc.LOG_ERR.Println("Select_rows err")
		return
	}
	for id_rows.Next() {
		id_rows.Scan(&Last_trans_info.Last_index, &Last_trans_info.Last_time)
	}
	Last_trans_info.Trans_status = NO_TRANS
	Write_bcp_file(rows, WA_BASIC_FJ_0001_IND, G_wacode[wacode], data_src_code)
	/*数据传输成功，写入outputlog*/
	if Last_trans_info.Trans_status == TRANS_SUCC {
		Insert_succ_log(db, wacode, data_type[WA_BASIC_FJ_0001_IND])
		Last_trans_info.Trans_status = NO_TRANS
		/*数据传输成功后清空起始参数*/
		Last_trans_info.Start_index = 0
		Last_trans_info.Start_time = 0
	} else if Last_trans_info.Trans_status == TRANS_FAIL {
		return
	}
	//limit_low += rc.MAX_BCP_RECORDS
	//}
	if Files_for_zip.bcp_file_count > 0 {
		t := time.Now()
		t_stamp := int(t.Unix())
		Zip_handle(WA_BASIC_FJ_0001_IND, t_stamp, G_wacode[wacode], data_src_code)
		if Last_trans_info.Trans_status == TRANS_SUCC {
			Insert_succ_log(db, wacode, data_type[WA_BASIC_FJ_0001_IND])
			Last_trans_info.Trans_status = NO_TRANS
			/*数据传输成功后清空起始参数*/
			Last_trans_info.Start_index = 0
			Last_trans_info.Start_time = 0
		} else if Last_trans_info.Trans_status == TRANS_FAIL {
			return
		}
	}
}

func Ap_data_handle(db *sql.DB, ap_id string, time_stamp int, time_dua int, data_type []string) {
	for i := WA_SOURCE_FJ_0001_IND; i <= WA_BASIC_FJ_0003_2_IND; i++ {
		data_table_name := fmt.Sprintf("%s%s", data_type[i], table_name_suffix)

		/*读取outputlog中最后传输成功的time和index*/
		last_index, last_time := Read_last_succ_log(db, data_type[i], ap_id, time_stamp-time_dua, time_stamp)

		time_start := max_num(last_time, time_stamp-time_dua)

		sql := fmt.Sprintf("select count(*) from %s where COLLECTION_EQUIPMENT_ID = \"%s\" and LOG_TIME>%d and LOG_TIME<=%d and id>%d", data_table_name, ap_id, time_start, time_stamp, last_index)
		data_count := rc.Select_counts(db, sql)
		rc.LOG_TRAC.Println("设备 ", ap_id, " 数据类型：", data_type[i], " 数 ", data_count)
		if data_count == 0 {
			//rc.LOG_WARNING.Println("device ", ap_id, " ", data_type[i], " no data")
			continue
		}

		sql = fmt.Sprintf("select DATA_SRC_CODE from %s where COLLECTION_EQUIPMENT_ID = \"%s\" and LOG_TIME>%d and LOG_TIME<=%d and id>%d limit 1", data_table_name, ap_id, time_start, time_stamp, last_index)
		rows := rc.Select_rows(db, sql)
		if rows == nil {
			rc.LOG_ERR.Println("Select_rows err")
			continue
		}
		data_src_code := ""
		for rows.Next() {
			rows.Scan(&data_src_code)
		}

		limit_low := 0
		items_format := form_string_for_sql(i)
		max_round := data_count/rc.MAX_BCP_RECORDS + 1
		remainder := data_count % rc.MAX_BCP_RECORDS
		for round := 1; round <= max_round; round++ {

			sql := fmt.Sprintf("select %s from %s where COLLECTION_EQUIPMENT_ID = \"%s\" and LOG_TIME>%d and LOG_TIME<=%d limit %d ,%d", items_format, data_table_name, ap_id, time_start, time_stamp, limit_low, rc.MAX_BCP_RECORDS)
			rc.LOG_TRAC.Println("sql = ", sql)
			rows := rc.Select_rows(db, sql)
			if rows == nil {
				rc.LOG_ERR.Println("Select_rows err")
				continue
			}
			/*获取本组数据的起始index和LOG_TIME*/
			if 0 == Last_trans_info.Start_time || 0 == Last_trans_info.Start_index {
				sql = fmt.Sprintf("select id, LOG_TIME from %s where COLLECTION_EQUIPMENT_ID = \"%s\" and LOG_TIME>%d and LOG_TIME<=%d and id > %d ORDER BY id limit %d,1", data_table_name, ap_id, time_start, time_stamp, last_index, limit_low)
				rc.LOG_TRAC.Println("sql = ", sql)
				id_rows := rc.Select_rows(db, sql)
				if id_rows == nil {
					rc.LOG_ERR.Println("Select_rows err")
					return
				}
				for id_rows.Next() {
					id_rows.Scan(&Last_trans_info.Start_index, &Last_trans_info.Start_time)
				}
			}
			/*获取本组数据的结束index和LOG_TIME*/
			tmp_limit := 0
			if round == max_round && remainder != 0 {
				tmp_limit = limit_low + remainder - 1
			} else {
				tmp_limit = limit_low + rc.MAX_BCP_RECORDS - 1
			}
			sql = fmt.Sprintf("select id, LOG_TIME from %s where COLLECTION_EQUIPMENT_ID = \"%s\" and LOG_TIME>%d and LOG_TIME<=%d and id > %d ORDER BY id limit %d,1", data_table_name, ap_id, time_start, time_stamp, last_index, tmp_limit)
			rc.LOG_TRAC.Println("sql = ", sql)
			id_rows := rc.Select_rows(db, sql)
			if id_rows == nil {
				rc.LOG_ERR.Println("Select_rows err")
				return
			}
			for id_rows.Next() {
				id_rows.Scan(&Last_trans_info.Last_index, &Last_trans_info.Last_time)
			}
			Last_trans_info.Trans_status = NO_TRANS
			//sql = fmt.Sprintf("select id from WA_BASIC_FJ_0002_log ORDER BY LOG_TIME DESC limit 1", ...)
			Write_bcp_file(rows, i, G_wacode[G_ap_id[ap_id]], data_src_code)
			if Last_trans_info.Trans_status == TRANS_SUCC {
				Insert_succ_log(db, ap_id, data_type[i])
				Last_trans_info.Trans_status = NO_TRANS
				/*数据传输成功后清空起始参数*/
				Last_trans_info.Start_index = 0
				Last_trans_info.Start_time = 0
			} else if Last_trans_info.Trans_status == TRANS_FAIL {
				continue
			}
			limit_low += rc.MAX_BCP_RECORDS
		}
		if Files_for_zip.bcp_file_count > 0 {
			t := time.Now()
			t_stamp := int(t.Unix())
			Zip_handle(i, t_stamp, G_wacode[G_ap_id[ap_id]], data_src_code)
			if Last_trans_info.Trans_status == TRANS_SUCC {
				Insert_succ_log(db, ap_id, data_type[i])
				Last_trans_info.Trans_status = NO_TRANS
				/*数据传输成功后清空起始参数*/
				Last_trans_info.Start_index = 0
				Last_trans_info.Start_time = 0
			} else if Last_trans_info.Trans_status == TRANS_FAIL {
				continue
			}
		}
	}
}

/*根据xml生成需要读取的字段串*/
func form_string_for_sql(index int) string {
	var buff bytes.Buffer
	st_len := len(g_xml_itmes_info[index])
	i := 1
	for _, st := range g_xml_itmes_info[index] {
		buff.WriteString(st.Eng)
		if i != st_len {
			buff.WriteString(",")
		}
		i++
	}
	rc.LOG_WARNING.Println(buff.String())
	return buff.String()
}

func max_num(a, b int) int {
	if a >= b {
		return a
	} else {
		return b
	}
}

func Db_conn_test(db *sql.DB) bool {
	sql := fmt.Sprintf("select SECURITY_SOFTWARE_ORGCODE from %s limit 1", rc.G_table_name.Table_org_status)
	c := rc.Select_rows(db, sql)
	if c == nil {
		return false
	}
	return true
}
