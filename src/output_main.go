/*go version go1.9.2 linux/amd64*/

package main

import (
	"data_proc"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	//"os/exec"
	"os/signal"
	rc "output/src/rc_config"
	//"sync"
	"syscall"
	"time"
)

const (
	//pid_path         = "/var/transfer.pid"
	Day_seconds  int = 60 * 60 * 24
	Hour_seconds int = 60 * 60
	Min_seconds  int = 60
)

var (
	timer_change   = make(chan int, 1)
	Last_work_time int
)

func config_get(conf *data_proc.Json_config) {
	resp, err := http.Get(rc.CONFIG_URL)
	for err != nil {
		rc.LOG_WARNING.Println("http get config error, wait for 5 seconds and get again.", err)
		time.Sleep(5 * time.Second)
		resp, err = http.Get(rc.CONFIG_URL)
	}

	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		rc.LOG_WARNING.Println("Read  config error, keep trying reading.", err)
		body, err = ioutil.ReadAll(resp.Body)
	}

	fmt.Println(string(body))
	//json_string := string(body)
	//rc.LOG_TRAC.Println("json string is ", json_string)
	json.Unmarshal(body, conf)
	//rc.LOG_TRAC.Println(*conf)
}

/*
	SIGUSR1:厂商、场所、设备信息变更
	SIGUSR2:配置变更，ftp、上传时间点等
*/
func signal_Listen() {
	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGUSR1, syscall.SIGUSR2)
	for {
		s := <-c
		switch s {
		case syscall.SIGUSR1:
			netbar_change_process(s)
		case syscall.SIGUSR2:
			trans_config_change_process(s)
		default:
			rc.LOG_TRAC.Println("unknow signal", s)
		}
	}
}

/*
	SIGUSR1:厂商、场所、设备信息变更
*/
func netbar_change_process(s os.Signal) {
	rc.LOG_INFO.Println("i got a signal:", s)
	/*对比出变化的厂商、场所、设备*/
	tmp_db := rc.Connect_db()
	defer func() {
		tmp_db.Close()
		data_proc.Working_mtx.Unlock()
	}()

	for tmp_db == nil {
		rc.LOG_ERR.Println("rc.Connect_db fail,wait for 5 seconds")
		time.Sleep(5 * time.Second)
		tmp_db = rc.Connect_db()
	}

	conn_status := data_proc.Db_conn_test(tmp_db)
	for conn_status == false {
		rc.LOG_ERR.Println("rc.Db_conn_test fail,wait for 5 seconds")
		time.Sleep(5 * time.Second)
		conn_status = data_proc.Db_conn_test(tmp_db)
	}
	/*正在传输数据，等待传输完成直接修改全局信息*/
	if data_proc.Working_flag {
		data_proc.Working_mtx.Lock()
		data_proc.Init_status_tables(tmp_db, &data_proc.G_orgcode, data_proc.G_wacode, data_proc.G_ap_id)
		return
	}
	data_proc.Working_mtx.Lock()
	var tmp_orgcode []string
	tmp_wacode := make(map[string]string)
	tmp_ap_id := make(map[string]string)
	data_proc.Init_status_tables(tmp_db, &tmp_orgcode, tmp_wacode, tmp_ap_id)

	rc.LOG_INFO.Println(tmp_orgcode)
	rc.LOG_INFO.Println(tmp_wacode)
	rc.LOG_INFO.Println(tmp_ap_id)
	data_type_list := data_proc.Get_data_type()
	time_now := int(time.Now().Unix())
	/*对比厂商列表*/
	for _, v := range data_proc.G_orgcode {
		is_found := false
		for _, tmp_v := range tmp_orgcode {
			if v == tmp_v {
				is_found = true
				break
			}
		}
		/*厂商信息变更，立即上报*/
		if !is_found {
			rc.LOG_INFO.Println("厂商 changed ", v)
			data_proc.Org_data_handle(tmp_db, v, time_now, Day_seconds, data_type_list)
		}
	}
	/*对比场所列表*/
	for v, _ := range data_proc.G_wacode {
		is_found := false
		for tmp_v, _ := range tmp_wacode {
			if v == tmp_v {
				is_found = true
				break
			}
		}
		if !is_found {
			rc.LOG_INFO.Println("场所 changed ", v)
			data_proc.Netbar_data_handle(tmp_db, v, time_now, Day_seconds, data_type_list)
		}
	}
	/*对比设备列表*/
	for v, _ := range data_proc.G_ap_id {
		is_found := false
		for tmp_v, _ := range tmp_ap_id {
			if v == tmp_v {
				is_found = true
				break
			}
		}
		if !is_found {
			rc.LOG_INFO.Println("设备 changed ", v)
			data_proc.Ap_data_handle(tmp_db, v, time_now, Day_seconds, data_type_list)
		}
	}
	/*修改全局表*/
	data_proc.G_orgcode = tmp_orgcode
	data_proc.G_wacode = tmp_wacode
	data_proc.G_ap_id = tmp_ap_id
}

/*
	SIGUSR2:配置变更，ftp、上传时间点等
*/
func trans_config_change_process(s os.Signal) {
	rc.LOG_INFO.Println("i got a signal:", s)
	var tmp_conf data_proc.Json_config
	config_get(&tmp_conf)
	rc.LOG_INFO.Println("tmp_conf", tmp_conf)
	if tmp_conf.Conf.Ftp != data_proc.G_conf.Conf.Ftp || tmp_conf.Conf.Data_dst_code != data_proc.G_conf.Conf.Data_dst_code || tmp_conf.Conf.Key != data_proc.G_conf.Conf.Key || tmp_conf.Conf.Iv != data_proc.G_conf.Conf.Iv {
		rc.LOG_TRAC.Println("ftp or encode config changed")
		data_proc.RC_mtx.Lock()
		data_proc.G_conf.Conf.Ftp = tmp_conf.Conf.Ftp
		data_proc.G_conf.Conf.Data_dst_code = tmp_conf.Conf.Data_dst_code
		data_proc.G_conf.Conf.Key = tmp_conf.Conf.Key
		data_proc.G_conf.Conf.Iv = tmp_conf.Conf.Iv
		data_proc.RC_mtx.Unlock()
	}

	if tmp_conf.Conf.Output_time != data_proc.G_conf.Conf.Output_time {
		rc.LOG_INFO.Println("output time changed")
		var hour, min int
		fmt.Sscanf(tmp_conf.Conf.Output_time, "%d:%d", &hour, &min)
		/*计算距离当天0点的相对秒数*/
		conf_sec := hour*Hour_seconds + min*Min_seconds
		//now_sec := (int(time.Now().Unix()) + 8*Hour_seconds) % Day_seconds

		time_now := time.Now()
		time_date := time.Date(time_now.Year(), time_now.Month(), time_now.Day(), 0, 0, 0, 0, time_now.Location())
		now_sec := int(time_date.Unix())
		/*停下当前工作*/
		if data_proc.Working_flag {
			data_proc.Working_flag = false
			data_proc.WAIT_GROUP.Wait()
		}
		/*若配置时间小于当前时间，立即work*/
		if conf_sec < now_sec {
			go data_proc.Data_process_handle()
		}
		/*修改timer到当前配置*/
		data_proc.G_conf.Conf.Output_time = tmp_conf.Conf.Output_time
		tmp := calc_next_time()
		timer_change <- tmp
	}
}

/*收到退出信号，等待当前zip包处理完成后退出程序*/
func signal_safe_quit(s os.Signal) {
	rc.LOG_WARNING.Println("i got a signal:", s)
	data_proc.Working_flag = false
	//data_proc.Working_mtx.Lock()
	data_proc.WAIT_GROUP.Wait()

}

/*返回距离下次work的seconds*/
func calc_next_time() int {
	var hour, min int
	fmt.Sscanf(data_proc.G_conf.Conf.Output_time, "%d:%d", &hour, &min)
	/*计算距离当天0点的相对秒数*/
	conf_sec := hour*Hour_seconds + min*Min_seconds
	//last_sec := (Last_work_time + 8*Hour_seconds) % Day_seconds
	now_sec := (int(time.Now().Unix()) + 8*Hour_seconds) % Day_seconds
	if conf_sec >= now_sec {
		return conf_sec - now_sec
	} else {
		return conf_sec + Day_seconds - now_sec
	}

}

/*trans timestamp to "2006-01-02 15:04:05"*/
func trans_time_format(raw int) string {
	tm := time.Unix(int64(raw), 0)
	ret := tm.Format("2006-01-02 15:04:05")
	return ret
}

func show_log() {
	tmp_db := rc.Connect_db()
	defer func() {
		tmp_db.Close()
	}()

	if tmp_db == nil {
		fmt.Println("rc.Connect_db fail")
		return
	}
	sql := fmt.Sprintf("select code, data_type, start_time, start_index, last_time, last_index from output_log order by id desc limit 10")
	fmt.Println(sql)
	rows := rc.Select_rows(tmp_db, sql)
	if rows == nil {
		fmt.Println("Select_rows err")
		return
	}
	var start_time, start_index, last_index, last_time int
	var code, data_type string

	for rows.Next() {
		rows.Scan(&code, &data_type, &start_time, &start_index, &last_time, &last_index)
		s_t := trans_time_format(start_time)
		l_t := trans_time_format(last_time)
		fmt.Printf("%s\t\t%s\t\t%s\t\t%d\t\t%s\t\t%d\n", code, data_type, s_t, start_index, l_t, last_index)
	}
}

func show_config() {
	tmp_conf := &data_proc.Json_config{}
	config_get(tmp_conf)
	fmt.Printf("%+v\n", *tmp_conf)
}

func show_help() {
	fmt.Printf("usage:\n-D:debug mode\n-SHOW:show output log\n")
	return
}

func main() {
	//fmt.Println("main func !")

	/*load local config*/
	rc.Config_load_local()

	/*print log in debug mode*/
	if len(os.Args) == 2 {
		switch os.Args[1] {
		case "-D":
			rc.Log_init(rc.DEBUG)
		case "-LOG":
			show_log()
			return
		case "-CONFIG":
			show_config()
			return
		default:
			show_help()
			return
		}
	} else if len(os.Args) != 1 {
		show_help()
		return
	} else {
		rc.Log_init(rc.NORMAL)
	}

	/*get config by http*/
	config_get(&data_proc.G_conf)
	//rc.LOG_TRAC.Println("log init success")

	pid_file, _ := os.Create(rc.PID_PATH)
	fmt.Fprintln(pid_file, os.Getpid())
	pid_file.Close()
	//defer os.Remove(rc.PID_PATH)

	data_proc.Xml_init()

	/*quit signal process*/
	quit_chan := make(chan os.Signal, 1)
	signal.Notify(quit_chan, syscall.SIGQUIT, syscall.SIGINT)
	rc.LOG_TRAC.Println("add notify sigquit and sigint, ", quit_chan)

	/*try working*/
	go data_proc.Data_process_handle()

	/*一次处理的时间长度，初始化为一天*/
	/*fmt.Println(time.Now().Format("2006-01-02 15:04:05"))*/

	time_to_wait := calc_next_time()
	rc.LOG_INFO.Println("time_to_wait ", time_to_wait)
	time_up := time.After(time.Duration(time_to_wait) * time.Second)

	go signal_Listen()

	for {
		select {
		case <-time_up:
			rc.LOG_TRAC.Println("got time_up, work!")
			//Last_work_time = time.Now().Unix()
			go data_proc.Data_process_handle()
			time_to_wait = calc_next_time()
			rc.LOG_INFO.Println("time_to_wait ", time_to_wait)
			time_up = time.After(time.Duration(time_to_wait) * time.Second)
		case time_to_wait := <-timer_change:
			rc.LOG_TRAC.Println("got timer change,reset time_up timer: ", time_to_wait)
			time_up = time.After(time.Duration(time_to_wait) * time.Second)
		case q := <-quit_chan:
			//rc.LOG_TRAC.Println("got ", q)
			signal_safe_quit(q)
			rc.LOG_TRAC.Println("SAFE QUIT")
			return
		default:
			//rc.LOG_ERR.Println("unknow channel")
			rc.LOG_TRAC.Println("no signal,sleep for 5 seconds")
			time.Sleep(5 * time.Second)
		}
	}
}
