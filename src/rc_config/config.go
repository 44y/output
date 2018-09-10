package rc_config

import (
	"fmt"
	"github.com/widuu/goini"
	"strconv"
	"crypto/sha256"
)

const (
	config_path = "/etc/auditoutput/config.ini"
)

var G_table_name struct {
	Table_org_status    string
	Table_netbar_status string
	Table_dev_status    string
	//Table_test          string
	Output_log string
}

var (
	MAX_BCP_RECORDS int
	MAX_BCP_FILES   int
	PID_PATH        string
	CONFIG_URL      string
	Template_path   string
	Zip_path        string
	Running_log     string
)

func Config_load_local() {
	conf := goini.SetConfig(config_path)
	sha256.Sum256()
	if conf == nil {
		fmt.Println("conf is nil")
		return
	}
	/*load database connect parameters*/
	g_db_config.db_user = conf.GetValue("DATABASE", "db_user")
	g_db_config.db_psw = conf.GetValue("DATABASE", "db_psw")
	g_db_config.db_host = conf.GetValue("DATABASE", "db_host")
	g_db_config.db_name = conf.GetValue("DATABASE", "db_name")
	//db_port_s := conf.GetValue("DATABASE", "db_port")
	//g_db_config.db_port, _ = strconv.Atoi(db_port_s)
	g_db_config.db_port, _ = strconv.Atoi(conf.GetValue("DATABASE", "db_port"))
	/*load table name*/
	G_table_name.Table_dev_status = conf.GetValue("TABLE", "tb_dev_status")
	G_table_name.Table_netbar_status = conf.GetValue("TABLE", "tb_netbar_status")
	G_table_name.Table_org_status = conf.GetValue("TABLE", "tb_org_status")
	//G_table_name.Table_test = conf.GetValue("TABLE", "tb_test")
	G_table_name.Output_log = conf.GetValue("TABLE", "tb_output_log")

	/*load file config*/
	MAX_BCP_RECORDS, _ = strconv.Atoi(conf.GetValue("FILE", "max_bcp_records"))
	MAX_BCP_FILES, _ = strconv.Atoi(conf.GetValue("FILE", "max_bcp_files"))
	//fmt.Println("MAX_BCP_RECORDS = ", MAX_BCP_RECORDS, "MAX_BCP_FILES = ", MAX_BCP_FILES)

	PID_PATH = conf.GetValue("CONFIG", "pid_path")
	CONFIG_URL = conf.GetValue("CONFIG", "config_url")
	tmp := conf.GetValue("CONFIG", "template_path")
	Template_path = fmt.Sprintf("%s/template/", tmp)
	Zip_path = conf.GetValue("CONFIG", "zip_path")
	Running_log = conf.GetValue("CONFIG", "running_log")
}
