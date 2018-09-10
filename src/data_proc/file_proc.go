package data_proc

import (
	"bytes"
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"os"
	"os/exec"
	rc "output/src/rc_config"
	"strconv"
	"time"
)

type file_s struct {
	//bcp_file_name    string
	//current_bcp_file *os.File
	Record_count   int          //当前bcp文件中的记录数
	Bcp_number     [MAX_IND]int //bcp文件号，最大9999后清0
	Zip_number     int          //zip文件号，最大9999后清0
	bcp_file_count int          //当前zip中的bcp文件数，最大100后进行zip压缩
	Bcp_path       string       //当前bcp文件目录
	//Xml_name       string
	buff bytes.Buffer
}

type Last_trans_st struct {
	Trans_status int //0:no trans,1:transed;-1:trans fail
	Start_time   int
	Start_index  int
	Last_time    int
	Last_index   int
}

type ZIP struct {
	Data_index    int
	T_stamp       int
	Org_code      string
	Data_src_code string
	Rpt_xml_name  string
	Zip_name      string //zip file name
	Whole_name    string // zip whole name ,with path
}

var g_xml_info []MESSAGE
var g_xml_itmes_info [][]ITEMS_2
var Files_for_zip file_s
var Last_trans_info Last_trans_st

/*
在xml中记录zip包里的bcp信息，map[bcp文件名]记录条数
*/
var Bcp_records_in_xml = make(map[string]string)

const (
	NO_TRANS = iota
	TRANS_SUCC
	TRANS_FAIL
)

const (
	//Zip_path         = "./bcp_dir" //存放zip和bcp文件夹的目录
	Xml_suffix       = "GAB_ZIP_INDEX.xml"
	data_dst_code    = "123456"
	MAX_BCP_FILE_NUM = 100
)

func Xml_init() {
	data_type := Get_data_type()

	for i := WA_SOURCE_FJ_0001_IND; i <= WA_BASIC_FJ_0002_IND; i++ {
		path := fmt.Sprintf("%s%s.xml", rc.Template_path, data_type[i])
		fmt.Println(path)
		//g_xml_info[i] = Parse_xml(path)
		msg, err := Parse_xml(path)
		if err != nil {
			rc.LOG_ERR.Println(err)
			continue
		}
		items := msg.Dataset1.Data1.Dataset2.Data2.Dataset3[1].Data3[0].Items
		g_xml_info = append(g_xml_info, msg)
		g_xml_itmes_info = append(g_xml_itmes_info, items)
	}
}

/*
每次打开一个新的bcp，一次写入最多1000行记录，写完就关闭
*/
func Write_bcp_file(rows *sql.Rows, data_index int, org_code, data_src_code string) {
	rc.LOG_TRAC.Println("write file !")

	dt := Get_data_type()
	data_type := dt[data_index]
	/*if dir not exist, create it*/
	Files_for_zip.Bcp_path = fmt.Sprintf("%s/%s", rc.Zip_path, data_type)
	_, err := os.Stat(Files_for_zip.Bcp_path)
	if err != nil {
		os.MkdirAll(Files_for_zip.Bcp_path, os.ModePerm)
	}

	Files_for_zip.Bcp_number[data_index]++
	if Files_for_zip.Bcp_number[data_index] > 9999 {
		Files_for_zip.Bcp_number[data_index] = 1
	}
	t := time.Now()
	t_stamp := int(t.Unix())
	file_name := fmt.Sprintf("145-%s-%d-%05d-%s-0.bcp", data_src_code, t_stamp, Files_for_zip.Bcp_number[data_index], data_type)
	whole_path := fmt.Sprintf("%s/%s", Files_for_zip.Bcp_path, file_name)
	//fmt.Println("whole paht = ", Whole_path)
	current_bcp_file, _ := os.Create(whole_path)
	defer func() {
		current_bcp_file.Close()
		Files_for_zip.bcp_file_count++
		//记录bcp文件信息到缓存map
		Bcp_records_in_xml[file_name] = strconv.Itoa(Files_for_zip.Record_count)
		/*bcp文件超过最大数量，进行zip压缩并ftp传输*/
		if Files_for_zip.bcp_file_count >= rc.MAX_BCP_FILES {
			Zip_handle(data_index, t_stamp, org_code, data_src_code)
		}
	}()
	cols, _ := rows.Columns()
	col_len := len(cols)
	//这里表示一行所有列的值，用[]byte表示
	vals := make([][]byte, col_len)
	//这里表示一行填充数据
	scans := make([]interface{}, col_len)
	//这里scans引用vals，把数据填充到[]byte里
	for k, _ := range vals {
		scans[k] = &vals[k]
	}
	var data_buffer bytes.Buffer
	items_len := len(g_xml_itmes_info[data_index])
	Files_for_zip.Record_count = 0
	/*依次读取每行数据*/
	for rows.Next() {
		rows.Scan(scans...)
		i := 1
		//把vals中的数据复制到row中
		for _, v := range vals {
			data_buffer.WriteString(string(v))
			if i < items_len {
				//data_buffer = append(data_buffer, byte("\t"))
				data_buffer.WriteString("\t")
			} else {
				//data_buffer = append(data_buffer, byte("\n"))
				data_buffer.WriteString("\n")
			}
			i++
		}
		Files_for_zip.Record_count++
	}
	//fmt.Println(data_buffer.String())
	current_bcp_file.WriteString(data_buffer.String())
}

/*
zip handle
create zip; encrypt; transfer by ftp
*/
func Zip_handle(data_index int, t_stamp int, org_code, data_src_code string) {

	var tmp_zip ZIP
	tmp_zip.Data_index = data_index
	tmp_zip.T_stamp = t_stamp
	tmp_zip.Org_code = org_code
	tmp_zip.Data_src_code = data_src_code
	/*
	   var tmp_msg MESSAGE
	   tmp_msg = g_xml_info[data_index]
	   create_rpt_xml_format(&tmp_msg)
	   rpt_xml_name := fmt.Sprintf("%s/%s", Files_for_zip.Bcp_path, Xml_suffix)
	   Create_xml(rpt_xml_name, tmp_msg)
	   Files_for_zip.Zip_number++
	   if Files_for_zip.Zip_number >= 9999 {
	       Files_for_zip.Zip_number = 1
	   }
	   zip_name := fmt.Sprintf("145-%s-%s-%s-%d-%05d.zip", org_code, data_src_code, G_conf.Conf.Data_dst_code, t_stamp, Files_for_zip.Zip_number)
	   zip_whole_name := fmt.Sprintf("%s/145-%s-%s-%s-%d-%05d", Zip_path, org_code, data_src_code, data_dst_code, t_stamp, Files_for_zip.Zip_number)
	   Create_zip(zip_whole_name, rpt_xml_name)
	*/

	tmp_zip.Create_rpt_xml()
	tmp_zip.create_zip_name()
	tmp_zip.Create_zip()
	tmp_zip.encrypt_zip()

	ret := false
	for i := 0; i < 3 && !ret; i++ {
		ret = tmp_zip.trans_by_ftp()
	}
	if !ret {
		rc.LOG_ERR.Println("ftp transfer failed for 3 times!")
		Files_for_zip.bcp_file_count = 0
		Last_trans_info.Trans_status = TRANS_FAIL
		return
	}
	Files_for_zip.bcp_file_count = 0
	Last_trans_info.Trans_status = TRANS_SUCC
}

/*
生成上报的xml格式，添加bcp描述字段
*/
func (zip ZIP) create_rpt_xml_format(msg *MESSAGE) {
	data_slice := msg.Dataset1.Data1.Dataset2.Data2.Dataset3[0].Data3
	data_format := data_slice[0]
	data_slice = append(data_slice[0:0])
	//rc.LOG_TRAC.Println("tmp_data ", data_format)
	for key, val := range Bcp_records_in_xml {
		tmp_data := data_format
		tmp_data.Items = make([]ITEMS_2, 3)
		for i := 0; i <= 2; i++ {
			tmp_data.Items[i].Key = data_format.Items[i].Key
			tmp_data.Items[i].Rmk = data_format.Items[i].Rmk
		}
		tmp_data.Items[1].Val = key
		tmp_data.Items[2].Val = val
		//rc.LOG_TRAC.Println("data_slice_before : ", data_slice)
		//rc.LOG_TRAC.Println("tmp_data : ", tmp_data)
		data_slice = append(data_slice, tmp_data)
		//rc.LOG_TRAC.Println("data_slice_after : ", data_slice)
		//rc.LOG_TRAC.Println("data_slice : ", data_slice)
		delete(Bcp_records_in_xml, key)
	}
	msg.Dataset1.Data1.Dataset2.Data2.Dataset3[0].Data3 = data_slice
	//rc.LOG_TRAC.Println(msg.Dataset1.Data1.Dataset2.Data2.Dataset3[0].Data3)
}

func (zip *ZIP) Create_rpt_xml() {
	tmp_msg := g_xml_info[zip.Data_index]
	zip.create_rpt_xml_format(&tmp_msg)
	zip.Rpt_xml_name = fmt.Sprintf("%s/%s", Files_for_zip.Bcp_path, Xml_suffix)
	Create_xml(zip.Rpt_xml_name, tmp_msg)
}

func (zip *ZIP) create_zip_name() {
	Files_for_zip.Zip_number++
	if Files_for_zip.Zip_number >= 9999 {
		Files_for_zip.Zip_number = 1
	}

	zip.Zip_name = fmt.Sprintf("145-%s-%s-%s-%d-%05d.zip", zip.Org_code, zip.Data_src_code, G_conf.Conf.Data_dst_code, zip.T_stamp, Files_for_zip.Zip_number)
	zip.Whole_name = fmt.Sprintf("%s/%s", rc.Zip_path, zip.Zip_name)
}

func (zip ZIP) trans_by_ftp() bool {
	rc.LOG_TRAC.Println("trans ftp  ", zip.Zip_name)
	ftp_host := G_conf.Conf.Ftp.Ftp_host
	ftp_account := G_conf.Conf.Ftp.Ftp_account
	ftp_psd := G_conf.Conf.Ftp.Ftp_psd
	ftp_port := G_conf.Conf.Ftp.Ftp_port
	ftp_path := G_conf.Conf.Ftp.Ftp_path

	tmp_name := fmt.Sprintf("%s.tmp", zip.Zip_name)
	cmd_string := ""
	if len(ftp_path) != 0 {
		cmd_string = fmt.Sprintf("curl --ftp-create-dirs --connect-timeout 10 -m 20 -u %s:%s -T %s ftp://%s:%s/%s/%s -Q \"-RNFR %s\" -Q \"-RNTO %s\"", ftp_account, ftp_psd, zip.Whole_name, ftp_host, ftp_port, ftp_path, tmp_name, tmp_name, zip.Zip_name)
	} else {
		cmd_string = fmt.Sprintf("curl --ftp-create-dirs --connect-timeout 10 -m 20 -u %s:%s -T %s ftp://%s:%s/%s -Q \"-RNFR %s\" -Q \"-RNTO %s\"", ftp_account, ftp_psd, zip.Whole_name, ftp_host, ftp_port, tmp_name, tmp_name, zip.Zip_name)
	}

	rc.LOG_INFO.Println("ftp cmd ", cmd_string)
	cmd := exec.Command("/bin/bash", "-c", cmd_string)
	_, errs := cmd.Output()

	/*上传无论成功失败都把zip删掉*/
	os.Remove(zip.Whole_name)
	if errs != nil {
		rc.LOG_ERR.Println(errs)
		return false
	}

	return true
}

/*
生成zip文件
*/
func (zip ZIP) Create_zip() {
	cmd_string := fmt.Sprintf("zip -0 -j -m %s %s %s/*.bcp", zip.Whole_name, zip.Rpt_xml_name, Files_for_zip.Bcp_path)
	rc.LOG_TRAC.Println("zip cmd ", cmd_string)
	cmd := exec.Command("/bin/bash", "-c", cmd_string)
	bytes, errs := cmd.Output()
	if errs != nil {
		rc.LOG_ERR.Println(errs)
		return
	}
	rc.LOG_TRAC.Println(string(bytes))
}

/*
encode
*/
func (zip ZIP) encrypt_zip() {
	key_str := fmt.Sprintf("%02X%02X%02X%02X%02X%02X%02X%02X", G_conf.Conf.Key[0], G_conf.Conf.Key[1], G_conf.Conf.Key[2], G_conf.Conf.Key[3], G_conf.Conf.Key[4], G_conf.Conf.Key[5], G_conf.Conf.Key[6], G_conf.Conf.Key[7])
	iv_str := fmt.Sprintf("%02X%02X%02X%02X%02X%02X%02X%02X", G_conf.Conf.Iv[0], G_conf.Conf.Iv[1], G_conf.Conf.Iv[2], G_conf.Conf.Iv[3], G_conf.Conf.Iv[4], G_conf.Conf.Iv[5], G_conf.Conf.Iv[6], G_conf.Conf.Iv[7])

	tmp_en := fmt.Sprintf("%s.en", zip.Whole_name)
	en_cmd := fmt.Sprintf("openssl des-cbc -K %s -iv %s -nosalt -in %s -out %s", key_str, iv_str, zip.Whole_name, tmp_en)
	rc.LOG_TRAC.Println("en_cmd = ", en_cmd)
	cmd := exec.Command("/bin/bash", "-c", en_cmd)
	cmd.Output()

	os.Remove(zip.Whole_name)

	base64_cmd := fmt.Sprintf("openssl base64 -in %s -out %s", tmp_en, zip.Whole_name)

	rc.LOG_TRAC.Println("en_cmd = ", base64_cmd)
	cmd = exec.Command("/bin/bash", "-c", base64_cmd)
	cmd.Output()

	os.Remove(tmp_en)
}
