package rc_config

import (
	//"fmt"
	//"github.com/color"
	"log"
	"os"
)

type LOG_DBG_T struct {
}

type LOG_INFO_T struct {
}

type LOG_TRAC_T struct {
}

type LOG_WARNING_T struct {
}

type LOG_ERR_T struct {
}

type LOG_DEAD_T struct {
}

/*
func New(out io.Writer, prefix string, flag int) *Logger
该函数一共有三个参数：
（1）输出位置out，是一个io.Writer对象，该对象可以是一个文件也可以是实现了该接口的对象。通常我们可以用这个来指定日志输出到哪个文件。
（2）prefix 我们在前面已经看到，就是在日志内容前面的东西。我们可以将其置为 "[Info]" 、 "[Warning]"等来帮助区分日志级别。
（3） flags 是一个选项，显示日志开头的东西，可选的值有：
Ldate         = 1 << iota     // 形如 2009/01/23 的日期
Ltime                         // 形如 01:23:23   的时间
Lmicroseconds                 // 形如 01:23:23.123123   的时间
Llongfile                     // 全路径文件名和行号: /a/b/c/d.go:23
Lshortfile                    // 文件名和行号: d.go:23
LstdFlags     = Ldate | Ltime // 日期和时间
*/

//var log_dbg, log_info, log_trac, log_warning, log_err, log_dead *log.Logger
var LOG_DBG, LOG_INFO, LOG_TRAC, LOG_WARNING, LOG_ERR, LOG_DEAD *log.Logger

const (
	NORMAL = iota
	DEBUG
)

/*
var LOG_INFO LOG_INFO_T
var LOG_DBG LOG_DBG_T
var LOG_TRAC LOG_TRAC_T
var LOG_WARNING LOG_WARNING_T
var LOG_ERR LOG_ERR_T
var LOG_DEAD LOG_DEAD_T
*/

func Log_init(lvl int) {
	var out, out_file *os.File
	out_file, _ = os.Create(Running_log)
	switch lvl {
	case NORMAL:
		out = nil
	case DEBUG:
		out = os.Stdout
		out_file = os.Stdout
	default:
		out = nil
	}
	LOG_DBG = log.New(out, "[DEBUG]", log.LstdFlags|log.Lshortfile)
	LOG_INFO = log.New(out_file, "[INFO]", log.LstdFlags|log.Lshortfile)
	LOG_TRAC = log.New(out, "[TRAC]", log.LstdFlags|log.Lshortfile)
	LOG_WARNING = log.New(out_file, "[WARNING]", log.LstdFlags|log.Lshortfile)
	LOG_ERR = log.New(out_file, "[ERROR]", log.LstdFlags|log.Lshortfile)
	LOG_DEAD = log.New(out, "[DEAD]", log.LstdFlags|log.Lshortfile)
}

/*
func Log_init() {
	//file, _ := os.Create("./test.log")
	log_dbg = log.New(os.Stdout, "[DEBUG]", log.LstdFlags|log.Lshortfile)
	log_info = log.New(os.Stdout, "[INFO]", log.LstdFlags|log.Lshortfile)
	log_trac = log.New(os.Stdout, "[TRAC]", log.LstdFlags|log.Lshortfile)
	log_warning = log.New(os.Stdout, "[WARNING]", log.LstdFlags|log.Lshortfile)
	log_err = log.New(os.Stdout, "[ERROR]", log.LstdFlags|log.Lshortfile)
	log_dead = log.New(os.Stdout, "[DEAD]", log.LstdFlags|log.Lshortfile)

}
*/

/*
func (l LOG_INFO_T) Println(v ...interface{}) {
	color.Set(color.FgGreen)
	defer color.Unset()
	log_info.Println(v...)
}

func (l LOG_TRAC_T) Println(v ...interface{}) {
	color.Set(color.FgBlue)
	defer color.Unset()
	log_trac.Println(v...)
}

func (l LOG_ERR_T) Println(v ...interface{}) {
	color.Set(color.FgRed)
	defer color.Unset()
	log_err.Println(v...)
}

func (l LOG_WARNING_T) Println(v ...interface{}) {
	color.Set(color.FgYellow)
	defer color.Unset()
	log_warning.Println(v...)
}

func (l LOG_DBG_T) Println(v ...interface{}) {
	log_dbg.Println(v...)
}
*/
