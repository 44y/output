package data_proc

import (
	"encoding/xml"
	"fmt"
	"io/ioutil"
	"os"
	//"strings"
)

type MESSAGE struct {
	XMLName xml.Name `xml:"MESSAGE"`
	//Description string    `xml:",innerxml"`
	Dataset1 DATASET_1 `xml:DATASET`
}

type DATASET_1 struct {
	XMLName     xml.Name `xml:"DATASET"`
	Name        string   `xml:"name,attr"`
	Description string   `xml:"description,attr"`
	Rmk         string   `xml:"rmk,attr"`
	Data1       DATA_1   `xml:"DATA"`
	//Description2 string   `xml:",innerxml"`
}

type DATA_1 struct {
	XMLName  xml.Name  `xml:"DATA"`
	Dataset2 DATASET_2 `xml:DATASET`
	//Description3 string    `xml:",innerxml"`
}

type DATASET_2 struct {
	XMLName xml.Name `xml:"DATASET"`
	Name    string   `xml:"name,attr"`
	Rmk     string   `xml:"rmk,attr"`
	Data2   DATA_2   `xml:"DATA"`
	//Description4 string   `xml:",innerxml"`
}

type DATA_2 struct {
	XMLName  xml.Name    `xml:"DATA"`
	Items    []ITEMS_1   `xml:"ITEM"`
	Dataset3 []DATASET_3 `xml:"DATASET"`
}

type DATASET_3 struct {
	XMLName xml.Name `xml:"DATASET"`
	Name    string   `xml:"name,attr"`
	Rmk     string   `xml:"rmk,attr"`
	Data3   []DATA_3 `xml:"DATA"`
	//Description6 string   `xml:",innerxml"`
}

type DATA_3 struct {
	XMLName xml.Name  `xml:"DATA"`
	Items   []ITEMS_2 `xml:"ITEM"`
	//Description7 string   `xml:",innerxml"`
}

type ITEMS_1 struct {
	XMLName xml.Name `xml:"ITEM"`
	Key     string   `xml:"key,attr"`
	Val     string   `xml:"val,attr"`
	Rmk     string   `xml:"rmk,attr"`
}

type ITEMS_2 struct {
	XMLName xml.Name `xml:"ITEM"`
	Key     string   `xml:"key,attr"`
	Eng     string   `xml:"eng,attr"`
	Chn     string   `xml:"chn,attr"`
	Rmk     string   `xml:"rmk,attr"`
	Val     string   `xml:"val,attr"`
}

func Parse_xml(xml_path string) (MESSAGE, error) {
	v := MESSAGE{}
	file, err1 := os.Open(xml_path) // For read access.
	if err1 != nil {
		fmt.Printf("error: %v", err1)
		return v, err1
	}
	defer file.Close()
	data, err2 := ioutil.ReadAll(file)
	if err2 != nil {
		fmt.Printf("error: %v", err2)
		return v, err2
	}
	err3 := xml.Unmarshal(data, &v)
	if err3 != nil {
		fmt.Printf("error: %v", err3)
		return v, err3
	}
	return v, nil
}

func Parse_xml_data_items(xml_path string) []ITEMS_2 {
	message, err := Parse_xml(xml_path)
	if err != nil {
		return nil
	}
	inner_item := message.Dataset1.Data1.Dataset2.Data2.Dataset3[1].Data3[0].Items
	return inner_item
}

func Create_xml(xml_name string, v interface{}) {
	xmlOutPut, outPutErr := xml.MarshalIndent(v, "", "")
	if outPutErr == nil {
		//加入XML头
		headerBytes := []byte(xml.Header)
		//拼接XML头和实际XML内容
		xmlOutPutData := append(headerBytes, xmlOutPut...)
		//写入文件
		ioutil.WriteFile(xml_name, xmlOutPutData, os.ModePerm)

	} else {
		fmt.Println(outPutErr)
	}
}
