package finaglethrift

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/elastic/beats/libbeat/logp"
	"fmt"
	"strings"
)

func thriftIdlForTesting(t *testing.T, content string) *ThriftIdl {
	f, _ := ioutil.TempFile("", "")
	defer os.Remove(f.Name())

	f.WriteString(content)
	f.Close()

	idl, err := NewThriftIdl([]string{f.Name()})
	if err != nil {
		t.Fatal("Parsing failed:", err)
	}

	return idl
}

func TestThriftIdl_thriftReadFiles(t *testing.T) {

	if testing.Verbose() {
		logp.LogInit(logp.LOG_DEBUG, "", false, true, []string{"thrift", "thriftdetailed"})
	}

	idl := thriftIdlForTesting(t, `
/* simple test */
service Test {
       i32 add(1:i32 num1, 2: i32 num2)
}
`)

	methods_map := idl.MethodsByName
	if len(methods_map) == 0 {
		t.Error("Empty methods_map")
	}
	m, exists := methods_map["add"]
	if !exists || m.Service == nil || m.Method == nil ||
		m.Service.Name != "Test" || m.Method.Name != "add" {

		t.Error("Bad data:", m)
	}
	if *m.Params[1] != "num1" || *m.Params[2] != "num2" {
		t.Error("Bad params", m.Params)
	}
	if len(m.Exceptions) != 0 {
		t.Error("Non empty exceptions", m.Exceptions)
	}
}


func TestThriftIdl_newThriftIdl(t *testing.T){
	var fileList1 []string //获取文件列表

	var thriftPath = "/work/goworkspace/src/github.com/elastic/beats/packetbeat/test_cfg"

	fileList1,_ = getFileList(thriftPath, ".thrift")


	for index, value := range fileList1 {
		fmt.Println("Index = ", index, "Value = ", value)
	}

	//var idl_files = []string{"/work/goworkspace/src/github.com/elastic/beats/packetbeat/test_cfg/result.thrift","/work/goworkspace/src/github.com/elastic/beats/packetbeat/test_cfg/order.thrift"}

	fmt.Println(1)
	_, err := NewThriftIdl(fileList1)
	if err != nil {
		t.Fatal("Parsing failed:", err)
	}
}


func getFileList(filePath string, suffix string) (files []string, err error) {
	var fileList []string //获取文件列表
	dir, err := ioutil.ReadDir(filePath)
	if err != nil {
		return nil, err
	}
	PthSep := string(os.PathSeparator)
	suffix = strings.ToUpper(suffix) //忽略后缀匹配的大小写
	for _, fi := range dir {
		if fi.IsDir() { // 忽略目录
			continue
		}
		if strings.HasSuffix(strings.ToUpper(fi.Name()), suffix) { //匹配文件
			fileList = append(fileList, filePath+PthSep+fi.Name())
		}
	}
	return fileList, nil
}
