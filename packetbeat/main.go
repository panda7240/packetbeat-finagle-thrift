package main

import (
	"os"

	"github.com/elastic/beats/libbeat/beat"
	"github.com/elastic/beats/packetbeat/beater"

	// import support protocol modules
	"github.com/siye1982/eagleye-health/config"
	"flag"
	"fmt"
	_ "github.com/elastic/beats/packetbeat/protos/amqp"
	_ "github.com/elastic/beats/packetbeat/protos/dns"
	_ "github.com/elastic/beats/packetbeat/protos/finaglethrift"
	_ "github.com/elastic/beats/packetbeat/protos/http"
	_ "github.com/elastic/beats/packetbeat/protos/memcache"
	_ "github.com/elastic/beats/packetbeat/protos/mongodb"
	_ "github.com/elastic/beats/packetbeat/protos/mysql"
	_ "github.com/elastic/beats/packetbeat/protos/nfs"
	_ "github.com/elastic/beats/packetbeat/protos/pgsql"
	_ "github.com/elastic/beats/packetbeat/protos/redis"
	_ "github.com/elastic/beats/packetbeat/protos/thrift"
	"io/ioutil"
	"net/http"
	"github.com/siye1982/eagleye-health/registry"
	"digger/utils"

)

var Name = "packetbeat"
var etcdHosts string

func initEtcd() {

	var cfgFile string

	//flag.Parse()
	cfgFile = flag.Lookup("c").Value.String()
	//fmt.Println("^^^^^^^^^^^^^^^^^^^^^^^^^^^^")
	//fmt.Println(cfgFile)
	config.EtcdHosts = etcdHosts
	config.GroupName = "packet"
	cfgFileContent, _ := getFileContentAsStringLines(cfgFile)
	//fmt.Println(cfgFileContent)
	config.HeartbeatConfig = cfgFileContent
	config.InitEtcdClient()
	registry.Start()
}

func getFileContentAsStringLines(filePath string) (string, error) {
	fmt.Println("get file content as lines: %v", filePath)
	result := "error"
	b, err := ioutil.ReadFile(filePath)
	if err != nil {
		fmt.Println("read file: %v error: %v", filePath, err)
		return result, err
	}
	result = string(b)
	return result, nil
}

// Setups and Runs Packetbeat
func main() {
	// etcd 地址
	flag.StringVar(&etcdHosts, "etcd", "", "etcd Address")

	// 监控程序http地址, 如果为空则不开启监控程序
	var pprofAddress string = "localhost:7080"
	flag.StringVar(&pprofAddress, "pprofAddress", "", "pprof http address")
	flag.Parse()

	fmt.Println("===== digger enabled: ", utils.Bootup , " ===== digger log level: ", utils.LogLevel)

	fmt.Println("===== pprof address : [", pprofAddress, "] =====")

	if pprofAddress != "" {
		go func() {
			http.ListenAndServe(pprofAddress, nil)
		}()
	}

	//如果etcd地址为空, 则不开启健康心跳收集
	if etcdHosts != "" {
		go func() {
			initEtcd()
		}()
	}

	if err := beat.Run(Name, "", beater.New()); err != nil {
		os.Exit(1)
	}
}
