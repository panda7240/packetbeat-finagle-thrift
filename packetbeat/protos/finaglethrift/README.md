
# Packetbeat finaglethrift

配置文件说明:

    输出到kafka (更多配置细节见:https://www.elastic.co/guide/en/beats/filebeat/master/kafka-output.html)


## 安装GO
在 [这里](https://golang.org/dl/) 获取对应的操作系统的GO安装bao

## GOPATH
* 安装好Go后需要设置环境变量,如下:

	```
	#这是Go的安装路径
	export GOROOT=/usr/local/go
 	export GOBIN=$GOROOT/bin

 	#这里可以理解为Go项目的工作空间, 这里允许有多个目录,注意用":"分割
 	#当有多个GOPATH时,执行 go get命令的内容默认会放在第一个目录下
 	export GOPATH=/work/goworkspace
	```

* GOPATH的的几个目录约定
	* src 放置Go项目的源码
	* pkg Go项目中使用的第三方包
	* bin 编译后生成的可执行文件, 可以把此目录加入到 PATH 变量中


## 获取并编译

   * 获取编译


```
 #创建相应目录
 mkdir -p $GOPATH/src/github.com/elastic/
 cd $GOPATH/src/github.com/elastic

 #签出源码
 git clone https://github.com/thanq/beats.git

 git checkout zhexxxbeat

 #切换到packetbeat模块
 cd beats/packetbeat

 #获取依赖信息
 (mkdir -p $GOPATH/src/golang.org/x/&&cd $GOPATH/src/golang.org/x &&git clone  https://github.com/golang/tools.git )
 go get github.com/tools/godep

 # 安装依赖环境
 sudo yum install gcc libpcap-devel

 # 当出现 gcc: unrecognized option '-no-pie', 请升级gcc版本大于4.5
 # 具体升级gcc, 请参考http://www.wengweitao.com/centos-sheng-ji-gcc-he-g-de-fang-fa.html

 # 添加eagleye-health组件
 mkdir -p $GOPATH/src/github.com/siye1982/ && cd $GOPATH/src/github.com/siye1982 && git clone https://github.com/siye1982/eagleye-health.git

 go get github.com/siye1982/eagleye-health

# 添加etcd组件
 mkdir -p $GOPATH/src/github.com/coreos && cd $GOPATH/src/github.com/coreos && git clone https://github.com/coreos/etcd.git

 go get github.com/coreos/etcd

 # 添加net组件
 mkdir -p $GOPATH/src/golang.org/x/ && cd $GOPATH/src/golang.org/x && git clone  https://github.com/golang/net.git

 go get golang.org/x/net/context


 # 添加digger组件
 cd $GOPATH/src && git clone git@git.xxx-inc.com:ruby/bj-ruby-digger.git digger


 #编译
 cd $GOPATH/src/github.com/elastic/beats/packetbeat
 make

 # 如果想通过pprof进行监控, 需要访问: http://localhost:7080/debug/pprof/

```

   * 文件目录说明


```
  |__ config  放置配置文件的地方, 其中包括thrift使用的idl文件集合, packetbeat使用的配置文件
     |__thrift 该目录下面方式所有thrift使用的idl配置文件
     |__ packetbeat.yml  packetbeat配置文件
  |__ packetbeat go编译出来的可执行文件, 其中linux和os x 系统下的不通用
  |__ run.sh 启动脚本
  |__ stop.sh 停止脚本

  将这些样本统一放在了源代码, packetbeat-finagle目录下面, 并针对每一项配置有详细的说明, 其中packetbeat的可执行文件暂时放置为linux平台中的可执行文件
  在linux环境下, 可以进入到 192.168.5.191:/home/eagleye/gopath/src/github.com/elastic/beats/packetbeat目录下执行
  git pull && make 编译出linux版本的packetbeat

```





## 抓取finagle数据包的字段说明

   ```
      {
        "@timestamp": "2016-05-05T02:55:23.336Z", //数据产生时间点,精确到毫秒
        "beat": {
          "hostname": "localhost",
          "name": "localhost"
        },
        "bytes_in": 61,                   // 返回结果数据大小
        "bytes_out": 80,                  // 请求数据大小
        "client_ip": "192.168.199.210",   // 消费者ip
        "client_port": 52382,             // 消费者随机端口
        "client_proc": "",
        "client_server": "",
        "consumer_app": "",               // 消费者所属应用app名称
        "consumer_service": "",           // 消费者所在服务名称
        "direction": "out",               // 请求方
        "ip": "192.168.5.191",            // 提供者ip
        "method": "detail",               // 请求的方法名
        "path": "OrderServ",              // finagle的idl类名
        "port": 20880,                    // 提供者端口
        "proc": "",
        "provider_app": "",               // 提供者所属应用app名称
        "provider_service": "",           // 提供者的服务名
        "query": "detail(userId: 1, userName: \"userName[1]\", orderId: \"orderId[1]\")",    // 请求内容
        "responsetime": 3,                // 响应时长,  responsetime如果为0, 并不代表代表会超时
        "server": "",
        "status": "OK",                   // 请求正常, 其中有 OK, Error, Client Error, Server Error
        "thrift": {
          // 针对异常信息, finaglethrift在TApplicationException中, 具体异常编号定义如下:
          //public static final int UNKNOWN = 0;
          //public static final int UNKNOWN_METHOD = 1;
          //public static final int INVALID_MESSAGE_TYPE = 2;
          //public static final int WRONG_METHOD_NAME = 3;
          //public static final int BAD_SEQUENCE_ID = 4;
          //public static final int MISSING_RESULT = 5;
          //public static final int INTERNAL_ERROR = 6;
          //public static final int PROTOCOL_ERROR = 7;
          "exceptions": "(1: \"Internal error processing detail: 'java.lang.RuntimeException: Loom调用抛出ReflectiveOperationException'\", 2: 6)",  // 业务抛出的运行时异常
          "params": "(userId: 1, userName: \"userName[1]\", orderId: \"orderId[1]\")",       // 请求参数, 默认截取200字符长
          "return_value": "(1: (1: 0), 2: (1: 1, 2: \"userName[1]\", 3: \"orderId[1]\"))",   // 返回值, 默认截取200字符长
          "service": "OrderServ",         // finagle的idl类名
          "status": "success"             // finagle自身的返回状态, 其中有, success, exception, timeout, 当result_value中有自定义的Result,并且其中该返回值的结构体中第一个子结构值为1,则认为是业务异常.
        },
        "type": "finaglethrift"           // 协议类型
      }
   ```


## 测试用例

### 原生Thrift

   * 消费者和生产者在同一台服务器上, 查看原生thrift是否可以同时抓取到in, out的数据


   ```
   {
     "@timestamp": "2016-05-05T02:23:27.298Z",
     "beat": {
       "hostname": "localhost",
       "name": "localhost"
     },
     "bytes_in": 61,
     "bytes_out": 80,
     "client_ip": "127.0.0.1",
     "client_port": 51988,
     "client_proc": "",
     "client_server": "localhost",
     "ip": "127.0.0.1",
     "method": "detail",
     "path": "OrderServ",
     "port": 20880,
     "proc": "",
     "query": "detail(userId: 1, userName: \"userName[1]\", orderId: \"orderId[1]\")",
     "responsetime": 50,
     "server": "localhost",
     "status": "OK",
     "thrift": {
       "params": "(userId: 1, userName: \"userName[1]\", orderId: \"orderId[1]\")",
       "return_value": "(1: (1: 0), 2: (1: 1, 2: \"userName[1]\", 3: \"orderId[1]\"))",
       "service": "OrderServ"
     },
     "type": "thrift"
   }
   ```

   * 消费者和生产者不在同一台服务器上, 抓取消费者的tcp包数据

   ```
   {
     "@timestamp": "2016-05-05T02:29:11.046Z",
     "beat": {
       "hostname": "localhost",
       "name": "localhost"
     },
     "bytes_in": 61,
     "bytes_out": 80,
     "client_ip": "192.168.199.210",
     "client_port": 52027,
     "client_proc": "",
     "client_server": "",
     "direction": "out",
     "ip": "192.168.10.235",
     "method": "detail",
     "path": "OrderServ",
     "port": 20880,
     "proc": "",
     "query": "detail(userId: 1, userName: \"userName[1]\", orderId: \"orderId[1]\")",
     "responsetime": 1,
     "server": "",
     "status": "OK",
     "thrift": {
       "params": "(userId: 1, userName: \"userName[1]\", orderId: \"orderId[1]\")",
       "return_value": "(1: (1: 0), 2: (1: 1, 2: \"userName[1]\", 3: \"orderId[1]\"))",
       "service": "OrderServ"
     },
     "type": "thrift"
   }
   ```


### Finagle-thrift

   * 生产者和消费者在同一台服务器上, 查看一次请求抓取到的数据包

   ```
   {
     "@timestamp": "2016-05-05T02:50:00.644Z",
     "beat": {
       "hostname": "localhost",
       "name": "localhost"
     },
     "bytes_in": 61,
     "bytes_out": 80,
     "client_ip": "127.0.0.1",
     "client_port": 52323,
     "client_proc": "",
     "client_server": "localhost",
     "consumer_app": "",
     "consumer_service": "",
     "ip": "127.0.0.1",
     "method": "detail",
     "path": "OrderServ",
     "port": 20880,
     "proc": "",
     "provider_app": "",
     "provider_service": "",
     "query": "detail(userId: 1, userName: \"userName[1]\", orderId: \"orderId[1]\")",
     "responsetime": 4,
     "server": "localhost",
     "status": "OK",
     "thrift": {
       "params": "(userId: 1, userName: \"userName[1]\", orderId: \"orderId[1]\")",
       "return_value": "(1: (1: 0), 2: (1: 1, 2: \"userName[1]\", 3: \"orderId[1]\"))",
       "service": "OrderServ"
     },
     "type": "finaglethrift"
   }

   ```

   * 生产者和消费者不在同一台服务器上, 抓取消费者端的数据包

   ```
   {
     "@timestamp": "2016-05-05T02:55:23.336Z",
     "beat": {
       "hostname": "localhost",
       "name": "localhost"
     },
     "bytes_in": 61,
     "bytes_out": 80,
     "client_ip": "192.168.199.210",
     "client_port": 52382,
     "client_proc": "",
     "client_server": "",
     "consumer_app": "",
     "consumer_service": "",
     "direction": "out",
     "ip": "192.168.5.191",
     "method": "detail",
     "path": "OrderServ",
     "port": 20880,
     "proc": "",
     "provider_app": "",
     "provider_service": "",
     "query": "detail(userId: 1, userName: \"userName[1]\", orderId: \"orderId[1]\")",
     "responsetime": 3,
     "server": "",
     "status": "OK",
     "thrift": {
       "params": "(userId: 1, userName: \"userName[1]\", orderId: \"orderId[1]\")",
       "return_value": "(1: (1: 0), 2: (1: 1, 2: \"userName[1]\", 3: \"orderId[1]\"))",
       "service": "OrderServ"
     },
     "type": "finaglethrift"
   }
   ```

   * 服务提供者抛出业务异常, 消费者端抓取的数据包

       * 消费者数据包

        ```
        {
          "@timestamp": "2016-05-05T03:25:34.180Z",
          "beat": {
            "hostname": "5dian192",
            "name": "5dian192"
          },
          "bytes_in": 61,
          "bytes_out": 141,
          "client_ip": "192.168.5.192",
          "client_port": 43834,
          "client_proc": "",
          "client_server": "",
          "consumer_app": "",
          "consumer_service": "",
          "direction": "out",
          "ip": "192.168.5.191",
          "method": "detail",
          "path": "OrderServ",
          "port": 20880,
          "proc": "",
          "provider_app": "",
          "provider_service": "",
          "query": "detail(userId: 1, userName: \"userName[1]\", orderId: \"orderId[1]\")",
          "responsetime": 39,
          "server": "",
          "status": "Error",
          "thrift": {
            "exceptions": "(1: \"Internal error processing detail: 'java.lang.RuntimeException: Loom调用抛出ReflectiveOperationException'\", 2: 6)",
            "params": "(userId: 1, userName: \"userName[1]\", orderId: \"orderId[1]\")",
            "return_value": "",
            "service": "OrderServ"
          },
          "type": "finaglethrift"
        }
        ```

        * 提供者数据包

        ```
        {
          "@timestamp": "2016-05-05T11:41:25.462Z",
          "beat": {
            "hostname": "5dian191",
            "name": "5dian191"
          },
          "bytes_in": 61,
          "bytes_out": 141,
          "client_ip": "192.168.5.192",
          "client_port": 43834,
          "client_proc": "",
          "client_server": "",
          "consumer_app": "",
          "consumer_service": "",
          "direction": "in",
          "ip": "192.168.5.191",
          "method": "detail",
          "path": "OrderServ",
          "port": 20880,
          "proc": "",
          "provider_app": "",
          "provider_service": "",
          "query": "detail(userId: 1, userName: \"userName[1]\", orderId: \"orderId[1]\")",
          "responsetime": 39,
          "server": "",
          "status": "Error",
          "thrift": {
            "exceptions": "(1: \"Internal error processing detail: 'java.lang.RuntimeException: Loom调用抛出ReflectiveOperationException'\", 2: 6)",
            "params": "(userId: 1, userName: \"userName[1]\", orderId: \"orderId[1]\")",
            "return_value": "",
            "service": "OrderServ"
          },
          "type": "finaglethrift"
        }
        ```


    * 消费者和提供者之间网络异常, 消费者端抓取数据包
        应用快速失败

    * 消费者消费服务超时, 消费者端, 提供者端抓取数据包

        * 消费者数据包

        ```
        {
          "@timestamp": "2016-05-05T03:20:33.901Z",
          "beat": {
            "hostname": "5dian192",
            "name": "5dian192"
          },
          "bytes_in": 61,
          "bytes_out": 0,
          "client_ip": "192.168.5.192",
          "client_port": 43831,
          "client_proc": "",
          "client_server": "",
          "consumer_app": "",
          "consumer_service": "",
          "direction": "out",
          "ip": "192.168.5.191",
          "method": "detail",
          "path": "OrderServ",
          "port": 20880,
          "proc": "",
          "provider_app": "",
          "provider_service": "",
          "query": "detail(userId: 1, userName: \"userName[1]\", orderId: \"orderId[1]\")",
          "responsetime": 0,
          "server": "",
          "status": "OK",
          "thrift": {
            "params": "(userId: 1, userName: \"userName[1]\", orderId: \"orderId[1]\")",
            "service": "OrderServ"
          },
          "type": "finaglethrift"
        }
        ```

        * 生产者数据包

        ```
        {
          "@timestamp": "2016-05-05T11:36:25.188Z",
          "beat": {
            "hostname": "5dian191",
            "name": "5dian191"
          },
          "bytes_in": 61,
          "bytes_out": 0,
          "client_ip": "192.168.5.192",
          "client_port": 43831,
          "client_proc": "",
          "client_server": "",
          "consumer_app": "",
          "consumer_service": "",
          "direction": "in",
          "ip": "192.168.5.191",
          "method": "detail",
          "path": "OrderServ",
          "port": 20880,
          "proc": "",
          "provider_app": "",
          "provider_service": "",
          "query": "detail(userId: 1, userName: \"userName[1]\", orderId: \"orderId[1]\")",
          "responsetime": 0,
          "server": "",
          "status": "OK",
          "thrift": {
            "params": "(userId: 1, userName: \"userName[1]\", orderId: \"orderId[1]\")",
            "service": "OrderServ"
          },
          "type": "finaglethrift"
        }
        ```


