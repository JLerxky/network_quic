# network_quic

`CITA-Cloud`中[network微服务](https://github.com/cita-cloud/cita_cloud_proto/blob/master/protos/network.proto)的实现，基于[tokio-rustls](https://crates.io/crates/tokio-rustls)。

## 编译docker镜像
```
docker build -t citacloud/network_tls .
```

## 使用方法

```
$ network -h
network
Network service for CITA-Cloud

USAGE:
    network [SUBCOMMAND]

OPTIONS:
    -h, --help    Print help information

SUBCOMMANDS:
    gen-config    generate TEST-ONLY network config
    help          Print this message or the help of the given subcommand(s)
    run           run network service
```

### network-gen-config

生成测试用的网络配置文件

```
$ network gen-config -h
network-gen-config
generate TEST-ONLY network config

USAGE:
    network gen-config [peer-count]

ARGS:
    <peer-count>    how many peers to generate [default: 2]

OPTIONS:
    -h, --help    Print help information
```

生成两个节点的配置文件：
```
$ network gen-config 2
Done.
WARNING: This config is for TEST-ONLY.
$ ls
ca_key.pem  peer0.toml  peer1.toml
```

### network-run

运行`network`服务。

```
$ network run -h
network-run
run network service

USAGE:
    network run [OPTIONS]

OPTIONS:
    -c, --config <config>
            the network config [default: config.toml]

    -d, --log-dir <log-dir>
            the log dir

    -f, --log-file-name <log-file-name>
            the log file name

    -h, --help
            Print help information

    -l, --log-level <log-level>
            the log level [default: info] [possible values: error, warn, info, debug, trace]

        --stdout
            if specified, log to stdout
```

参数：
1. `config` 微服务配置文件。

    参见示例`example/config.toml`。

    其中：
    * `ca_cert` 为`CA`根证书。
    * `cert` 为节点证书。
    * `priv_key` 为节点证书对应的私钥。
    * `grpc_port` 为`gRPC`服务监听的端口号。
    * `listen_port` 为节点网络的监听端口。
    * `peers` 为邻居节点的网络信息，其中`host`字段为`ip`或者域名，`port`字段为端口号，`domain`字段为该邻居节点申请证书时使用的域名。
    * `reconnect_timeout` 当无法网络连接到某个邻居节点时，尝试重连的超时时间。
    * `try_hot_update_interval` 为配置文件热更新功能，扫描间隔，单位为秒。
2. `log-dir` 日志的输出目录。
3. `log-file-name` 日志输出的文件名。
4. `log-level` 为日志等级。可选项有：`Error`，`Warn`，`Info`，`Debug`，`Trace`，默认为`Info`。
5. `--stdout` 不传该参数时，日志输出到文件；传递该参数时，日志输出到标准输出。

输出到日志文件：
```
$ network run -c peer0.toml -d . -f peer0
$ cat peer0.2022-03-11
Mar 11 10:53:55.250  INFO network::server: listen on `0.0.0.0:40000`
Mar 11 10:53:55.250  INFO network::server: monitoring config file: (peer0.toml)
Mar 11 10:53:55.250  INFO network::server: config file initial md5: b89c95ec84b09b4e1abeaa367b729669
Mar 11 10:53:55.251  INFO network::peer: connecting.. peer=peer1.fy host=localhost port=40001
```

输出到标注输出：
```
$ network run -c peer0.toml --stdout
Mar 11 10:58:10.133  INFO network::server: listen on `0.0.0.0:40000`
Mar 11 10:58:10.133  INFO network::server: monitoring config file: (peer0.toml)
Mar 11 10:58:10.134  INFO network::peer: connecting.. peer=peer1.fy host=localhost port=40001
Mar 11 10:58:10.139  INFO network::server: config file initial md5: b89c95ec84b09b4e1abeaa367b729669
```

## 设计


