# gohangout-input-kafkago
此包为 https://github.com/childe/gohangout 项目的 kafka inputs 插件。

# 特点
使用[kafka-go](https://github.com/segmentio/kafka-go) 这个仓库来作为input
### TODO
TLS配置项的支持

### DONE
SASL已经支持

# 使用方法

将 `gokafka_input.go` 复制到 `gohangout` 主目录下面, 运行

```bash
go build -buildmode=plugin -o gokafka_input.so gokafka_input.go
```

将 `gokafka_input.so` 路径作为 inputs

## gohangout 配置示例
所有参数字段名字都使用kafka-go原生的，所以和gohangout的kafka插件的配置名字有些不一样。主要是为了偷懒.   
新增加一个配置项：**StatsAddr** 用来提供一个http端口，统计一些消费的信息

```yaml
inputs:
  - '/usr/local/services/waf-attack-cls-go-1.0/bin/gokafka_input.so':
      Brokers:
        - '10.1.1.1:9092'
      GroupID: 'test.abc'
      Topic: 'con_attack_log'
      StatsAddr: 'localhost:11456'
      SASL:
        Type: 'Plain'
        Username: 'huangjacky'
        Password: 'test'
outputs:
  - Kafka:
      topic: 'test'
      producer_settings:
        bootstrap.servers: '9.1.1.1:9092'
        acks: "1"
```
查看消费详情
```bash
curl localhost:11456
```
获得的信息
```json
{"Dials":0,"Fetches":0,"Messages":2357,"Bytes":3366716,"Rebalances":0,"Timeouts":0,"Errors":0,"DialTime":{"Avg":0,"Min":0,"Max":0},"ReadTime":{"Avg":0,"Min":0,"Max":0},"WaitTime":{"Avg":0,"Min":0,"Max":0},"FetchSize":{"Avg":0,"Min":0,"Max":0},"FetchBytes":{"Avg":0,"Min":0,"Max":0},"Offset":475595146,"Lag":847232,"MinBytes":10000,"MaxBytes":10000000,"MaxWait":10000000000,"QueueLength":100,"QueueCapacity":100,"ClientID":"","Topic":"con_attack_log","Partition":"-1","DeprecatedFetchesWithTypo":0}
```
