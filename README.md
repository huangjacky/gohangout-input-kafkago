# gohangout-input-kafkago
此包为 https://github.com/childe/gohangout 项目的 kafka inputs 插件。

# 特点
使用[kafka-go](https://github.com/segmentio/kafka-go) 这个仓库来作为input
还有很多参数没有暴露出来.

# 使用方法

将 `gokafka_input.go` 复制到 `gohangout` 主目录下面, 运行

```bash
go build -buildmode=plugin -o gokafka_input.so gokafka_input.go
```

将 `gokafka_input.so` 路径作为 inputs

## gohangout 配置示例
所有参数字段名字都使用kafka-go原生的，所以和gohangout的kafka插件的配置名字有些不一样。主要是为了偷懒

```yaml
inputs:
  - '/usr/local/services/waf-attack-cls-go-1.0/bin/gokafka_input.so':
      Brokers:
        - '100.1.1.1:9092'
      GroupID: 'test.abc'
      Topic: 'attack'
outputs:
  - Kafka:
      topic: 'test'
      producer_settings:
          bootstrap.servers: '127.0.0.1:9092'
          acks: "1"
```
