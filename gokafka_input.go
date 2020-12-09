package main

import (
	"context"
	"crypto/tls"
	js "encoding/json"
	"net/http"
	"time"

	"github.com/childe/gohangout/codec"
	"github.com/golang/glog"
	kafka_go "github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl"
	"github.com/segmentio/kafka-go/sasl/plain"
	"github.com/segmentio/kafka-go/sasl/scram"
)

// GoKafkaInput 使用的Kafka-go的input插件
type GoKafkaInput struct {
	config         map[interface{}]interface{}
	decorateEvents bool
	messages       chan *kafka_go.Message
	decoder        codec.Decoder
	reader         *kafka_go.Reader
	readConfig     *kafka_go.ReaderConfig
}

/*
HTTPKafka 增加一个状态获取的接口
*/
type HTTPKafka struct {
	kafka *GoKafkaInput
}

/**
返回reader的status接口的数据
*/
func (h *HTTPKafka) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	writer.Header().Set("Content-Type", "application/json")
	writer.WriteHeader(http.StatusOK)
	if h.kafka.reader != nil {
		stats := h.kafka.reader.Stats()
		if data, err := js.Marshal(stats); err == nil {
			_, _ = writer.Write(data)
		} else {
			http.Error(writer, err.Error(), http.StatusInternalServerError)
		}
		return
	}
	_, _ = writer.Write([]byte(`{}`))
}

/**
格式转换
*/
func (p *GoKafkaInput) getConsumerConfig(config map[interface{}]interface{}) (*kafka_go.ReaderConfig, error) {
	c := &kafka_go.ReaderConfig{
		Brokers: make([]string, 1),
	}
	if v, ok := config["Brokers"]; ok {
		for _, vv := range v.([]interface{}) {
			c.Brokers = append(c.Brokers, vv.(string))
		}
	} else {
		glog.Fatal("Brokers must be set")
	}
	if v, ok := config["GroupID"]; ok {
		c.GroupID = v.(string)
	} else {
		glog.Fatal("GroupID must be set")
	}
	if v, ok := config["Topic"]; ok {
		c.Topic = v.(string)
	} else {
		glog.Fatal("Topic must be set")
	}
	if v, ok := config["MinBytes"]; ok {
		c.MinBytes = v.(int)
	} else {
		c.MinBytes = 10e3
	}
	if v, ok := config["MaxBytes"]; ok {
		c.MaxBytes = v.(int)
	} else {
		c.MaxBytes = 10e6
	}
	if v, ok := config["HeartbeatInterval"]; ok {
		c.HeartbeatInterval = time.Duration(v.(int)) * time.Second
	}
	if v, ok := config["CommitInterval"]; ok {
		c.CommitInterval = time.Duration(v.(int)) * time.Second
	}
	if v, ok := config["MaxWait"]; ok {
		c.MaxWait = time.Duration(v.(int)) * time.Second
	}
	if v, ok := config["SessionTimeout"]; ok {
		c.SessionTimeout = time.Duration(v.(int)) * time.Second
	}
	if v, ok := config["RebalanceTimeout"]; ok {
		c.RebalanceTimeout = time.Duration(v.(int)) * time.Second
	}
	var dialer *kafka_go.Dialer
	if v, ok := config["Timeout"]; ok {
		i := v.(int)
		dialer = &kafka_go.Dialer{
			Timeout:   time.Duration(i) * time.Second,
			DualStack: true,
		}
	} else {
		dialer = &kafka_go.Dialer{
			Timeout:   10 * time.Second,
			DualStack: true,
		}
	}
	if v, ok := config["SASL"]; ok {
		vh := v.(map[string]interface{})
		v1, ok1 := vh["Type"]
		v2, ok2 := vh["Username"]
		v3, ok3 := vh["Password"]
		if ok1 && ok2 && ok3 {
			var (
				mechanism sasl.Mechanism
				err       error
			)
			switch v1.(string) {
			case "Plain":
				mechanism = plain.Mechanism{
					Username: v2.(string),
					Password: v3.(string),
				}
			case "SCRAM":
				mechanism, err = scram.Mechanism(
					scram.SHA512,
					v2.(string),
					v3.(string),
				)
				if err != nil {
					glog.Fatal("ERROR FOR SCRAM: ", err)
				}
			default:
				glog.Fatalf("ERROR SASL type: %s", v1.(string))
			}
			dialer.SASLMechanism = mechanism
		} else {
			glog.Fatal("NO CONFIG FOR SASL")
		}
	}
	//TODO 后面再看是否需要完成了，理论上内网的KAFKA不会开启TLS，毕竟这个耗性能
	if v, ok := config["TLS"]; ok {
		vh := v.(map[string]interface{})
		var tls *tls.Config
		if _, ok1 := vh["PrivateKey"]; ok1 {
			glog.Info("TODO")
		}
		dialer.TLS = tls
	}
	c.Dialer = dialer
	if err := c.Validate(); err != nil {
		glog.Fatal("ReadConfig Validate error: ", err)
		return nil, err
	}
	return c, nil
}

/*
New 插件模式的初始化
*/
func New(config map[interface{}]interface{}) interface{} {
	p := &GoKafkaInput{
		messages:       make(chan *kafka_go.Message, 10),
		decorateEvents: false,
		reader:         nil,
	}
	if v, ok := config["decorateEvents"]; ok {
		p.decorateEvents = v.(bool)
	}
	var codertype = "plain"
	if v, ok := config["code"]; ok {
		codertype = v.(string)
	}
	p.decoder = codec.NewDecoder(codertype)
	// 起携程，将所有收到的消息，存放到现在这个队列里面
	var err error

	if p.readConfig, err = p.getConsumerConfig(config); err == nil {
		p.reader = kafka_go.NewReader(*p.readConfig)
	} else {
		glog.Fatal("consumer_settings wrong")
	}

	if listen, ok := config["StatsAddr"]; ok {
		httpAddr := listen.(string)
		HTTPKafka := &HTTPKafka{
			kafka: p,
		}
		go func() {
			glog.Info("Start Http Server: ", httpAddr)
			_ = http.ListenAndServe(httpAddr, HTTPKafka)
		}()

	}
	go func() {
		for {
			m, err := p.reader.ReadMessage(context.Background())
			if err != nil {
				glog.Error("ReadMessage Error: ", err)
				break
			}
			//TODO 这里是不是要做一些异常检查
			p.messages <- &m
		}
	}()
	return p
}

//ReadOneEvent 单次事件的处理函数
func (p *GoKafkaInput) ReadOneEvent() map[string]interface{} {
	message, ok := <-p.messages
	if ok {
		event := p.decoder.Decode(message.Value)
		if p.decorateEvents {
			kafkaMeta := make(map[string]interface{})
			kafkaMeta["topic"] = message.Topic
			kafkaMeta["length"] = len(message.Value)
			kafkaMeta["partition"] = message.Partition
			kafkaMeta["offset"] = message.Offset
			event["@metadata"] = map[string]interface{}{"kafka": kafkaMeta}
		}
		return event
	}
	return nil
}

//Shutdown 关闭需要做的事情
func (p *GoKafkaInput) Shutdown() {
	if err := p.reader.Close(); err != nil {
		glog.Fatal("failed to close reader:", err)
	}
}
