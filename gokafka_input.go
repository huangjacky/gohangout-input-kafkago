package main

import (
	"context"
	js "encoding/json"
	"github.com/childe/gohangout/codec"
	"github.com/golang/glog"
	kafka_go "github.com/segmentio/kafka-go"
	"net/http"
	"time"
)

type GoKafkaInput struct {
	config         map[interface{}]interface{}
	decorateEvents bool
	messages       chan *kafka_go.Message
	decoder        codec.Decoder
	reader         *kafka_go.Reader
	readConfig     *kafka_go.ReaderConfig
}

/**
增加一个状态获取的接口
*/
type HttpKafka struct {
	kafka *GoKafkaInput
}

/**
返回reader的status接口的数据
*/
func (h *HttpKafka) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
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
	if err := c.Validate(); err != nil {
		glog.Fatal("ReadConfig Validate error: ", err)
		return nil, err
	}
	return c, nil
}

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
		httpKafka := &HttpKafka{
			kafka: p,
		}
		go func() {
			glog.Info("Start Http Server: ", httpAddr)
			_ = http.ListenAndServe(httpAddr, httpKafka)
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
func (p *GoKafkaInput) Shutdown() {
	if err := p.reader.Close(); err != nil {
		glog.Fatal("failed to close reader:", err)
	}
}
