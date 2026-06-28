package crawler

import (
	"context"
	"log"
	"time"

	"github.com/segmentio/kafka-go"
)

type KafkaProducer struct {
	BootstrapAddr string
	Topic         string
	writer        *kafka.Writer
}

type Message struct {
	Key   string
	Value string
}

func NewKafkaProducer(brokers []string, topic string) *KafkaProducer {
	newProducer := KafkaProducer{
		BootstrapAddr: brokers[0],
		Topic:         topic,
	}

	newProducer.writer = &kafka.Writer{
		Addr:         kafka.TCP(brokers...),
		Topic:        topic,
		Balancer:     &kafka.LeastBytes{},
		RequiredAcks: kafka.RequireOne,
		Async:        false,
	}

	return &newProducer
}

func (p *KafkaProducer) SendMessage(mess Message) bool {
	msg := Message{
		Key:   mess.Key,
		Value: mess.Value,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	err := p.writer.WriteMessages(ctx, kafka.Message{
		Key:   []byte(msg.Key),
		Value: []byte(msg.Value),
		Time:  time.Now(),
	})

	if err != nil {
		log.Printf("failed to send kafka message for url %s: %v", mess.Key, err)
		return false
	}

	return true
}

func (p *KafkaProducer) Close() error {
	if p.writer == nil {
		return nil
	}
	return p.writer.Close()
}
