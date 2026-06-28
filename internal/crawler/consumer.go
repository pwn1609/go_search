package crawler

import (
	"context"

	"github.com/segmentio/kafka-go"
)

type KafkaConsumer struct {
	reader *kafka.Reader
}

func NewKafkaConsumer(brokers []string, topic, groupID string) *KafkaConsumer {
	return &KafkaConsumer{
		reader: kafka.NewReader(kafka.ReaderConfig{
			Brokers: brokers,
			Topic:   topic,
			GroupID: groupID,
		}),
	}
}

// ReadHost blocks until a host message is available, then returns the host string.
func (c *KafkaConsumer) ReadHost(ctx context.Context) (string, error) {
	msg, err := c.reader.ReadMessage(ctx)
	if err != nil {
		return "", err
	}
	return string(msg.Key), nil
}

func (c *KafkaConsumer) Close() error {
	return c.reader.Close()
}
