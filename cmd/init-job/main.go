package main

import (
	"fmt"
	"log"
	"os"

	"github.com/pwn1609/GoSearch/internal/crawler"
	"gopkg.in/yaml.v3"
)

type initConfig struct {
	Kafka struct {
		Brokers    []string `yaml:"brokers"`
		HostsTopic string   `yaml:"hostsTopic"`
	} `yaml:"kafka"`
	Seed string `yaml:"seed"`
}

func main() {
	data, err := os.ReadFile("config.yaml")
	if err != nil {
		log.Fatalf("read config: %v", err)
	}

	var cfg initConfig
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		log.Fatalf("parse config: %v", err)
	}

	if len(cfg.Kafka.Brokers) == 0 {
		log.Fatal("kafka.brokers is required")
	}
	if cfg.Kafka.HostsTopic == "" {
		log.Fatal("kafka.hostsTopic is required")
	}
	if cfg.Seed == "" {
		log.Fatal("seed is required")
	}

	producer := crawler.NewKafkaProducer(cfg.Kafka.Brokers, cfg.Kafka.HostsTopic)
	defer producer.Close()

	ok := producer.SendMessage(crawler.Message{Key: cfg.Seed, Value: cfg.Seed})
	if !ok {
		log.Fatalf("failed to publish seed host %q to %s", cfg.Seed, cfg.Kafka.HostsTopic)
	}

	fmt.Printf("Seeded %q into %s\n", cfg.Seed, cfg.Kafka.HostsTopic)
}
