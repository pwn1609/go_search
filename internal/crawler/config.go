package crawler

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

type Config struct {
	Kafka   KafkaConfig   `yaml:"kafka"`
	Crawler CrawlerConfig `yaml:"crawler"`
}

type KafkaConfig struct {
	Brokers []string `yaml:"brokers"`
	Topic   string   `yaml:"topic"`
}

type CrawlerConfig struct {
	SeedURL         string `yaml:"seed"`
	MaxWorkers      int    `yaml:"maxWorkers"`
	MaxPagesPerHost int    `yaml:"maxPagesPerHost"`
}

// Load reads and parses the YAML config file, then validates required fields.
func LoadConfig(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config file %q: %w", path, err)
	}

	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("parse config file %q: %w", path, err)
	}

	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("validate config file %q: %w", path, err)
	}

	return &cfg, nil
}

func (c *Config) Validate() error {
	if len(c.Kafka.Brokers) == 0 {
		return fmt.Errorf("kafka.brokers is required and must contain at least one broker")
	}

	for i, broker := range c.Kafka.Brokers {
		if broker == "" {
			return fmt.Errorf("kafka.brokers[%d] cannot be empty", i)
		}
	}

	if c.Kafka.Topic == "" {
		return fmt.Errorf("kafka.topic is required")
	}

	if c.Crawler.SeedURL == "" {
		return fmt.Errorf("crawler.seed is required")
	}

	if c.Crawler.MaxWorkers <= 0 {
		c.Crawler.MaxWorkers = 5
	}

	if c.Crawler.MaxPagesPerHost <= 0 {
		c.Crawler.MaxPagesPerHost = 500
	}

	return nil
}
