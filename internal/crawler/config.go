package crawler

import (
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

type Config struct {
	Kafka   KafkaConfig   `yaml:"kafka"`
	Crawler CrawlerConfig `yaml:"crawler"`
	Redis   RedisConfig   `yaml:"redis"`
	Filter  FilterConfig  `yaml:"filter"`
}

type FilterConfig struct {
	BlockedKeywords []string `yaml:"blockedKeywords"`
	BlockedDomains  []string `yaml:"blockedDomains"`
}

type KafkaConfig struct {
	Brokers    []string `yaml:"brokers"`
	PagesTopic string   `yaml:"pagesTopic"`
	HostsTopic string   `yaml:"hostsTopic"`
}

type RedisConfig struct {
	Addr     string        `yaml:"addr"`
	ClaimTTL time.Duration `yaml:"claimTTL"`
}

type CrawlerConfig struct {
	MaxWorkers      int `yaml:"maxWorkers"`
	MaxPagesPerHost int `yaml:"maxPagesPerHost"`
	MaxBodyBytes    int `yaml:"maxBodyBytes"`
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

	if c.Kafka.PagesTopic == "" {
		return fmt.Errorf("kafka.pagesTopic is required")
	}

	if c.Kafka.HostsTopic == "" {
		return fmt.Errorf("kafka.hostsTopic is required")
	}

	if c.Redis.Addr == "" {
		return fmt.Errorf("redis.addr is required")
	}

	if c.Redis.ClaimTTL <= 0 {
		c.Redis.ClaimTTL = 24 * time.Hour
	}

	if c.Crawler.MaxWorkers <= 0 {
		c.Crawler.MaxWorkers = 5
	}

	if c.Crawler.MaxPagesPerHost <= 0 {
		c.Crawler.MaxPagesPerHost = 500
	}

	if c.Crawler.MaxBodyBytes <= 0 {
		c.Crawler.MaxBodyBytes = 512 * 1024 // 512KB
	}

	return nil
}
