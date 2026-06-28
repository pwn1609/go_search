package crawler

import (
	"context"
	"fmt"
	"io"
	"log"
	"net/url"
	"strings"
	"time"
)


type Crawler struct {
	Config *Config
}

func (s *Crawler) StartCrawl() {
	ctx := context.Background()

	consumer := NewKafkaConsumer(s.Config.Kafka.Brokers, s.Config.Kafka.HostsTopic, "crawler-group")
	defer consumer.Close()

	pagesWriter := NewKafkaProducer(s.Config.Kafka.Brokers, s.Config.Kafka.PagesTopic)
	defer pagesWriter.Close()

	hostsWriter := NewKafkaProducer(s.Config.Kafka.Brokers, s.Config.Kafka.HostsTopic)
	defer hostsWriter.Close()

	redis := NewRedisClient(s.Config.Redis.Addr)
	filter := NewFilter(s.Config.Filter)

	sem := make(chan struct{}, s.Config.Crawler.MaxWorkers)

	for {
		host, err := consumer.ReadHost(ctx)
		if err != nil {
			log.Printf("consumer error: %v", err)
			continue
		}

		if filter.IsBlocked(host) {
			log.Printf("blocked host: %s", host)
			continue
		}

		if !redis.TryClaimHost(ctx, host, s.Config.Redis.ClaimTTL) {
			continue
		}

		sem <- struct{}{}
		go func(h string) {
			defer func() { <-sem }()
			s.crawl(h, pagesWriter, hostsWriter, filter)
		}(host)
	}
}

func (s *Crawler) crawl(baseDomain string, pagesWriter, hostsWriter *KafkaProducer, filter *Filter) {
	hos := &Host{
		baseDomain: baseDomain,
		subDomains: make([]string, 0),
		seen:       make(map[string]int),
	}
	queuedNewHosts := make(map[string]bool)

	err := getRobotsTxt(hos.baseDomain, hos)
	fmt.Println("Got Robots.txt")
	if err != nil {
		hos.errs = append(hos.errs, err.Error())
		fmt.Printf("Error: %s \n", err)
	}
	if hos.disallowAll {
		return
	}

	if hos.crawlDelay == 0 {
		hos.crawlDelay = time.Second
	}

	hos.subDomains = append(hos.subDomains, hos.baseDomain)
	for i := 0; i < len(hos.subDomains); i++ {

		time.Sleep(hos.crawlDelay)
		domain := strings.TrimRight(hos.subDomains[i], "/")
		hos.subDomains[i] = ""
		hos.seen[domain] += 1

		resp, err := fetch(domain)
		fmt.Printf("Fetched Subdomain: %s \n", domain)
		if err != nil {
			fmt.Printf("Error in get request: %s \n", err.Error())
			hos.errs = append(hos.errs, err.Error())
			continue
		}

		if resp.StatusCode != 200 {
			resp.Body.Close()
			fmt.Printf("Bad Status code from: %s \n", domain)
			continue
		}

		bodyBytes, err := io.ReadAll(resp.Body)
		resp.Body.Close()

		if err != nil {
			fmt.Printf("Error reading response body: %s \n", err.Error())
			continue
		}

		if len(bodyBytes) > s.Config.Crawler.MaxBodyBytes {
			log.Printf("skipping oversized page %s (%d bytes)", domain, len(bodyBytes))
			continue
		}

		if !pagesWriter.SendMessage(Message{
			Key:   domain,
			Value: string(bodyBytes),
		}) {
			fmt.Println("Failed to send Message to Kafka")
		}

		links := getLinksFromHTML(resp, bodyBytes)
		if links == nil {
			fmt.Printf("No links grabbed for %s \n", domain)
			continue
		}

		for _, rawURL := range links {
			rawURL = strings.TrimRight(normalizePageURL(rawURL), "/")
			if hos.seen[rawURL] > 0 {
				continue
			}
			if u, err := url.Parse(rawURL); err == nil {
				if host := u.Hostname(); filter.IsBlocked(host) {
					log.Printf("skipped blocked URL: %s", rawURL)
					continue
				}
			}
			new, newbase := isNewHost(hos.baseDomain, rawURL)
			if new {
				if !queuedNewHosts[newbase] && !filter.IsBlocked(newbase) {
					queuedNewHosts[newbase] = true
					fmt.Printf("New host: %s \n", newbase)
					hostsWriter.SendMessage(Message{Key: newbase, Value: newbase})
				}
				continue
			}
			if isDisallowed(rawURL, hos.disallowed) {
				continue
			}
			if len(hos.subDomains) < s.Config.Crawler.MaxPagesPerHost {
				hos.subDomains = append(hos.subDomains, rawURL)
				hos.seen[rawURL] = 1
			}
		}
	}
}
