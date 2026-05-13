package crawler

//TODO:
//Change Dockerfile so its not copying in the entire interal and cmd directory

import (
	"fmt"
	"io"
	"strings"
	"sync"
	"time"
)

type Crawler struct {
	RetriesPerPage   int
	RequestPerSecond int
	Delay            time.Time
	Config           *Config
}

func (s *Crawler) StartCrawl() {

	var wg sync.WaitGroup
	seenHosts := make(map[string]int)
	hosts := make(chan Host, 100)
	startingHost := Host{
		baseDomain: s.Config.Crawler.SeedURL,
		subDomains: make([]string, 0),
		seen:       make(map[string]int),
	}

	go func() {
		wg.Wait()
		close(hosts)
	}()

	writer := NewKafkaProducer(s.Config.Kafka.Brokers[0], s.Config.Kafka.Topic)
	fmt.Println(s.Config.Kafka.Brokers[0])
	defer writer.Close()

	wg.Add(1)
	hosts <- startingHost
	i := 0

	for host := range hosts {
		//skip if already seen
		i++

		if seenHosts[host.baseDomain] == 0 {
			fmt.Printf("Crawling: %s\n", host.baseDomain)
			seenHosts[host.baseDomain] = 1
			go s.crawl(&host, hosts, &wg, writer)
		} else {
			wg.Done()
		}
		if i >= 10 {
			break
		}
	}

	// Drain the channel so crawl goroutines blocked on list <- newHost can finish.
	// Each drained item must release its wg count since the main loop won't process it.
	go func() {
		for range hosts {
			wg.Done()
		}
	}()

	wg.Wait()
}

func (s *Crawler) crawl(hos *Host, list chan Host, wg *sync.WaitGroup, writer *KafkaProducer) {
	defer wg.Done()

	err := getRobotsTxt(hos.baseDomain, hos)
	fmt.Println("Got Robots.txt")
	if err != nil {
		hos.errs = append(hos.errs, err.Error())
		fmt.Printf("Error: %s \n", err)
	}
	if hos.disallowAll {
		return
	}

	// Default to 1 second if no crawl-delay was specified
	if hos.crawlDelay == 0 {
		hos.crawlDelay = time.Second
	}

	hos.subDomains = append(hos.subDomains, hos.baseDomain)
	for i := 0; i < len(hos.subDomains); i++ {
		//temporary break - only crawl first 30 pages of a host
		if i > 30 {
			break
		}

		time.Sleep(hos.crawlDelay)
		domain := strings.TrimRight(hos.subDomains[i], "/")
		hos.seen[domain] += 1

		resp, err := fetch(domain)
		fmt.Printf("Fetched Subdomain: %s \n", domain)
		//Error comes after this somewhere
		if err != nil {
			fmt.Printf("Error in get request: %s \n", err.Error())
			hos.errs = append(hos.errs, err.Error())
			continue
		}

		//check status code
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

		//send url and body to kafka
		if !writer.SendMessage(Message{
			Key:   domain,
			Value: string(bodyBytes),
		}) {
			fmt.Println("Failed to send Message to Kafka")
		}

		//get all href's
		links := getLinksFromHTML(resp, bodyBytes)
		if links == nil {
			fmt.Printf("No links grabbed for %s \n", domain)
			continue
		}

		//determine if subdomain has been seen and determine if new host
		//append new domains to list
		for _, url := range links {
			url = strings.TrimRight(url, "/")
			if hos.seen[url] > 0 {
				continue
			}
			//if new host then create a new host and add to channel then finish ittr
			new, newbase := isNewHost(hos.baseDomain, url)
			if new {
				fmt.Printf("New host: %s \n", newbase)
				newHost := Host{
					baseDomain: newbase,
					subDomains: make([]string, 0),
					seen:       make(map[string]int),
				}
				wg.Add(1)
				list <- newHost
				continue
			}
			if isDisallowed(url, hos.disallowed) {
				continue
			}
			hos.subDomains = append(hos.subDomains, url)

		}
	}

}
