package crawler

import (
	"bufio"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

const crawlerUserAgent = "MyCrawler/1.0"

var httpClient = &http.Client{Timeout: 15 * time.Second}

func fetch(raw string) (*http.Response, error) {
	// Parse the URL first
	u, err := url.Parse(raw)
	if err != nil {
		return nil, err
	}

	// Ensure scheme exists
	if u.Scheme == "" {
		u.Scheme = "https"
	}

	// If host is still empty (like "example.com/path" without scheme)
	if u.Host == "" {
		// Re-parse assuming https
		u, err = url.Parse("https://" + raw)
		if err != nil {
			return nil, err
		}
	}

	// Build request explicitly (better than http.Get)
	req, err := http.NewRequest("GET", u.String(), nil)
	if err != nil {
		return nil, err
	}

	// Set a User-Agent (important for crawlers)
	req.Header.Set("User-Agent", crawlerUserAgent)

	return httpClient.Do(req)
}

func getRobotsTxt(baseDom string, hos *Host) error {
	if !strings.HasPrefix(baseDom, "http://") && !strings.HasPrefix(baseDom, "https://") {
		baseDom = "https://" + baseDom
	}

	urlRes, err := url.JoinPath(baseDom, "/robots.txt")
	if err != nil {
		return fmt.Errorf("getRobotsTxt: failed to build URL for %q: %w", baseDom, err)
	}
	fmt.Printf("getRobotsTxt: fetching %s\n", urlRes)

	res, err := httpClient.Get(urlRes)
	if err != nil {
		return fmt.Errorf("getRobotsTxt: HTTP request to %q failed: %w", urlRes, err)
	}
	defer res.Body.Close()

	switch res.StatusCode {
	case 401, 403:
		fmt.Printf("getRobotsTxt: %d response, disallowing all for %s\n", res.StatusCode, baseDom)
		hos.disallowAll = true
		return nil
	case 404:
		fmt.Printf("getRobotsTxt: no robots.txt found for %s, proceeding\n", baseDom)
		return nil
	}

	scanner := bufio.NewScanner(res.Body)
	lineNum := 0
	relevantBlock := false // whether current User-Agent block applies to us
	for scanner.Scan() {
		lineNum++
		line := scanner.Text()
		if line == "" || line[0] == '#' {
			continue
		}
		splitLine := strings.SplitN(line, ":", 2)
		if len(splitLine) < 2 {
			fmt.Printf("getRobotsTxt: skipping malformed line %d: %q\n", lineNum, line)
			continue
		}
		key := strings.ToLower(strings.TrimSpace(splitLine[0]))
		value := strings.TrimSpace(splitLine[1])

		if key == "user-agent" {
			agent := strings.ToLower(value)
			relevantBlock = (agent == "*" || strings.Contains(agent, "mycrawler"))
			continue
		}

		if !relevantBlock {
			continue
		}

		switch key {
		case "disallow":
			hos.disallowed = append(hos.disallowed, value)
		case "allow":
			hos.allowed = append(hos.allowed, value)
		case "crawl-delay":
			if seconds, err := strconv.ParseFloat(value, 64); err == nil && seconds > 0 {
				hos.crawlDelay = time.Duration(seconds * float64(time.Second))
			}
		}
	}
	if err := scanner.Err(); err != nil {
		return fmt.Errorf("getRobotsTxt: error reading body from %q: %w", urlRes, err)
	}
	return nil
}
