package crawler

import (
	"bytes"
	"fmt"
	"net/http"
	"net/url"
	urlpkg "net/url"
	"strings"

	"github.com/PuerkitoBio/goquery"
)

func getLinksFromHTML(resp *http.Response, body []byte) []string {

	urls := make([]string, 0)

	doc, err := goquery.NewDocumentFromReader(bytes.NewReader(body))
	if err != nil {
		return nil
	}

	doc.Find("a[href]").Each(func(i int, s *goquery.Selection) {
		href, exists := s.Attr("href")
		if exists {
			href = strings.TrimSpace(href)
			if len(href) == 0 || href[0] == '#' {
				return
			}
			ref, err := url.Parse(href)
			if err != nil {
				return
			}
			abs := resp.Request.URL.ResolveReference(ref)
			if abs.Scheme != "http" && abs.Scheme != "https" {
				return
			}
			urls = append(urls, abs.String())
		}
	})
	return urls
}

func isDisallowed(rawURL string, patterns []string) bool {
	parsed, err := urlpkg.Parse(rawURL)
	if err != nil {
		return false
	}
	for _, pattern := range patterns {
		if pattern != "" && strings.HasPrefix(parsed.Path, pattern) {
			fmt.Printf("isDisallowed: %s matched pattern %q\n", rawURL, pattern)
			return true
		}
	}
	return false
}
