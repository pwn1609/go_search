package crawler

import "strings"

type Filter struct {
	blockedKeywords []string
	blockedDomains  map[string]bool
}

func NewFilter(cfg FilterConfig) *Filter {
	blocked := make(map[string]bool, len(cfg.BlockedDomains))
	for _, d := range cfg.BlockedDomains {
		blocked[strings.ToLower(d)] = true
	}
	return &Filter{
		blockedKeywords: cfg.BlockedKeywords,
		blockedDomains:  blocked,
	}
}

// IsBlocked returns true if the host matches any blocked domain or keyword.
func (f *Filter) IsBlocked(host string) bool {
	lower := strings.ToLower(host)
	if f.blockedDomains[lower] {
		return true
	}
	for _, kw := range f.blockedKeywords {
		if strings.Contains(lower, kw) {
			return true
		}
	}
	return false
}
