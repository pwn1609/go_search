package crawler

import (
	"net/url"
	"time"

	"golang.org/x/net/publicsuffix"
)

type Host struct {
	crawlDelay     time.Duration
	subDomains     []string
	baseDomain     string
	seen           map[string]int
	disallowed     []string
	allowed        []string
	errs           []string
	disallowAll    bool
	temporaryDelay time.Time // use as a delay to retry
}

func isNewHost(currentHost, newStr string) (bool, string) {
	u, err := url.Parse(newStr)
	if err != nil {
		return false, ""
	}

	newHost := u.Hostname()
	if newHost == "" {
		newHost = newStr
	}

	curReg, _ := publicsuffix.EffectiveTLDPlusOne(currentHost)
	newReg, err := publicsuffix.EffectiveTLDPlusOne(newHost)
	// fmt.Printf("Host compare: %s, %s, %t \n", newReg, curReg, newReg != curReg)
	return newReg != curReg, newReg
}
