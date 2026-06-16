package main

import (
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"
)

type Config struct {
	Hosts              []string      `json:"hosts"`
	DNSServers         []string      `json:"dns_servers"`
	RoundInterval      time.Duration `json:"round_interval_seconds"`
	RoundTimeout       time.Duration `json:"round_timeout_seconds"`
	RoundRetryInterval time.Duration `json:"round_retry_interval_seconds"`
	IPCheckInterval    time.Duration `json:"ip_check_interval_seconds"`
	IPCheckTimeout     time.Duration `json:"ip_check_timeout_seconds"`
	IPCheckURL         string        `json:"ip_check_url"`
}

type HostCache struct {
	ip              string
	consecutiveUses int
}

type HourlyLogger struct {
	outputDir string
	mu        sync.Mutex
	file      *os.File
	currHour  int
	strb      strings.Builder
}

type Session struct {
	sessionID    string
	config       Config
	caches       map[string]*HostCache
	cacheMu      sync.Mutex
	logger       *HourlyLogger
	httpClient   *http.Client
	roundControl *RoundControl
	DNSResolvers []DNSResolver
}

type ProbeOutcomeKind int

const (
	ProbeOutcomeKindSuccess ProbeOutcomeKind = iota
	ProbeOutcomeKindError
	ProbeOutcomeKindStopped
)

type ProbeOutcome struct {
	kind     ProbeOutcomeKind
	err      error
	avgMs    int64
	detail   string
	waitNext time.Duration
}

type RoundControl struct {
	minRequired int
	latencies   []time.Duration
	strbResult  strings.Builder
	// Cache control
	successIPs   map[string]string
	usedCachedIP map[string]bool
}

type DNSResolver struct {
	server   string
	resolver *net.Resolver
}
