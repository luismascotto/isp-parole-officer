package main

import (
	"net/http"
	"os"
	"sync"
	"time"
)

type Config struct {
	Hosts                  []string `json:"hosts"`
	DNSServers             []string `json:"dns_servers"`
	IntervalSeconds        int      `json:"interval_seconds"`
	RoundTimeoutSeconds    int      `json:"round_timeout_seconds"`
	RetryIntervalSeconds   int      `json:"retry_interval_seconds"`
	IPCheckIntervalSeconds int      `json:"ip_check_interval_seconds"`
	IPCheckURL             string   `json:"ip_check_url"`
}

type HostCache struct {
	ip              string
	consecutiveUses int
}

type HourlyLogger struct {
	outputDir string
	//sessionID string
	mu      sync.Mutex
	file    *os.File
	hourKey string
}

type Session struct {
	sessionID  string
	config     Config
	caches     map[string]*HostCache
	cacheMu    sync.Mutex
	logger     *HourlyLogger
	httpClient *http.Client
}

type ProbeOutcomeKind int

const (
	ProbeOutcomeKindSuccess ProbeOutcomeKind = iota
	ProbeOutcomeKindError
	ProbeOutcomeKindStopped
)

type ProbeOutcome struct {
	kind         ProbeOutcomeKind
	err          error
	latency      map[string]time.Duration
	successIPs   map[string]string
	usedCachedIP map[string]bool
	avgMs        int64
	detail       string
	waitNext     time.Duration
}

type HostLatency struct {
	host    string
	latency time.Duration
}
