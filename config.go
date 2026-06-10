package main

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
)

func loadConfig(path string) (Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return Config{}, err
	}
	var cfg Config
	if err := json.Unmarshal(data, &cfg); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

func validateConfig(cfg Config) error {
	if len(cfg.Hosts) == 0 {
		return fmt.Errorf("hosts must not be empty")
	}
	for _, h := range cfg.Hosts {
		if strings.TrimSpace(h) == "" {
			return fmt.Errorf("hosts must not contain empty entries")
		}
	}
	if cfg.IntervalSeconds <= 0 {
		return fmt.Errorf("interval_seconds must be > 0")
	}
	if cfg.RoundTimeoutSeconds <= 0 {
		return fmt.Errorf("round_timeout_seconds must be > 0")
	}
	if cfg.RetryIntervalSeconds <= 0 {
		return fmt.Errorf("retry_interval_seconds must be > 0")
	}
	if cfg.IPCheckIntervalSeconds < 0 {
		return fmt.Errorf("ip_check_interval_seconds must be >= 0")
	}
	return nil
}

func formatStartLine(cfg Config) string {
	dns := "system default"
	if len(cfg.DNSServers) > 0 {
		dns = strings.Join(cfg.DNSServers, " ")
	}
	ipCheck := "off"
	if cfg.IPCheckIntervalSeconds > 0 {
		url := cfg.IPCheckURL
		if url == "" {
			url = defaultIPCheckURL
		}
		ipCheck = fmt.Sprintf("%ds via %s", cfg.IPCheckIntervalSeconds, url)
	}
	return fmt.Sprintf(
		"[START] hosts: %s  dns: %s  interval: %ds  round_timeout: %ds  retry: %ds  ip_check: %s",
		strings.Join(cfg.Hosts, " "),
		dns,
		cfg.IntervalSeconds,
		cfg.RoundTimeoutSeconds,
		cfg.RetryIntervalSeconds,
		ipCheck,
	)
}
