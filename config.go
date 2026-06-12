package main

import (
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
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
	if cfg.RoundInterval <= 0 {
		return fmt.Errorf("interval_seconds must be > 0")
	}
	if cfg.RoundTimeout <= 0 {
		return fmt.Errorf("round_timeout_seconds must be > 0")
	}
	if cfg.RoundRetryInterval <= 0 {
		return fmt.Errorf("retry_interval_seconds must be > 0")
	}
	if cfg.IPCheckInterval < 0 {
		return fmt.Errorf("ip_check_interval_seconds must be >= 0")
	}
	if cfg.IPCheckInterval > 0 && cfg.IPCheckTimeout <= 0 {
		return fmt.Errorf("ip_check_timeout_seconds must be > 0 when ip_check_interval_seconds is > 0")
	}
	return nil
}

func formatConfig(cfg Config) string {
	var strb strings.Builder
	strb.WriteString("\t[HOSTS] ")
	strb.WriteString(strings.Join(cfg.Hosts, "  "))
	strb.WriteString("\n\t[DNS] ")
	if len(cfg.DNSServers) > 0 {
		strb.WriteString(strings.Join(cfg.DNSServers, "  "))
	} else {
		strb.WriteString("system default")
	}

	strb.WriteString("\n\t[ROUND] interval ")
	strb.WriteString(strconv.Itoa(int(cfg.RoundInterval / time.Second)))
	strb.WriteString("s, timeout ")
	strb.WriteString(strconv.Itoa(int(cfg.RoundTimeout / time.Second)))
	strb.WriteString("s, retry ")
	strb.WriteString(strconv.Itoa(int(cfg.RoundRetryInterval / time.Second)))
	strb.WriteString("s")

	strb.WriteString("\n\t[IP CHECK] ")
	if cfg.IPCheckInterval > 0 {
		strb.WriteString("interval ")
		strb.WriteString(strconv.Itoa(int(cfg.IPCheckInterval / time.Second)))
		strb.WriteString("s, timeout ")
		strb.WriteString(strconv.Itoa(int(cfg.IPCheckTimeout / time.Second)))
		strb.WriteString("s, URL ")
		strb.WriteString(cfg.IPCheckURL)
	} else {
		strb.WriteString("off")
	}
	return strb.String()
}
