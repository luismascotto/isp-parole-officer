package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/url"
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

func (cfg Config) formatConfig() string {
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

func (cfg Config) newDNSResolvers() []DNSResolver {
	resolvers := make([]DNSResolver, 0)
	if len(cfg.DNSServers) == 0 {
		resolvers = append(resolvers, DNSResolver{server: "local", resolver: &net.Resolver{
			PreferGo: true,
		}})
	} else {
		for _, server := range cfg.DNSServers {
			resolvers = append(resolvers, DNSResolver{server: server, resolver: &net.Resolver{
				PreferGo: true,
				Dial: func(ctx context.Context, network, address string) (net.Conn, error) {
					return net.Dial(network, address)
				},
			}})
		}
	}
	return resolvers
}

func (cfg Config) newRoundControl() *RoundControl {
	roundControl := &RoundControl{
		latencies: make([]time.Duration, 0, len(cfg.Hosts)),
		//strbResult:   &strings.Builder{},
		minRequired:  max(1, (len(cfg.Hosts)+1)/2),
		successIPs:   make(map[string]string, len(cfg.Hosts)),
		usedCachedIP: make(map[string]bool, len(cfg.Hosts)),
	}
	return roundControl
}

func (cfg Config) newIPChecker() IPChecker {
	if cfg.IPCheckURL == "" {
		return IPChecker{}
	}
	u, err := url.Parse(cfg.IPCheckURL)
	if err != nil {
		return IPChecker{}
	}
	return IPChecker{
		httpClient: &http.Client{
			Timeout: cfg.IPCheckTimeout,
		},
		req: &http.Request{
			Method: http.MethodGet,
			URL:    u,
		},
		bodyResp: make([]byte, maxIPCheckResponse),
	}
}
