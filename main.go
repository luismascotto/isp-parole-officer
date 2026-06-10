package main

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/signal"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/google/uuid"
)

const (
	tcpProbePort       = "443"
	maxConsecutiveIP   = 5
	resultsDirName     = "Results"
	defaultIPCheckURL  = "https://api.ipify.org"
	maxIPCheckResponse = 64
)

func main() {
	cfg, err := loadConfig("config.json")
	if err != nil {
		fmt.Fprintf(os.Stderr, "config error: %v\n", err)
		os.Exit(1)
	}

	if err := validateConfig(cfg); err != nil {
		fmt.Fprintf(os.Stderr, "config error: %v\n", err)
		os.Exit(1)
	}

	uuidV7, err := uuid.NewV7()
	if err != nil {
		fmt.Fprintf(os.Stderr, "session id error: %v\n", err)
		os.Exit(1)
	}
	sessionID := uuidV7.String()

	logger, err := newHourlyLogger(sessionID)
	if err != nil {
		fmt.Fprintf(os.Stderr, "log file error: %v\n", err)
		os.Exit(1)
	}

	s := &Session{
		sessionID: sessionID,
		config:    cfg,
		caches:    make(map[string]*HostCache, len(cfg.Hosts)),
		logger:    logger,
		httpClient: &http.Client{
			Timeout: time.Duration(cfg.RoundTimeoutSeconds) * time.Second,
		},
	}

	if err := s.preflight(); err != nil {
		fmt.Fprintf(os.Stderr, "%v\n%s\n", err, "Review and adjust config.json, then run again.")
		os.Exit(1)
	}

	s.logger.logLine(formatStartLine(cfg))

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	var background sync.WaitGroup
	if cfg.IPCheckIntervalSeconds > 0 {
		background.Add(1)
		go func() {
			defer background.Done()
			s.runIPChecker(ctx)
		}()
	}

	monitorDone := make(chan struct{})
	go func() {
		defer close(monitorDone)
		if !sleepOrStop(ctx, time.Duration(cfg.IntervalSeconds)*time.Second) {
			return
		}
		for {
			if ctx.Err() != nil {
				return
			}
			outcome := s.runRound(ctx)
			if outcome.kind == ProbeOutcomeKindStopped || ctx.Err() != nil {
				return
			}
			s.applyCacheAfterRound(outcome)
			s.logger.logLine(outcome.detail)
			if !sleepOrStop(ctx, outcome.waitNext) {
				return
			}
		}
	}()

	<-ctx.Done()
	stop()

	select {
	case <-monitorDone:
	case <-time.After(time.Duration(cfg.RoundTimeoutSeconds)*time.Second + 2*time.Second):
	}

	waitBackground := make(chan struct{})
	go func() {
		background.Wait()
		close(waitBackground)
	}()
	select {
	case <-waitBackground:
	case <-time.After(2 * time.Second):
	}

	s.logger.logLine("[STOP]")
}

func (s *Session) preflight() error {
	timeout := time.Duration(s.config.RoundTimeoutSeconds) * time.Second
	for _, host := range s.config.Hosts {
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		_, _, _, err := s.probeHost(ctx, host, false)
		cancel()
		if err != nil {
			return fmt.Errorf("preflight failed for %s: %w", host, err)
		}
	}
	return nil
}

func (s *Session) runRound(parent context.Context) ProbeOutcome {
	required := max(1, (len(s.config.Hosts)+1)/2)
	roundTimeout := time.Duration(s.config.RoundTimeoutSeconds) * time.Second
	ctx, cancel := context.WithCancel(parent)
	defer cancel()

	type workerResult struct {
		host         string
		latency      time.Duration
		ip           string
		usedCachedIP bool
		err          error
	}

	results := make(chan workerResult, len(s.config.Hosts))
	var decided atomic.Bool
	var workers sync.WaitGroup

	launchWorker := func(host string) {
		workers.Add(1)
		go func() {
			defer workers.Done()
			hostCtx, hostCancel := context.WithTimeout(ctx, roundTimeout)
			defer hostCancel()

			latency, ip, usedCachedIP, err := s.probeHost(hostCtx, host, true)
			if err == nil && hostCtx.Err() == context.DeadlineExceeded {
				err = context.DeadlineExceeded
			}

			if ctx.Err() != nil || decided.Load() {
				return
			}

			select {
			case <-ctx.Done():
			case results <- workerResult{
				host:         host,
				latency:      latency,
				ip:           ip,
				usedCachedIP: usedCachedIP,
				err:          err,
			}:
			}
		}()
	}

	for _, host := range s.config.Hosts {
		launchWorker(host)
	}

	successes := make(map[string]time.Duration)
	successIPs := make(map[string]string)
	usedCachedIP := make(map[string]bool)
	var roundErr error
	stopped := false

	waitWorkers := func() {
		done := make(chan struct{})
		go func() {
			workers.Wait()
			close(done)
		}()
		select {
		case <-done:
		case <-time.After(roundTimeout + time.Second):
		}
	}

collect:
	for received := 0; received < len(s.config.Hosts); received++ {
		select {
		case <-parent.Done():
			cancel()
			waitWorkers()
			stopped = true
			break collect
		case res := <-results:
			if decided.Load() {
				continue
			}

			if res.err != nil {
				decided.Store(true)
				cancel()
				roundErr = res.err
				break collect
			}

			successes[res.host] = res.latency
			successIPs[res.host] = res.ip
			usedCachedIP[res.host] = res.usedCachedIP

			if len(successes) >= required {
				decided.Store(true)
				cancel()
				break collect
			}
		}
	}

	if stopped {
		return ProbeOutcome{kind: ProbeOutcomeKindStopped, waitNext: 0}
	}

	if roundErr != nil {
		return ProbeOutcome{
			kind:     ProbeOutcomeKindError,
			err:      roundErr,
			detail:   "[ERROR] " + describeError(roundErr),
			waitNext: time.Duration(s.config.RetryIntervalSeconds) * time.Second,
		}
	}

	avg := averageLatencyMs(successes)
	detail := fmt.Sprintf(
		"[SUCCESS] %dms (%s)",
		avg,
		formatHostLatencies(s.config.Hosts, successes),
	)

	return ProbeOutcome{
		kind:         ProbeOutcomeKindSuccess,
		latency:      successes,
		successIPs:   successIPs,
		usedCachedIP: usedCachedIP,
		avgMs:        avg,
		detail:       detail,
		waitNext:     time.Duration(s.config.IntervalSeconds) * time.Second,
	}
}

func (s *Session) probeHost(ctx context.Context, host string, useCache bool) (time.Duration, string, bool, error) {
	target, usedCachedIP := s.targetForHost(host, useCache)

	start := time.Now()
	conn, err := dialTCP(ctx, target)
	if err != nil {
		if usedCachedIP {
			resolved, resolveErr := s.resolve(ctx, host)
			if resolveErr != nil {
				return 0, "", false, resolveErr
			}
			conn, err = dialTCP(ctx, resolved)
			if err != nil {
				return 0, "", false, err
			}
			target = resolved
			usedCachedIP = false
		} else if net.ParseIP(target) == nil {
			resolved, resolveErr := s.resolve(ctx, host)
			if resolveErr != nil {
				return 0, "", false, resolveErr
			}
			conn, err = dialTCP(ctx, resolved)
			if err != nil {
				return 0, "", false, err
			}
			target = resolved
		} else {
			return 0, "", usedCachedIP, err
		}
	}
	_ = conn.Close()
	return time.Since(start), target, usedCachedIP, nil
}

func (s *Session) targetForHost(host string, useCache bool) (target string, usedCachedIP bool) {
	if !useCache {
		return host, false
	}

	s.cacheMu.Lock()
	defer s.cacheMu.Unlock()

	entry := s.caches[host]
	if entry != nil && entry.ip != "" && entry.consecutiveUses < maxConsecutiveIP {
		return entry.ip, true
	}
	return host, false
}

func (s *Session) resolve(ctx context.Context, host string) (string, error) {
	if ip := net.ParseIP(host); ip != nil {
		return host, nil
	}

	if len(s.config.DNSServers) == 0 {
		resolver := &net.Resolver{PreferGo: true}
		ips, err := resolver.LookupIPAddr(ctx, host)
		if err != nil {
			return "", fmt.Errorf("couldn't resolve name %s: %w", host, err)
		}
		if len(ips) == 0 {
			return "", fmt.Errorf("couldn't resolve name %s", host)
		}
		return ips[0].IP.String(), nil
	}

	var lastErr error
	for _, server := range s.config.DNSServers {
		resolver := &net.Resolver{
			PreferGo: true,
			Dial: func(ctx context.Context, network, address string) (net.Conn, error) {
				d := net.Dialer{}
				return d.DialContext(ctx, "udp", net.JoinHostPort(server, "53"))
			},
		}
		ips, err := resolver.LookupIPAddr(ctx, host)
		if err != nil {
			lastErr = err
			continue
		}
		if len(ips) == 0 {
			lastErr = fmt.Errorf("no records")
			continue
		}
		return ips[0].IP.String(), nil
	}
	if lastErr != nil {
		return "", fmt.Errorf("couldn't resolve name %s: %w", host, lastErr)
	}
	return "", fmt.Errorf("couldn't resolve name %s", host)
}

func dialTCP(ctx context.Context, host string) (net.Conn, error) {
	var d net.Dialer
	return d.DialContext(ctx, "tcp", net.JoinHostPort(host, tcpProbePort))
}

func (s *Session) applyCacheAfterRound(outcome ProbeOutcome) {
	s.cacheMu.Lock()
	defer s.cacheMu.Unlock()

	if outcome.kind != ProbeOutcomeKindSuccess {
		s.caches = make(map[string]*HostCache, len(s.config.Hosts))
		return
	}

	for _, host := range s.config.Hosts {
		if _, ok := outcome.latency[host]; !ok {
			continue
		}

		ip := outcome.successIPs[host]
		entry := s.caches[host]
		if entry == nil {
			entry = &HostCache{}
			s.caches[host] = entry
		}

		entry.ip = ip
		if outcome.usedCachedIP[host] {
			entry.consecutiveUses++
			continue
		}
		entry.consecutiveUses = 0
	}
}

func averageLatencyMs(successes map[string]time.Duration) int64 {
	if len(successes) == 0 {
		return 0
	}
	var total time.Duration
	for _, d := range successes {
		total += d
	}
	return total.Milliseconds() / int64(len(successes))
}

func formatHostLatencies(hosts []string, successes map[string]time.Duration) string {
	// Sort successes by latency
	hostLatencies := make([]HostLatency, 0, len(successes))
	for host, latency := range successes {
		hostLatencies = append(hostLatencies, HostLatency{host: host, latency: latency})
	}
	sort.Slice(hostLatencies, func(i, j int) bool {
		return hostLatencies[i].latency < hostLatencies[j].latency
	})

	parts := make([]string, 0, len(hostLatencies))
	for _, hostLatency := range hostLatencies {
		parts = append(parts, fmt.Sprintf("%s:%dms", hostLatency.host, hostLatency.latency.Milliseconds()))
	}

	for _, host := range hosts {
		if _, ok := successes[host]; !ok {
			parts = append(parts, host+":-")
		}
	}

	return strings.Join(parts, ", ")
}

func describeError(err error) string {
	if err == nil {
		return "unknown error"
	}
	if err == context.DeadlineExceeded {
		return "Timeout"
	}
	msg := err.Error()
	lower := strings.ToLower(msg)
	switch {
	case strings.Contains(lower, "resolve"):
		return msg
	case strings.Contains(lower, "timeout"), strings.Contains(lower, "i/o timeout"):
		return "Timeout"
	case strings.Contains(lower, "connection refused"):
		return "Connection refused"
	case strings.Contains(lower, "no such host"):
		return "Couldn't resolve name"
	default:
		return msg
	}
}

func sleepOrStop(ctx context.Context, d time.Duration) bool {
	if ctx.Err() != nil {
		return false
	}
	if d <= 0 {
		return true
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func (s *Session) runIPChecker(ctx context.Context) {
	interval := time.Duration(s.config.IPCheckIntervalSeconds) * time.Second
	url := s.config.IPCheckURL
	if url == "" {
		url = defaultIPCheckURL
	}

	var lastIP string
	check := func() {
		if ctx.Err() != nil {
			return
		}
		checkCtx, cancel := context.WithTimeout(ctx, time.Duration(s.config.RoundTimeoutSeconds)*time.Second)
		defer cancel()

		ip, err := s.fetchPublicIP(checkCtx, url)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			s.logger.logLine("[IP] ERROR " + err.Error())
			return
		}

		if lastIP != "" && ip != lastIP {
			s.logger.logLine(fmt.Sprintf("[IP] changed %s -> %s", lastIP, ip))
		} else {
			s.logger.logLine("[IP] " + ip)
		}
		lastIP = ip
	}

	check()

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			check()
		}
	}
}

func (s *Session) fetchPublicIP(ctx context.Context, url string) (string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return "", err
	}

	resp, err := s.httpClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("service returned HTTP %d", resp.StatusCode)
	}

	body, err := io.ReadAll(io.LimitReader(resp.Body, maxIPCheckResponse))
	if err != nil {
		return "", err
	}

	ip := strings.TrimSpace(string(body))
	if parsed := net.ParseIP(ip); parsed == nil {
		return "", fmt.Errorf("invalid IP in response: %q", ip)
	}
	return ip, nil
}
