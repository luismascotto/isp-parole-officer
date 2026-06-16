package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	_ "net/http/pprof"

	"github.com/google/uuid"
)

const (
	tcpProbePort         = "443"
	maxConsecutiveIP     = 3
	resultsDirName       = "Results"
	resultFileNameFormat = "2006-01-02_15"
	resultFileExtension  = ".txt"
	defaultIPCheckURL    = "https://api.ipify.org"
	maxIPCheckResponse   = 64
	preflightTimeout     = 10 * time.Second
)

func main() {
	// Start an internal port exclusively for profiling and health checks
	// go tool pprof http://localhost:6060/debug/pprof/heap to view heap allocations
	// go tool pprof http://localhost:6060/debug/pprof/profile to view CPU profile
	// go tool pprof -http=:8080 http://localhost:6060/debug/pprof/* web interface
	go func() {
		log.Println(http.ListenAndServe("localhost:6060", nil))
	}()

	config, err := loadConfig("config.json")
	if err != nil {
		fmt.Fprintf(os.Stderr, "config error: %v\n", err)
		os.Exit(1)
	}

	if err := validateConfig(config); err != nil {
		fmt.Fprintf(os.Stderr, "config error: %v\n", err)
		os.Exit(1)
	}
	config.RoundInterval *= time.Second
	config.RoundTimeout *= time.Second
	config.RoundRetryInterval *= time.Second
	config.IPCheckInterval *= time.Second
	config.IPCheckTimeout *= time.Second
	if config.IPCheckURL == "" {
		config.IPCheckURL = defaultIPCheckURL
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

	roundControl := &RoundControl{
		latencies: make([]time.Duration, 0, len(config.Hosts)),
		//strbResult:   &strings.Builder{},
		minRequired:  max(1, (len(config.Hosts)+1)/2),
		successIPs:   make(map[string]string, len(config.Hosts)),
		usedCachedIP: make(map[string]bool, len(config.Hosts)),
	}

	s := &Session{
		sessionID: sessionID,
		config:    config,
		caches:    make(map[string]*HostCache, len(config.Hosts)),
		logger:    logger,
		httpClient: &http.Client{
			Timeout: config.RoundTimeout,
		},
		roundControl: roundControl,
	}

	s.logger.LogLine("[CONFIG]\n" + formatConfig(config))

	if err := s.preflight(); err != nil {
		fmt.Fprintf(os.Stderr, "%v\n%s\n", err, "Review and adjust config.json, then run again.")
		os.Exit(1)
	}

	s.logger.LogLine("[START]")

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	var background sync.WaitGroup
	if config.IPCheckInterval > 0 {
		background.Go(func() {
			s.runIPChecker(ctx)
		})
	}

	monitorDone := make(chan struct{})
	go func() {
		defer close(monitorDone)
		roundInterval := config.RoundInterval
		for {
			if !sleepOrStop(ctx, roundInterval) {
				return
			}
			outcome := s.runRound(ctx)
			if outcome.kind == ProbeOutcomeKindStopped || ctx.Err() != nil {
				return
			}
			s.applyCacheAfterRound(outcome)
			s.logger.LogLine(outcome.detail)
			roundInterval = outcome.waitNext
		}
	}()

	<-ctx.Done()
	stop()

	WaitAny(monitorDone, config.RoundTimeout+2*time.Second)

	WaitGroupUpTo(&background, 2*time.Second)

	s.logger.LogLine("[STOP]")
}

func WaitGroupUpTo(wg *sync.WaitGroup, timeout time.Duration) {
	wgDoneCh := make(chan struct{})
	go func() {
		wg.Wait()
		close(wgDoneCh)
	}()
	select {
	case <-wgDoneCh:
	case <-time.After(timeout):
	}
}

func WaitAny(wgDoneCh chan struct{}, timeout time.Duration) {
	select {
	case <-wgDoneCh:
	case <-time.After(timeout):
	}
}

func (s *Session) preflight() error {
	ctx, cancel := context.WithTimeout(context.Background(), preflightTimeout)
	defer cancel()
	for _, host := range s.config.Hosts {
		if _, _, _, err := s.probeHost(ctx, host, false); err != nil {
			return fmt.Errorf("round preflight failed for %s: %w", host, err)
		}
	}
	if s.config.IPCheckInterval > 0 {
		if err := s.checkIP(ctx, nil); err != nil {
			return fmt.Errorf("ip preflight failed: %w", err)
		}
	}
	return nil
}

func (s *Session) runRound(parent context.Context) ProbeOutcome {
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
		workers.Go(func() {
			hostCtx, hostCancel := context.WithTimeout(ctx, s.config.RoundTimeout)
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
		})
	}

	for _, host := range s.config.Hosts {
		launchWorker(host)
	}

	var roundErr error
	stopped := false

	s.roundControl.Clear()

collect:
	for received := 0; received < len(s.config.Hosts); received++ {
		select {
		case <-parent.Done():
			cancel()
			WaitGroupUpTo(&workers, s.config.RoundTimeout+time.Second)
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

			s.roundControl.AddHostResult(res.host, res.latency, res.ip, res.usedCachedIP)

			if s.roundControl.EarlyRoundDecision() {
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
			waitNext: s.config.RoundRetryInterval,
		}
	}

	avg := averageLatencyMs(s.roundControl.latencies)
	detail := "[SUCCESS] " + strconv.FormatInt(avg, 10) + "ms (" + s.roundControl.strbResult.String() + ")"

	return ProbeOutcome{
		kind:     ProbeOutcomeKindSuccess,
		avgMs:    avg,
		detail:   detail,
		waitNext: s.config.RoundInterval,
	}
}

func (s *Session) probeHost(ctx context.Context, host string, useCache bool) (time.Duration, string, bool, error) {
	target, usedCachedIP := s.targetForHost(host, useCache)

	start := time.Now()
	conn, err := dialTCP(ctx, target)
	if err != nil {
		if !usedCachedIP && net.ParseIP(target) != nil {
			return 0, "", false, err
		}
		resolved, resolveErr := resolve(ctx, host, s.config.DNSServers)
		if resolveErr != nil {
			return 0, "", false, resolveErr
		}
		conn, err = dialTCP(ctx, resolved)
		if err != nil {
			return 0, "", false, err
		}
		target = resolved
		usedCachedIP = false

		// if usedCachedIP {
		// 	resolved, resolveErr := s.resolve(ctx, host)
		// 	if resolveErr != nil {
		// 		return 0, "", false, resolveErr
		// 	}
		// 	conn, err = dialTCP(ctx, resolved)
		// 	if err != nil {
		// 		return 0, "", false, err
		// 	}
		// 	target = resolved
		// 	usedCachedIP = false
		// } else if net.ParseIP(target) == nil {
		// 	resolved, resolveErr := s.resolve(ctx, host)
		// 	if resolveErr != nil {
		// 		return 0, "", false, resolveErr
		// 	}
		// 	conn, err = dialTCP(ctx, resolved)
		// 	if err != nil {
		// 		return 0, "", false, err
		// 	}
		// 	target = resolved
		// } else {
		// 	return 0, "", usedCachedIP, err
		// }
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

func (s *Session) applyCacheAfterRound(outcome ProbeOutcome) {
	s.cacheMu.Lock()
	defer s.cacheMu.Unlock()

	if outcome.kind != ProbeOutcomeKindSuccess {
		s.caches = make(map[string]*HostCache, len(s.config.Hosts))
		return
	}

	for _, host := range s.config.Hosts {
		ip := s.roundControl.successIPs[host]
		entry := s.caches[host]
		if entry == nil {
			entry = &HostCache{}
			s.caches[host] = entry
		}

		entry.ip = ip
		if s.roundControl.usedCachedIP[host] {
			entry.consecutiveUses++
			continue
		}
		entry.consecutiveUses = 0
	}
}

func averageLatencyMs(latencies []time.Duration) int64 {
	if len(latencies) == 0 {
		return 0
	}
	var total time.Duration
	for _, d := range latencies {
		total += d
	}
	return total.Milliseconds() / int64(len(latencies))
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

func (r *RoundControl) AddHostResult(host string, latency time.Duration, ip string, usedCachedIP bool) {
	r.latencies = append(r.latencies, latency)
	r.successIPs[host] = ip
	r.usedCachedIP[host] = usedCachedIP
	r.strbResult.WriteString(host)
	if !usedCachedIP {
		r.strbResult.WriteString("*")
	}
	r.strbResult.WriteString(":" + strconv.FormatInt(latency.Milliseconds(), 10) + "ms ")
}

func (r *RoundControl) Clear() {
	r.latencies = r.latencies[:0]
	r.strbResult.Reset()
	for k := range r.successIPs {
		delete(r.successIPs, k)
	}
	for k := range r.usedCachedIP {
		delete(r.usedCachedIP, k)
	}
}

func (r *RoundControl) EarlyRoundDecision() bool {
	if r.minRequired == 0 {
		return false
	}
	return len(r.latencies) >= r.minRequired
}
