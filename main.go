package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"sync"
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
	// curl -s http://localhost:6060/debug/pprof/heap > base.prof
	// go tool pprof -diff base.prof main.prof
	// go func() {
	// 	log.Println(http.ListenAndServe("localhost:6060", nil))
	// }()

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

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	logger, err := newHourlyLogger(ctx, sessionID, config.UseTUI)
	if err != nil {
		fmt.Fprintf(os.Stderr, "log file error: %v\n", err)
		os.Exit(1)
	}

	s := &Session{
		sessionID:    sessionID,
		config:       config,
		caches:       make(map[string]*HostCache, len(config.Hosts)),
		logger:       logger,
		roundControl: config.newRoundControl(),
		DNSResolvers: config.newDNSResolvers(),
		IPChecker:    config.newIPChecker(),
	}

	var IPCheckWg sync.WaitGroup
	doneCh := make(chan struct{})
	go s.RunApplication(ctx, &IPCheckWg, doneCh)

	if s.config.UseTUI {
		_, _ = s.logger.tuiProgram.Run()
	}
	cancel()
	WaitDoneCh(doneCh, s.config.RoundTimeout+2*time.Second)
	WaitWgUpTo(&IPCheckWg, 2*time.Second)
}

func (s *Session) RunApplication(ctx context.Context, IPCheckWg *sync.WaitGroup, doneCh chan struct{}) {
	s.setRunning(true)
	s.logger.LogLine("[CONFIG]")
	configs := strings.SplitSeq(s.config.formatConfig(), "\n")
	for config := range configs {
		s.logger.LogLine(config)
	}

	if err := s.preflight(); err != nil {
		fmt.Fprintf(os.Stderr, "%v\n%s\n", err, "Review and adjust config.json, then run again.")
		os.Exit(1)
	}

	s.logger.LogLine("[START]")

	// ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	// defer stop()

	//var IPCheckWg sync.WaitGroup
	if s.config.IPCheckInterval > 0 {
		IPCheckWg.Go(func() {
			s.runIPChecker(ctx)
		})
	}

	go func() {
		defer close(doneCh)
		waitInterval := s.config.RoundInterval
		for {
			s.setRunning(false)
			if !SleepOrStop(ctx, waitInterval) {
				return
			}
			s.setRunning(true)
			outcome := s.runRound(ctx)

			if ctx.Err() != nil {
				return
			}
			switch outcome.kind {
			case ProbeOutcomeKindStopped:
				return
			case ProbeOutcomeKindError:
				waitInterval = s.config.RoundRetryInterval
			default:
				waitInterval = s.config.RoundInterval
			}
			s.logger.LogLine(outcome.detail)
		}
	}()

	<-ctx.Done()

	s.logger.LogLine("[STOP]")
}

func WaitWgUpTo(wg *sync.WaitGroup, d time.Duration) {
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	WaitDoneCh(done, d)
}

func WaitDoneCh(done chan struct{}, d time.Duration) {
	select {
	case <-done:
	case <-time.After(d):
	}
}

func SleepOrStop(ctx context.Context, d time.Duration) bool {
	if ctx.Err() != nil {
		return false
	}
	if d <= 0 {
		return true
	}

	select {
	case <-ctx.Done():
		return false
	case <-time.After(d):
		return true
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

func (s *Session) setRunning(running bool) {
	if !s.logger.useTUI {
		return
	}
	s.logger.tuiProgram.Send(runningMsg{running: running})
}
