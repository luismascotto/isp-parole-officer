package main

import (
	"context"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

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
			WaitWgUpTo(&workers, s.config.RoundTimeout+time.Second)
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
		return ProbeOutcome{kind: ProbeOutcomeKindStopped}
	}

	var outcome ProbeOutcome
	if roundErr != nil {
		outcome = ProbeOutcome{
			kind:   ProbeOutcomeKindError,
			err:    roundErr,
			detail: "[ERROR] " + describeError(roundErr),
		}
	} else {
		avgMs := averageLatencyMs(s.roundControl.latencies)
		outcome = ProbeOutcome{
			kind:   ProbeOutcomeKindSuccess,
			avgMs:  avgMs,
			detail: "[SUCCESS] " + strconv.FormatInt(avgMs, 10) + "ms (" + s.roundControl.strbResult.String() + ")",
		}
	}
	s.applyCacheAfterRound(outcome)

	return outcome
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
	switch {
	case strings.Contains(msg, "resolve"):
		return msg
	case strings.Contains(msg, "timeout"), strings.Contains(msg, "i/o timeout"):
		return "Timeout"
	case strings.Contains(msg, "connection refused"):
		return "Connection refused"
	case strings.Contains(msg, "no such host"):
		return "Couldn't resolve name"
	default:
		return msg
	}
}

func (r *RoundControl) AddHostResult(host string, latency time.Duration, ip string, usedCachedIP bool) {
	if len(r.latencies) > 0 {
		r.strbResult.WriteString(" ")
	}
	r.latencies = append(r.latencies, latency)
	r.successIPs[host] = ip
	r.usedCachedIP[host] = usedCachedIP
	r.strbResult.WriteString(host)
	if !usedCachedIP {
		r.strbResult.WriteString("*")
	}
	r.strbResult.WriteString(":" + strconv.FormatInt(latency.Milliseconds(), 10) + "ms")
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
