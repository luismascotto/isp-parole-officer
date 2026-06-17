package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"time"
)

func (s *Session) probeHost(ctx context.Context, host string, useCache bool) (time.Duration, string, bool, error) {
	target, usedCachedIP := s.targetForHost(host, useCache)

	start := time.Now()
	conn, err := dialTCP(ctx, target)
	if err != nil {
		if !usedCachedIP && net.ParseIP(target) != nil {
			return 0, "", false, err
		}
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
	}
	_ = conn.Close()
	return time.Since(start), target, usedCachedIP, nil
}

func (s *Session) resolve(ctx context.Context, host string) (string, error) {
	if ip := net.ParseIP(host); ip != nil {
		return host, nil
	}

	var errs error
	for _, dns := range s.DNSResolvers {
		ips, err := dns.resolver.LookupIPAddr(ctx, host)
		if err != nil {
			errs = errors.Join(errs, err)
			continue
		}
		if len(ips) == 0 {
			errs = errors.Join(errs, fmt.Errorf("no records on %s", dns.server))
			continue
		}
		return ips[0].IP.String(), nil
	}
	return "", errs
}

func dialTCP(ctx context.Context, host string) (net.Conn, error) {
	var d net.Dialer
	return d.DialContext(ctx, "tcp", net.JoinHostPort(host, tcpProbePort))
}
