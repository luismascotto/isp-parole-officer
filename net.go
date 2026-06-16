package main

import (
	"context"
	"errors"
	"fmt"
	"net"
)

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
