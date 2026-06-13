package main

import (
	"context"
	"fmt"
	"net"
)

func resolve(ctx context.Context, host string, DNSServers []string) (string, error) {
	if ip := net.ParseIP(host); ip != nil {
		return host, nil
	}

	if len(DNSServers) == 0 {
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
	for _, server := range DNSServers {
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
