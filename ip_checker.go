package main

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"time"
)

func (s *Session) runIPChecker(ctx context.Context) {
	var lastIP string
	// checkIP := func() {
	// 	if ctx.Err() != nil {
	// 		return
	// 	}
	// 	checkCtx, cancel := context.WithTimeout(ctx, s.config.IPCheckTimeout)
	// 	defer cancel()

	// 	ip, err := s.fetchPublicIP(checkCtx, s.config.IPCheckURL)
	// 	if err != nil {
	// 		if ctx.Err() != nil {
	// 			return
	// 		}
	// 		s.logger.LogLine("[IP] ERROR " + err.Error())
	// 		return
	// 	}

	// 	if lastIP != "" && ip != lastIP {
	// 		s.logger.LogLine("[IP] changed " + lastIP + " -> " + ip)
	// 	} else {
	// 		s.logger.LogLine("[IP] " + ip)
	// 	}
	// 	lastIP = ip
	// }

	// checkIP()

	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(s.config.IPCheckInterval):
			_ = s.checkIP(ctx, &lastIP)
		}
	}
}

func (s *Session) checkIP(ctx context.Context, lastIP *string) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	checkCtx, cancel := context.WithTimeout(ctx, s.config.IPCheckTimeout)
	defer cancel()
	ip, err := s.fetchPublicIP(checkCtx, s.config.IPCheckURL)
	if err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		s.logger.LogLine("[IP] ERROR " + err.Error())
		return err
	}

	// Only update reference when necessary (valid ref empty or changed)
	if lastIP != nil && len(*lastIP) == 0 {
		*lastIP = ip
	}
	if lastIP == nil || *lastIP == ip {
		s.logger.LogLine("[IP] " + ip)
		return nil
	}

	s.logger.LogLine("[IP] changed " + *lastIP + " -> " + ip)
	*lastIP = ip

	return nil
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
