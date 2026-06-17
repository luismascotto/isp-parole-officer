package main

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"time"
)

func (s *Session) runIPChecker(ctx context.Context) {
	var lastIP string

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
	ip, err := s.fetchPublicIP(checkCtx)
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

func (s *Session) fetchPublicIP(ctx context.Context) (string, error) {
	req := s.IPChecker.req.WithContext(ctx)

	resp, err := s.IPChecker.httpClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("service returned HTTP %d", resp.StatusCode)
	}
	if resp.ContentLength > maxIPCheckResponse {
		return "", fmt.Errorf("response is too large")
	}
	_, err = io.ReadAtLeast(resp.Body, s.IPChecker.bodyResp, int(resp.ContentLength))
	if err != nil {
		return "", fmt.Errorf("failed to read response body: %w", err)
	}

	ip := string(s.IPChecker.bodyResp[:resp.ContentLength])
	if parsed := net.ParseIP(ip); parsed == nil {
		return "", fmt.Errorf("invalid IP in response: %q", ip)
	}
	return ip, nil
}
