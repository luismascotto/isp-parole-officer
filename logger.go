package main

import (
	"fmt"
	"os"
	"path/filepath"
	"time"
)

func newHourlyLogger(sessionID string) (*HourlyLogger, error) {
	wd, err := os.Getwd()
	if err != nil {
		return nil, err
	}
	l := &HourlyLogger{outputDir: filepath.Join(wd, resultsDirName, sessionID)}
	if err := os.MkdirAll(l.outputDir, 0755); err != nil {
		return nil, err
	}
	return l, nil
}

func (l *HourlyLogger) resultsFileName(t time.Time) string {
	return fmt.Sprintf("%s.txt", t.Format("2006-01-02_15"))
}

func (l *HourlyLogger) logLine(line string) {
	moment := time.Now()
	fmt.Println(moment.Format("2006-01-02 15:04:05"), line)
	err := l.WriteLine(moment, line)
	if err != nil {
		fmt.Fprintf(os.Stderr, "log file error: %v\n", err)
	}
}

func (l *HourlyLogger) WriteLine(moment time.Time, line string) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	name := l.resultsFileName(moment)
	f, err := os.OpenFile(filepath.Join(l.outputDir, name), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = fmt.Fprintln(f, moment.Format("2006-01-02 15:04:05"), line)
	return err
}
