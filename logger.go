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
	now := time.Now()
	l := &HourlyLogger{
		outputDir: filepath.Join(wd, resultsDirName, sessionID),
		currHour:  now.Hour(),
	}
	l.strb.Grow(128)

	// Write filesystem errors and move on
	if err := os.MkdirAll(l.outputDir, 0755); err != nil {
		fmt.Fprintln(os.Stderr, "log directory creation error:", err)
	}
	l.prepareResultsFile(now)
	return l, nil
}

func (l *HourlyLogger) prepareResultsFile(moment time.Time) {
	if l.file != nil {
		l.file.Close()
		l.file = nil
	}

	f, err := os.OpenFile(
		filepath.Join(l.outputDir, moment.Format(resultFileNameFormat)+resultFileExtension),
		os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		fmt.Fprintln(os.Stderr, "log file open error:", err)
	}
	l.file = f
}

func (l *HourlyLogger) LogLine(line string) {
	l.mu.Lock()
	defer l.mu.Unlock()

	moment := time.Now()

	// Clear terminal and prepare new results file if hour has changed.
	//  In long running sessions, some terminals can crash due buffer overflow.
	if l.currHour != moment.Hour() {
		l.currHour = moment.Hour()

		os.Stdout.WriteString("\x1bc")
		l.prepareResultsFile(moment)
	}

	l.strb.Reset()
	l.strb.WriteString(moment.Format("2006-01-02 15:04:05"))
	l.strb.WriteString(" ")
	l.strb.WriteString(line)

	timestampedLine := l.strb.String()

	fmt.Println(timestampedLine)

	if l.file != nil {
		if w, err := fmt.Fprintln(l.file, timestampedLine); err != nil {
			fmt.Fprintln(os.Stderr, "log file write error:", err, "\nBytes written:", w)
		}
	}
}
