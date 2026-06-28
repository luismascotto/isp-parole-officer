package main

// A simple example that shows how to send messages to a Bubble Tea program
// from outside the program using Program.Send(Msg).

import (
	"fmt"
	"math/rand"
	"os"
	"strings"
	"time"

	"charm.land/bubbles/v2/spinner"
	tea "charm.land/bubbletea/v2"
	"charm.land/lipgloss/v2"
)

var (
	spinnerStyle  = lipgloss.NewStyle().Foreground(lipgloss.Color("63"))
	helpStyle     = lipgloss.NewStyle().Foreground(lipgloss.Color("241")).Margin(1, 0)
	dotStyle      = helpStyle.UnsetMargins()
	durationStyle = dotStyle
	appStyle      = lipgloss.NewStyle().Margin(1, 2, 0, 2)
	messageStyle  = lipgloss.NewStyle().Foreground(lipgloss.Color("#FFFFFF"))
)

type resultMsg struct {
	line string
}

func (r resultMsg) String() string {
	// if r.duration == 0 {
	// 	return dotStyle.Render(strings.Repeat(".", 30))
	// }
	// return fmt.Sprintf("🍔 Ate %s %s", r.food,
	// 	durationStyle.Render(r.duration.String()))
	return messageStyle.Render(r.line)
}

type runningMsg struct {
	running bool
}

type model struct {
	spinnerRunning spinner.Model
	spinnerWaiting spinner.Model
	results        []resultMsg
	quitting       bool
	running        bool
}

const numLastResults = 20

func newModel() model {

	return model{
		spinnerRunning: spinner.New(
			spinner.WithStyle(spinnerStyle),
			spinner.WithSpinner(spinner.Jump),
		),
		spinnerWaiting: spinner.New(
			spinner.WithStyle(spinnerStyle),
			spinner.WithSpinner(spinner.Monkey),
		),
		results: make([]resultMsg, numLastResults),
	}
}

func (m model) Init() tea.Cmd {
	return tea.Batch(m.spinnerRunning.Tick, m.spinnerWaiting.Tick)
}

func (m model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	var cmd tea.Cmd
	var cmds []tea.Cmd
	switch msg := msg.(type) {
	case tea.KeyMsg:
		switch msg.String() {
		case "q", "esc", "ctrl+c":
			m.quitting = true
			return m, tea.Quit
		}
		return m, nil
	case runningMsg:
		m.running = msg.running
		return m, nil
	case resultMsg:
		m.results = append(m.results[1:], msg)
		//m.running = false
		return m, nil
	case spinner.TickMsg:
		m.spinnerRunning, cmd = m.spinnerRunning.Update(msg)
		if cmd != nil {
			cmds = append(cmds, cmd)
		}
		m.spinnerWaiting, cmd = m.spinnerWaiting.Update(msg)
		if cmd != nil {
			cmds = append(cmds, cmd)
		}
		return m, tea.Batch(cmds...)
	default:
		return m, nil
	}
}

func (m model) View() tea.View {
	var b strings.Builder

	if m.quitting {
		b.WriteString("SIUUUUUU!")
		b.WriteString(dotStyle.Render(strings.Repeat("\n", numLastResults+3)))
	} else {
		if m.running {
			b.WriteString(m.spinnerRunning.View())
			b.WriteString(" Running...")
		} else {
			b.WriteString(m.spinnerWaiting.View())
			b.WriteString(" Waiting...")
		}

		b.WriteString("\n\n")

		for _, res := range m.results {
			b.WriteString(messageStyle.Render(res.line))
			b.WriteString("\n")
		}

		b.WriteString(helpStyle.Render("Ctrl+C, ESC, or Q to exit..."))
	}

	return tea.NewView(appStyle.Render(b.String()))
}

func main_view() {
	p := tea.NewProgram(newModel())

	// Simulate activity
	go func() {
		for {
			pause := time.Duration(rand.Int63n(899)+100) * time.Millisecond // nolint:gosec
			time.Sleep(pause)

			// Send the Bubble Tea program a message from outside the
			// tea.Program. This will block until it is ready to receive
			// messages.
			p.Send(resultMsg{line: randomFood()})
		}
	}()

	if _, err := p.Run(); err != nil {
		fmt.Println("Error running program:", err)
		os.Exit(1)
	}
}

func randomFood() string {
	food := []string{
		"an apple", "a pear", "a gherkin", "a party gherkin",
		"a kohlrabi", "some spaghetti", "tacos", "a currywurst", "some curry",
		"a sandwich", "some peanut butter", "some cashews", "some ramen",
	}
	return food[rand.Intn(len(food))] // nolint:gosec
}
