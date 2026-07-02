package main

// A simple example that shows how to send messages to a Bubble Tea program
// from outside the program using Program.Send(Msg).

import (
	"strings"

	"charm.land/bubbles/v2/spinner"
	tea "charm.land/bubbletea/v2"
	"charm.land/lipgloss/v2"
)

var (
	appStyle            = lipgloss.NewStyle().Margin(1, 2, 0, 2)
	spinnerRunningStyle = lipgloss.NewStyle().Foreground(lipgloss.Color("154"))
	spinnerWaitingStyle = lipgloss.NewStyle().Foreground(lipgloss.Color("220"))
	helpStyle           = lipgloss.NewStyle().Foreground(lipgloss.Color("241")).Width(64).Margin(1, 0).Align(lipgloss.Center)
	dotStyle            = helpStyle.UnsetMargins()
	messageStyle        = lipgloss.NewStyle().Foreground(lipgloss.Color("194"))
	messageAltStyle     = lipgloss.NewStyle().Foreground(lipgloss.Color("195"))
	messageErrorStyle   = lipgloss.NewStyle().Foreground(lipgloss.Color("197"))
)

type resultMsg struct {
	line string
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
			spinner.WithStyle(spinnerRunningStyle),
			spinner.WithSpinner(spinner.Jump),
		),
		spinnerWaiting: spinner.New(
			spinner.WithStyle(spinnerWaitingStyle),
			spinner.WithSpinner(spinner.Meter),
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
		case "q", "esc":
			//cancelGlobalContext()
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
		b.WriteString(m.spinnerWaiting.View())
		b.WriteString(spinnerWaitingStyle.Render(" Stopping..."))
		b.WriteString(dotStyle.Render(strings.Repeat("\n", numLastResults/2)))
	} else {
		if m.running {
			b.WriteString(m.spinnerRunning.View())
			b.WriteString(spinnerRunningStyle.Render(" Running..."))
		} else {
			b.WriteString(m.spinnerWaiting.View())
			b.WriteString(spinnerWaitingStyle.Render(" Waiting..."))
		}

		b.WriteString("\n\n")

		for i, res := range m.results {
			if len(res.line) == 0 {
				b.WriteString(messageStyleFor(i).Render(strings.Repeat(" ", 64)))
			} else {
				b.WriteString(messageStyleFor(i).Render(res.line))
			}
			b.WriteString("\n")
		}

		b.WriteString(helpStyle.Render("ESC, or Q to exit..."))
	}

	return tea.NewView(appStyle.Render(b.String()))
}

func messageStyleFor(line int) lipgloss.Style {
	if line%2 == 0 {
		return messageStyle
	}
	return messageAltStyle
}
