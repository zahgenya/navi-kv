// CLI for navi-kv cluster. Prompt for cluster commands, plus live log tabs.
package main

import (
	"context"
	"fmt"
	"os"

	"github.com/charmbracelet/bubbles/textinput"
	"github.com/charmbracelet/bubbles/viewport"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
)

var (
	promptStyle      = lipgloss.NewStyle().Foreground(lipgloss.Color("#88B04B")).Bold(true)
	errorStyle       = lipgloss.NewStyle().Foreground(lipgloss.Color("196"))
	welcomeStyle     = lipgloss.NewStyle().Foreground(lipgloss.Color("74")).Bold(true)
	activeTabStyle   = lipgloss.NewStyle().Foreground(lipgloss.Color("74")).Bold(true).Underline(true)
	inactiveTabStyle = lipgloss.NewStyle().Foreground(lipgloss.Color("240"))
)

const asciiLogo = `
                     ___
                     0@@
                     ___
          __    _yg@@@@@@@gy_    __
        yg@@@ y$@@@PF~~~~4@@@@y 4@@@y
      a@@@F~ g@@P~         ~4@@$,~?@@@g_
    y@@P~   g@@F    _agg_    ~@@@   ~4@@g
   @@@~     @@$    $@@@@@@,   4@@L    '$@@,
  4@@$     4@@l   4@@@@@@@F   J@@$     j@@$
   @@@y     @@$    4@@@@@P    a@@F    y@@@'
    ~@@@y_  4@@$    '4~~'    a@@F  _y$@@F
      ~@@@gy ~@@@y_       _y@@@F _g@@@F
    $@$ ~?@@F '!@@@@g   a@@@@F'  4@F~ a@@
    *~*          ~9@@   @@@~           ~~
                  J@@   @@$
           _      J@@   @@[      _
          0@@_    a@@   @@$_   _g@@
           ~@@@@@@@@~   '@@@@@@@@F
             ~~~~'       '~~~~''
`

var welcomeMessage = fmt.Sprintf("NaVi CLI\nRaft conseunsus implementation\n%s\ntype `help` for commands, or `quit` to exit", asciiLogo)

type tabModel struct {
	title   string
	node    string
	content string
	vp      viewport.Model
}

type model struct {
	input   textinput.Model
	history []string

	tabs      []tabModel
	activeTab int
	width     int
	height    int

	composePath string
	nodePorts   []int

	logChan   chan logLine
	logCtx    context.Context
	logCancel context.CancelFunc
}

func initialModel() model {
	ti := textinput.New()
	ti.Placeholder = "command"
	ti.Prompt = promptStyle.Render("navi> ")
	ti.Focus()

	return model{
		input: ti,
		tabs:  []tabModel{{title: "Console"}, {title: "Errors/Warnings"}},
	}
}

func (m model) Init() tea.Cmd {
	return textinput.Blink
}

func (m model) errorsTabIndex() int { return 1 }

func (m model) nodeTabIndex(node string) int {
	for i, t := range m.tabs {
		if t.node == node {
			return i
		}
	}
	return -1
}

func (m *model) appendToTab(idx int, line string) {
	if idx < 0 || idx >= len(m.tabs) {
		return
	}
	m.tabs[idx].content += line + "\n"
	m.tabs[idx].vp.SetContent(m.tabs[idx].content)
	m.tabs[idx].vp.GotoBottom()
}

func (m *model) resizeTabs() {
	h := m.height - 2
	if h < 0 {
		h = 0
	}
	for i := range m.tabs {
		m.tabs[i].vp.Width = m.width
		m.tabs[i].vp.Height = h
	}
}

func waitForLog(ctx context.Context, ch <-chan logLine) tea.Cmd {
	return func() tea.Msg {
		select {
		case line, ok := <-ch:
			if !ok {
				return nil
			}
			return logLineMsg(line)
		case <-ctx.Done():
			return nil
		}
	}
}

type logLineMsg logLine

type clusterStartedMsg struct {
	path  string
	ports []int
	err   error
}

type clusterStoppedMsg struct {
	reset bool
	err   error
}

type commandResultMsg struct {
	lines []string
}

func (m model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.width, m.height = msg.Width, msg.Height
		m.resizeTabs()
		return m, nil

	case tea.KeyMsg:
		switch msg.Type {
		case tea.KeyCtrlC, tea.KeyEsc:
			if m.logCancel != nil {
				m.logCancel()
			}
			return m, tea.Quit
		case tea.KeyTab:
			m.activeTab = (m.activeTab + 1) % len(m.tabs)
			return m, nil
		case tea.KeyShiftTab:
			m.activeTab = (m.activeTab - 1 + len(m.tabs)) % len(m.tabs)
			return m, nil
		}

		if m.activeTab != 0 {
			var cmd tea.Cmd
			m.tabs[m.activeTab].vp, cmd = m.tabs[m.activeTab].vp.Update(msg)
			return m, cmd
		}

		if msg.Type == tea.KeyEnter {
			line := m.input.Value()
			m.input.SetValue("")

			if line == "" {
				return m, nil
			}
			m.history = append(m.history, promptStyle.Render("navi> ")+line)

			return runCommand(m, line)
		}

	case commandResultMsg:
		m.history = append(m.history, msg.lines...)
		return m, nil

	case clusterStartedMsg:
		if msg.err != nil {
			m.history = append(m.history, errorStyle.Render(fmt.Sprintf("start-navi failed: %s", msg.err)))
			return m, nil
		}

		m.composePath = msg.path
		m.nodePorts = msg.ports

		m.tabs = []tabModel{{title: "Console"}, {title: "Errors/Warnings"}}
		for i := 1; i <= len(msg.ports); i++ {
			node := fmt.Sprintf("node%d", i)
			m.tabs = append(m.tabs, tabModel{title: node, node: node})
		}
		m.resizeTabs()

		ctx, cancel := context.WithCancel(context.Background())
		m.logCtx = ctx
		m.logCancel = cancel
		m.logChan = make(chan logLine, 256)

		for i := 1; i <= len(msg.ports); i++ {
			container := fmt.Sprintf("kv-node%d", i)
			node := fmt.Sprintf("node%d", i)
			go streamContainerLogs(ctx, container, node, m.logChan)
		}

		m.history = append(m.history, fmt.Sprintf("cluster started: %d node(s), ports %v", len(msg.ports), msg.ports))
		return m, waitForLog(ctx, m.logChan)

	case clusterStoppedMsg:
		if msg.err != nil {
			m.history = append(m.history, errorStyle.Render(fmt.Sprintf("stop failed: %s", msg.err)))
			return m, nil
		}

		if m.logCancel != nil {
			m.logCancel()
		}
		m.logChan = nil
		m.logCtx = nil
		m.logCancel = nil
		m.composePath = ""
		m.nodePorts = nil
		m.tabs = []tabModel{{title: "Console"}, {title: "Errors/Warnings"}}
		m.activeTab = 0
		m.resizeTabs()

		verb := "stopped"
		if msg.reset {
			verb = "reset"
		}
		m.history = append(m.history, fmt.Sprintf("cluster %s", verb))
		return m, nil

	case logLineMsg:
		if m.logChan == nil {
			return m, nil
		}

		idx := m.nodeTabIndex(msg.Node)
		m.appendToTab(idx, msg.Text)

		if msg.Level == "WARN" || msg.Level == "ERROR" {
			m.appendToTab(m.errorsTabIndex(), fmt.Sprintf("[%s] %s", msg.Node, msg.Text))
		}

		return m, waitForLog(m.logCtx, m.logChan)
	}

	var cmd tea.Cmd
	m.input, cmd = m.input.Update(msg)
	return m, cmd
}

func (m model) View() string {
	var bar string
	for i, t := range m.tabs {
		style := inactiveTabStyle
		if i == m.activeTab {
			style = activeTabStyle
		}
		bar += style.Render(fmt.Sprintf(" %s ", t.title))
	}

	if m.activeTab != 0 {
		return bar + "\n\n" + m.tabs[m.activeTab].vp.View()
	}

	s := bar + "\n\n" + welcomeStyle.Render(welcomeMessage) + "\n\n"
	for _, line := range m.history {
		s += line + "\n"
	}
	s += m.input.View() + "\n"
	return s
}

func main() {
	p := tea.NewProgram(initialModel(), tea.WithAltScreen())
	if _, err := p.Run(); err != nil {
		fmt.Fprintf(os.Stderr, "error: %s\n", err)
		os.Exit(1)
	}
}
