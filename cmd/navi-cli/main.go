// CLI for navi-kv cluster. Prompt for cluster commands, plus live log tabs.
package main

import (
	"context"
	"fmt"
	"os"
	"strings"

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
	searchMatchStyle = lipgloss.NewStyle().Foreground(lipgloss.Color("0")).Background(lipgloss.Color("221"))
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
	lines   []string
	vp      viewport.Model

	isSearch  bool
	sourceTab int
	query     string
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
	debug       bool

	logChan   chan logLine
	logCtx    context.Context
	logCancel context.CancelFunc

	killSelectActive bool
	killCursor       int
	killNodes        []killNodeInfo

	searchActive bool
	searchInput  textinput.Model
}

type killNodeInfo struct {
	node string
	port int
	up   bool
}

func initialModel() model {
	ti := textinput.New()
	ti.Placeholder = "command"
	ti.Prompt = promptStyle.Render("navi> ")
	ti.Focus()

	si := textinput.New()
	si.Placeholder = "search"
	si.Prompt = "/"

	return model{
		input:       ti,
		tabs:        []tabModel{{title: "Console"}, {title: "Errors/Warnings"}},
		searchInput: si,
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
	m.tabs[idx].lines = append(m.tabs[idx].lines, line)
	m.tabs[idx].content += line + "\n"
	m.tabs[idx].vp.SetContent(m.tabs[idx].content)
	m.tabs[idx].vp.GotoBottom()

	m.propagateToSearchTabs(idx, line)
}

func (m *model) propagateToSearchTabs(idx int, line string) {
	lower := strings.ToLower(line)
	for i := range m.tabs {
		t := &m.tabs[i]
		if !t.isSearch || t.sourceTab != idx {
			continue
		}
		if !strings.Contains(lower, strings.ToLower(t.query)) {
			continue
		}
		t.lines = append(t.lines, line)
		t.content += renderLine(line, t.query) + "\n"
		t.vp.SetContent(t.content)
		t.vp.GotoBottom()
	}
}

// renderLine highlights every case-insensitive occurrence of query in line.
func renderLine(line, query string) string {
	if query == "" {
		return line
	}
	lower := strings.ToLower(line)
	lq := strings.ToLower(query)
	idx := strings.Index(lower, lq)
	if idx == -1 {
		return line
	}
	var b strings.Builder
	for idx != -1 {
		b.WriteString(line[:idx])
		b.WriteString(searchMatchStyle.Render(line[idx : idx+len(query)]))
		line = line[idx+len(query):]
		lower = lower[idx+len(query):]
		idx = strings.Index(lower, lq)
	}
	b.WriteString(line)
	return b.String()
}

// openSearchTab spawns a new split tab right after source, containing only
// the lines of source that match query (highlighted), and wires it to keep
// tailing future lines appended to source.
func (m *model) openSearchTab(source int, query string) {
	if source < 0 || source >= len(m.tabs) || query == "" {
		return
	}
	insertAt := source + 1

	for i := range m.tabs {
		if m.tabs[i].isSearch && m.tabs[i].sourceTab >= insertAt {
			m.tabs[i].sourceTab++
		}
	}

	nt := tabModel{
		title:     fmt.Sprintf("%s /%s", m.tabs[source].title, query),
		isSearch:  true,
		sourceTab: source,
		query:     query,
	}
	q := strings.ToLower(query)
	var b strings.Builder
	for _, l := range m.tabs[source].lines {
		if strings.Contains(strings.ToLower(l), q) {
			nt.lines = append(nt.lines, l)
			b.WriteString(renderLine(l, query))
			b.WriteString("\n")
		}
	}
	nt.content = b.String()

	tabs := make([]tabModel, 0, len(m.tabs)+1)
	tabs = append(tabs, m.tabs[:insertAt]...)
	tabs = append(tabs, nt)
	tabs = append(tabs, m.tabs[insertAt:]...)
	m.tabs = tabs

	m.activeTab = insertAt
	m.resizeTabs()
	m.tabs[insertAt].vp.SetContent(nt.content)
	m.tabs[insertAt].vp.GotoBottom()
}

// closeSearchTab removes a search split tab and returns focus to its source.
func (m *model) closeSearchTab(idx int) {
	if idx < 0 || idx >= len(m.tabs) || !m.tabs[idx].isSearch {
		return
	}
	source := m.tabs[idx].sourceTab

	m.tabs = append(m.tabs[:idx], m.tabs[idx+1:]...)
	for i := range m.tabs {
		if m.tabs[i].isSearch && m.tabs[i].sourceTab > idx {
			m.tabs[i].sourceTab--
		}
	}

	if source > idx {
		source--
	}
	switch {
	case source < 0:
		source = 0
	case source >= len(m.tabs):
		source = len(m.tabs) - 1
	}
	m.activeTab = source
	m.resizeTabs()
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
	debug bool
	err   error
}

type nodeAddedMsg struct {
	index int
	port  int
	err   error
}

type clusterStoppedMsg struct {
	reset bool
	err   error
}

type commandResultMsg struct {
	lines []string
}

type killNodesLoadedMsg struct {
	nodes []killNodeInfo
}

type nodeKilledMsg struct {
	node string
	err  error
}

func (m model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.width, m.height = msg.Width, msg.Height
		m.resizeTabs()
		return m, nil

	case tea.KeyMsg:
		if m.killSelectActive {
			switch msg.Type {
			case tea.KeyUp:
				if m.killCursor > 0 {
					m.killCursor--
				}
			case tea.KeyDown:
				if m.killCursor < len(m.killNodes)-1 {
					m.killCursor++
				}
			case tea.KeyEnter:
				target := m.killNodes[m.killCursor]
				m.killSelectActive = false
				return m, func() tea.Msg {
					return nodeKilledMsg{node: target.node, err: doKill(target.port)}
				}
			case tea.KeyEsc, tea.KeyCtrlC:
				m.killSelectActive = false
				m.history = append(m.history, "kill-node cancelled")
			}
			return m, nil
		}

		if m.searchActive {
			switch msg.Type {
			case tea.KeyEnter:
				m.searchActive = false
				query := m.searchInput.Value()
				m.searchInput.SetValue("")
				m.openSearchTab(m.activeTab, query)
				return m, nil
			case tea.KeyEsc, tea.KeyCtrlC:
				m.searchActive = false
				m.searchInput.SetValue("")
				return m, nil
			}
			var cmd tea.Cmd
			m.searchInput, cmd = m.searchInput.Update(msg)
			return m, cmd
		}

		if m.activeTab != 0 && m.tabs[m.activeTab].isSearch && msg.Type == tea.KeyEsc {
			m.closeSearchTab(m.activeTab)
			return m, nil
		}

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
			if msg.String() == "/" {
				m.searchActive = true
				m.searchInput.SetValue("")
				m.searchInput.Focus()
				return m, textinput.Blink
			}
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

	case killNodesLoadedMsg:
		m.killSelectActive = true
		m.killNodes = msg.nodes
		m.killCursor = 0
		return m, nil

	case nodeKilledMsg:
		if msg.err != nil {
			m.history = append(m.history, errorStyle.Render(fmt.Sprintf("kill %s failed: %s", msg.node, msg.err)))
		} else {
			m.history = append(m.history, fmt.Sprintf("%s killed", msg.node))
		}
		return m, nil

	case nodeAddedMsg:
		if msg.err != nil {
			m.history = append(m.history, errorStyle.Render(fmt.Sprintf("add-node failed: %s", msg.err)))
			return m, nil
		}

		m.nodePorts = append(m.nodePorts, msg.port)

		node := fmt.Sprintf("node%d", msg.index)
		m.tabs = append(m.tabs, tabModel{title: node, node: node})
		m.resizeTabs()

		if m.logCtx != nil {
			container := fmt.Sprintf("kv-node%d", msg.index)
			go streamContainerLogs(m.logCtx, container, node, m.logChan)
		}

		m.history = append(m.history, fmt.Sprintf("%s added: port %d", node, msg.port))
		return m, nil

	case clusterStartedMsg:
		if msg.err != nil {
			m.history = append(m.history, errorStyle.Render(fmt.Sprintf("start-navi failed: %s", msg.err)))
			return m, nil
		}

		m.composePath = msg.path
		m.nodePorts = msg.ports
		m.debug = msg.debug

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
	if m.killSelectActive {
		s := welcomeStyle.Render("kill-node: select a node") + "\n\n"
		for i, n := range m.killNodes {
			state := "down"
			if n.up {
				state = "up"
			}
			line := fmt.Sprintf("%s :%d  %s", n.node, n.port, state)
			if i == m.killCursor {
				line = activeTabStyle.Render("> " + line)
			} else {
				line = inactiveTabStyle.Render("  " + line)
			}
			s += line + "\n"
		}
		s += "\n" + inactiveTabStyle.Render("up/down select  enter kill  esc cancel") + "\n"
		return s
	}

	var bar string
	for i, t := range m.tabs {
		style := inactiveTabStyle
		if i == m.activeTab {
			style = activeTabStyle
		}
		bar += style.Render(fmt.Sprintf(" %s ", t.title))
	}

	if m.activeTab != 0 {
		statusLine := ""
		switch {
		case m.searchActive:
			statusLine = m.searchInput.View()
		case m.tabs[m.activeTab].isSearch:
			statusLine = inactiveTabStyle.Render(fmt.Sprintf("filtered on /%s  (%d matches)  esc close",
				m.tabs[m.activeTab].query, len(m.tabs[m.activeTab].lines)))
		}
		return bar + "\n" + statusLine + "\n" + m.tabs[m.activeTab].vp.View()
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
