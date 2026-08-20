package main

import (
	"fmt"

	tea "github.com/charmbracelet/bubbletea"
)

func (m model) updateKillSelect(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
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
		m.mode = modeNormal
		return m, func() tea.Msg {
			return nodeKilledMsg{node: target.node, err: doKill(target.port)}
		}
	case tea.KeyEsc, tea.KeyCtrlC:
		m.mode = modeNormal
		m.history = append(m.history, "kill-node cancelled")
	}
	return m, nil
}

func (m model) viewKillSelect() string {
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

func (m model) updateSearchInput(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	switch msg.Type {
	case tea.KeyEnter:
		m.mode = modeNormal
		query := m.searchInput.Value()
		m.searchInput.SetValue("")
		m.openSearchTab(m.activeTab, query)
		return m, nil
	case tea.KeyEsc, tea.KeyCtrlC:
		m.mode = modeNormal
		m.searchInput.SetValue("")
		return m, nil
	}
	var cmd tea.Cmd
	m.searchInput, cmd = m.searchInput.Update(msg)
	return m, cmd
}
