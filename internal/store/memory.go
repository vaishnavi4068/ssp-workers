package store

import (
	"sync"
)

// Memory is the in-process twin used when real DB/GCS/K8s is disabled (unit/replay tests).
type Memory struct {
	mu sync.Mutex

	StateUnits map[string]StateUnit
	nextRev    int
}

type StateUnit struct {
	ID        string
	ProjectID string
	Status    string // active | frozen
}

func NewMemory() *Memory {
	return &Memory{StateUnits: map[string]StateUnit{}}
}

func (m *Memory) EnsureStateUnit(id, projectID, status string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if status == "" {
		status = "active"
	}
	m.StateUnits[id] = StateUnit{ID: id, ProjectID: projectID, Status: status}
}

func (m *Memory) GetStateUnit(id, projectID string) (StateUnit, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if su, ok := m.StateUnits[id]; ok {
		return su, true
	}
	for _, su := range m.StateUnits {
		if projectID != "" && su.ProjectID == projectID {
			return su, true
		}
	}
	return StateUnit{}, false
}

func (m *Memory) NextRevision() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.nextRev++
	return m.nextRev
}

// Default is the global test store (Python DEFAULT_STORE analogue).
var Default = NewMemory()
