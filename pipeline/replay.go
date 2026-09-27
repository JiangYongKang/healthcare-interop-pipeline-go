package pipeline

// ReplayReport summarizes one consistency replay: the in-memory state
// was rebuilt from the append-only journals and compared against the
// incrementally maintained state.
type ReplayReport struct {
	Events     int  `json:"events"`
	Deliveries int  `json:"deliveries"`
	Pending    int  `json:"pending"`
	Reviews    int  `json:"reviews"`
	Mismatches int  `json:"mismatches"`
	Replaced   bool `json:"replaced"`
}
