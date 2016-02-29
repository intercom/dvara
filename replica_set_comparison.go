package dvara

type ReplicaSetComparison struct {
	// Extra members that are in this state, but not in new state
	ExtraMembers map[string]*Proxy
	// Missing members that aren't in this state, but are in new
	MissingMembers map[string]*Proxy
}
