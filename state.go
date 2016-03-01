package dvara

// ReplicaState is the state of a node in the replica.
//https://docs.mongodb.org/v2.6/reference/replica-states/
type ReplicaState string

const (
	// ReplicaStatePrimary indicates the node is a primary.
	ReplicaStatePrimary = ReplicaState("PRIMARY")

	// ReplicaStateSecondary indicates the node is a secondary.
	ReplicaStateSecondary = ReplicaState("SECONDARY")

	// ReplicaStateArbiter indicates the node is an arbiter.
	ReplicaStateArbiter = ReplicaState("ARBITER")

	// ReplicaStateRemoved indicates the node was removed from the ReplicaSet
	ReplicaStateRemoved = ReplicaState("REMOVED")

	// ReplicaStateStartup indicates the node is still starting up
	ReplicaStateStartup = ReplicaState("STARTUP")

	// ReplicaStateStartup2 indicates the node is still starting up
	//Cannot vote. Forks replication and election threads before becoming a secondary.
	ReplicaStateStartup2 = ReplicaState("STARTUP2")

	// Replica is trying to figure out its state. Who am I it says?
	ReplicaStateUnknown = ReplicaState("UNKNOWN")
)
