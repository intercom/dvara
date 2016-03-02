package dvara

import (
	"testing"

	"github.com/facebookgo/mgotest"
)

func TestFilterRSStatus(t *testing.T) {
	t.Parallel()
	cases := []struct {
		Name string
		A    *replSetGetStatusResponse
		B    *replSetGetStatusResponse
	}{
		{
			Name: "Only stable members",
			A: &replSetGetStatusResponse{
				Members: []statusMember{
					{Name: "a", State: ReplicaStatePrimary},
				},
			},
			B: &replSetGetStatusResponse{
				Members: []statusMember{
					{Name: "a", State: ReplicaStatePrimary},
				},
			},
		},
		{
			Name: "Removing startup2 member",
			A: &replSetGetStatusResponse{
				Members: []statusMember{
					{Name: "a", State: ReplicaStatePrimary},
					{Name: "c", State: ReplicaStateStartup2},
				},
			},
			B: &replSetGetStatusResponse{
				Members: []statusMember{
					{Name: "a", State: ReplicaStatePrimary},
				},
			},
		},
		{
			Name: "both nil",
		},
		{
			Name: "A nil B empty",
			B:    &replSetGetStatusResponse{},
		},
		{
			Name: "A empty B nil",
			A:    &replSetGetStatusResponse{},
		},
	}

	for _, c := range cases {
		response, err := filterReplGetStatus(c.A, nil)
		if !sameRSMembers(response, c.B) || err != nil {
			t.Fatalf("failed %s with input %s response %s expected response %s", c.Name, c.A, response, c.B)
		}
	}
}

func TestSameRSMembers(t *testing.T) {
	t.Parallel()
	cases := []struct {
		Name string
		A    *replSetGetStatusResponse
		B    *replSetGetStatusResponse
	}{
		{
			Name: "the same",
			A: &replSetGetStatusResponse{
				Members: []statusMember{
					{Name: "a", State: "b"},
				},
			},
			B: &replSetGetStatusResponse{
				Members: []statusMember{
					{Name: "a", State: "b"},
				},
			},
		},
		{
			Name: "out of order",
			A: &replSetGetStatusResponse{
				Members: []statusMember{
					{Name: "a", State: "b"},
					{Name: "c", State: "d"},
				},
			},
			B: &replSetGetStatusResponse{
				Members: []statusMember{
					{Name: "c", State: "d"},
					{Name: "a", State: "b"},
				},
			},
		},
		{
			Name: "both nil",
		},
		{
			Name: "A nil B empty",
			B:    &replSetGetStatusResponse{},
		},
		{
			Name: "A empty B nil",
			A:    &replSetGetStatusResponse{},
		},
	}

	for _, c := range cases {
		if !sameRSMembers(c.A, c.B) {
			t.Fatalf("failed %s", c.Name)
		}
	}
}

func TestNotSameRSMembers(t *testing.T) {
	t.Parallel()
	cases := []struct {
		Name string
		A    *replSetGetStatusResponse
		B    *replSetGetStatusResponse
	}{
		{
			Name: "different name",
			A: &replSetGetStatusResponse{
				Members: []statusMember{
					{Name: "a", State: "b"},
				},
			},
			B: &replSetGetStatusResponse{
				Members: []statusMember{
					{Name: "b", State: "b"},
				},
			},
		},
		{
			Name: "different state",
			A: &replSetGetStatusResponse{
				Members: []statusMember{
					{Name: "a", State: "b"},
				},
			},
			B: &replSetGetStatusResponse{
				Members: []statusMember{
					{Name: "a", State: "c"},
				},
			},
		},
		{
			Name: "subset A",
			A: &replSetGetStatusResponse{
				Members: []statusMember{
					{Name: "a", State: "b"},
					{Name: "b", State: "c"},
				},
			},
			B: &replSetGetStatusResponse{
				Members: []statusMember{
					{Name: "a", State: "b"},
				},
			},
		},
		{
			Name: "subset B",
			A: &replSetGetStatusResponse{
				Members: []statusMember{
					{Name: "a", State: "b"},
				},
			},
			B: &replSetGetStatusResponse{
				Members: []statusMember{
					{Name: "a", State: "b"},
					{Name: "b", State: "c"},
				},
			},
		},
		{
			Name: "nil A",
			B: &replSetGetStatusResponse{
				Members: []statusMember{
					{Name: "b", State: "b"},
				},
			},
		},
		{
			Name: "nil B",
			A: &replSetGetStatusResponse{
				Members: []statusMember{
					{Name: "a", State: "b"},
				},
			},
		},
	}

	for _, c := range cases {
		if sameRSMembers(c.A, c.B) {
			t.Fatalf("failed %s", c.Name)
		}
	}
}

func TestSameIMMembers(t *testing.T) {
	t.Parallel()
	cases := []struct {
		Name string
		A    *isMasterResponse
		B    *isMasterResponse
	}{
		{
			Name: "the same",
			A: &isMasterResponse{
				Hosts: []string{"a", "b"},
			},
			B: &isMasterResponse{
				Hosts: []string{"a", "b"},
			},
		},
		{
			Name: "out of order",
			A: &isMasterResponse{
				Hosts: []string{"a", "b"},
			},
			B: &isMasterResponse{
				Hosts: []string{"b", "a"},
			},
		},
		{
			Name: "both nil",
		},
		{
			Name: "A nil B empty",
			B:    &isMasterResponse{},
		},
		{
			Name: "A empty B nil",
			A:    &isMasterResponse{},
		},
	}

	for _, c := range cases {
		if !sameIMMembers(c.A, c.B) {
			t.Fatalf("failed %s", c.Name)
		}
	}
}

func TestNotSameIMMembers(t *testing.T) {
	t.Parallel()
	cases := []struct {
		Name string
		A    *isMasterResponse
		B    *isMasterResponse
	}{
		{
			Name: "different name",
			A: &isMasterResponse{
				Hosts: []string{"a"},
			},
			B: &isMasterResponse{
				Hosts: []string{"b"},
			},
		},
		{
			Name: "subset A",
			A: &isMasterResponse{
				Hosts: []string{"a", "b"},
			},
			B: &isMasterResponse{
				Hosts: []string{"a"},
			},
		},
		{
			Name: "subset B",
			A: &isMasterResponse{
				Hosts: []string{"a"},
			},
			B: &isMasterResponse{
				Hosts: []string{"a", "b"},
			},
		},
		{
			Name: "nil A",
			B: &isMasterResponse{
				Hosts: []string{"a"},
			},
		},
		{
			Name: "nil B",
			A: &isMasterResponse{
				Hosts: []string{"b"},
			},
		},
	}

	for _, c := range cases {
		if sameIMMembers(c.A, c.B) {
			t.Fatalf("failed %s", c.Name)
		}
	}
}

func TestNewReplicaSetStateFailure(t *testing.T) {
	t.Parallel()
	mgo := mgotest.NewStartedServer(t)
	mgo.Stop()
	_, err := NewReplicaSetState("", "", mgo.URL())
	const expected = "no reachable servers"
	if err == nil || err.Error() != expected {
		t.Fatalf("unexpected error: %s", err)
	}
}
