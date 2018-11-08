package dvara

import (
	"net"
	"sync/atomic"
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
			t.Fatalf("failed %v with input %v response %v expected response %v", c.Name, c.A, response, c.B)
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

func TestReplicaStateFailsFast(t *testing.T) {
	t.Parallel()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := closedTCPServer{listener: listener}
	defer server.Close()
	err = server.Start()
	if err != nil {
		t.Fatal(err)
	}

	_, err = NewReplicaSetState("", "", listener.Addr().String())
	if err == nil {
		t.Fatal("expected error")
	}
	numAccepts := server.CountAccepts()
	if numAccepts != 1 {
		t.Errorf("RS state sync tried to connect %d times, expecting once", numAccepts)
	}
}

type closedTCPServer struct {
	listener net.Listener
	ops      uint64
}

func (server *closedTCPServer) Start() error {
	go server.acceptLoop()
	return nil
}

func (server *closedTCPServer) Close() error {
	return server.listener.Close()
}

func (server *closedTCPServer) CountAccepts() uint64 {
	return atomic.LoadUint64(&server.ops)
}

func (server *closedTCPServer) acceptLoop() {
	for {
		conn, err := server.listener.Accept()
		if err != nil {
			return
		}
		atomic.AddUint64(&server.ops, 1)
		if conn.Close() != nil {
			return
		}
	}
}
