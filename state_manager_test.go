package dvara

import (
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/josler/mgotest"
)

func TestManagerFindsMissingExtraMembers(t *testing.T) {
	manager := newManager()

	comparison, _ := manager.getComparison(getStatusResponse("a", "b"), getStatusResponse("a", "c"))
	if _, ok := comparison.ExtraMembers["b"]; !ok {
		t.Fatal("Extra member b not found")
	}
	if _, ok := comparison.MissingMembers["c"]; !ok {
		t.Fatal("Missing member c not found")
	}
	if len(comparison.ExtraMembers) != 1 {
		t.Fatalf("too many extra members, expecting %d, got %d", 1, len(comparison.ExtraMembers))
	}
	if len(comparison.MissingMembers) != 1 {
		t.Fatalf("too many extra members, expecting %d, got %d", 1, len(comparison.MissingMembers))
	}
}

func TestManagerAddsAndRemovesProxies(t *testing.T) {
	manager := newManager()
	manager.addProxies("mongoA", "mongoB")
	comparison, _ := manager.getComparison(getStatusResponse("mongoA", "mongoB"), getStatusResponse("mongoA", "mongoC"))
	manager.addRemoveProxies(comparison)
	if _, ok := manager.proxies[manager.realToProxy["mongoA"]]; !ok {
		t.Fatal("proxyA was removed")
	}
	if _, ok := manager.proxies[manager.realToProxy["mongoB"]]; ok {
		t.Fatal("proxyB was not removed")
	}
	if _, ok := manager.proxies[manager.realToProxy["mongoC"]]; !ok {
		t.Fatal("proxyC was not added")
	}
}

func TestReplicaSetMembers(t *testing.T) {
	t.Parallel()
	h := NewReplicaSetHarness(3, t)
	defer h.Stop()

	proxyMembers := h.Manager.ProxyMembers()
	session := h.ProxySession()
	defer session.Close()
	status, err := replSetGetStatus(session)
	if err != nil {
		t.Fatal(err)
	}

outerProxyResponseCheckLoop:
	for _, m := range status.Members {
		for _, p := range proxyMembers {
			if m.Name == p {
				continue outerProxyResponseCheckLoop
			}
		}
		t.Fatalf("Unexpected member: %s", m.Name)
	}
}

func TestProxyNotInReplicaSet(t *testing.T) {
	t.Parallel()
	h := NewSingleHarness(t)
	defer h.Stop()
	addr := "127.0.0.1:666"
	expected := fmt.Sprintf("mongo %s is not in ReplicaSet", addr)
	_, err := h.Manager.Proxy(addr)
	if err == nil || err.Error() != expected {
		t.Fatalf("did not get expected error, got: %s", err)
	}
}

func TestAddSameProxyToReplicaSet(t *testing.T) {
	t.Parallel()
	m := newManager()
	p := &Proxy{
		ProxyAddr: "1",
		MongoAddr: "2",
	}
	if _, err := m.addProxy(p); err != nil {
		t.Fatal(err)
	}
	expected := fmt.Sprintf("proxy %s already used in ReplicaSet", p.ProxyAddr)
	_, err := m.addProxy(p)
	if err == nil || err.Error() != expected {
		t.Fatalf("did not get expected error, got: %s", err)
	}
}

func TestAddSameMongoToReplicaSet(t *testing.T) {
	t.Parallel()
	m := newManager()
	p := &Proxy{
		ProxyAddr: "1",
		MongoAddr: "2",
	}
	if _, err := m.addProxy(p); err != nil {
		t.Fatal(err)
	}
	p = &Proxy{
		ProxyAddr: "3",
		MongoAddr: p.MongoAddr,
	}
	expected := fmt.Sprintf("mongo %s already exists in ReplicaSet", p.MongoAddr)
	_, err := m.addProxy(p)
	if err == nil || err.Error() != expected {
		t.Fatalf("did not get expected error, got: %s", err)
	}
}

func TestAddRemoveProxy(t *testing.T) {
	t.Parallel()
	m := newManager()
	p := &Proxy{
		ProxyAddr: "1",
		MongoAddr: "2",
	}
	if _, err := m.addProxy(p); err != nil {
		t.Fatal(err)
	}
	m.removeProxy(p)
	if _, ok := m.proxies[p.ProxyAddr]; ok {
		t.Fatal("failed to remove proxy")
	}
}

func TestSingleNodeWhenExpectingRS(t *testing.T) {
	t.Parallel()
	mgoserver := mgotest.NewStartedServer(t)
	defer mgoserver.Stop()
	replicaSet := ReplicaSet{
		Log:   nopLogger{},
		Addrs: fmt.Sprintf("127.0.0.1:%d,127.0.0.1:%d", mgoserver.Port, mgoserver.Port+1),
		ReplicaSetStateCreator: &ReplicaSetStateCreator{Log: nopLogger{}},
		MaxConnections:         1,
	}
	manager := NewStateManager(&replicaSet)
	manager.Log = replicaSet.Log
	err := manager.Start()
	if err == nil || !strings.Contains(err.Error(), "was expecting it to be in a replica set") {
		t.Fatalf("did not get expected error, got: %s", err)
	}
}

func newManager() *StateManager {
	replicaSet := setupReplicaSet()
	return newManagerWithReplicaSet(replicaSet)
}

func newManagerWithReplicaSet(replicaSet *ReplicaSet) *StateManager {
	return &StateManager{
		Log:         replicaSet.Log,
		RWMutex:     &sync.RWMutex{},
		replicaSet:  replicaSet,
		baseAddrs:   replicaSet.Addrs,
		proxyToReal: make(map[string]string),
		realToProxy: make(map[string]string),
		proxies:     make(map[string]*Proxy),
	}
}

func getStatusResponse(memberNames ...string) *replSetGetStatusResponse {
	members := []statusMember{}
	for _, name := range memberNames {
		members = append(members, statusMember{Name: name, State: "state"})
	}
	return &replSetGetStatusResponse{
		Members: members,
	}
}
