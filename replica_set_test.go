package dvara

import (
	"testing"

	"github.com/facebookgo/subset"

	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

func TestStopNodeInReplica(t *testing.T) {
	t.Parallel()
	h := NewReplicaSetHarness(2, t)
	defer h.Stop()

	const dbName = "test"
	const colName = "foo"
	const keyName = "answer"
	d := bson.M{"answer": "42"}
	s := h.ProxySession()
	defer s.Close()
	s.SetSafe(&mgo.Safe{W: 2, WMode: "majority"})
	if err := s.DB(dbName).C(colName).Insert(d); err != nil {
		t.Fatal(err)
	}

	h.MgoReplicaSet.Servers[0].Stop()

	s.SetMode(mgo.Monotonic, true)
	var actual bson.M
	if err := s.DB(dbName).C(colName).Find(d).One(&actual); err != nil {
		t.Fatal(err)
	}

	subset.Assert(t, d, actual)
}

func TestNewListenerZeroZeroRandomPort(t *testing.T) {
	t.Parallel()
	r := &ReplicaSet{}
	l, err := r.newListener()
	if err != nil {
		t.Fatal(err)
	}
	l.Close()
}

func TestNewListenerError(t *testing.T) {
	t.Parallel()
	r := &ReplicaSet{PortStart: 1, PortEnd: 1}
	_, err := r.newListener()
	expected := "could not find a free port in range 1-1"
	if err == nil || err.Error() != expected {
		t.Fatalf("did not get expected error, got: %s", err)
	}
}

func TestNoAddrsGiven(t *testing.T) {
	replicaSet := ReplicaSet{MaxConnections: 1}
	err := replicaSet.Start()
	if err != errNoAddrsGiven {
		t.Fatalf("did not get expected error, got: %s", err)
	}
}

func setupReplicaSet() *ReplicaSet {
	log := nopLogger{}
	return &ReplicaSet{
		Log: log,
		ReplicaSetStateCreator: &ReplicaSetStateCreator{Log: log},
	}
}
