package dvara

import "testing"

func TestCheckerFindsMissingExtraMembers(t *testing.T) {
	checker := newChecker()

	comparison, _ := checker.getComparison(getStatusResponse("a", "b"), getStatusResponse("a", "c"))
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

func TestCheckerAddsAndRemovesProxies(t *testing.T) {
	checker := newChecker()
	checker.replicaSet.AddProxy("mongoA")
	checker.replicaSet.AddProxy("mongoB")

	comparison, _ := checker.getComparison(getStatusResponse("mongoA", "mongoB"), getStatusResponse("mongoA", "mongoC"))
	checker.addRemoveProxies(comparison)
	if _, ok := checker.replicaSet.proxies[checker.replicaSet.realToProxy["mongoA"]]; !ok {
		t.Fatal("proxyA was removed")
	}
	if _, ok := checker.replicaSet.proxies[checker.replicaSet.realToProxy["mongoB"]]; ok {
		t.Fatal("proxyB was not removed")
	}
	if _, ok := checker.replicaSet.proxies[checker.replicaSet.realToProxy["mongoC"]]; !ok {
		t.Fatal("proxyC was not added")
	}
}

func newChecker() *ReplicaSetChecker {
	return &ReplicaSetChecker{
		Log:        nopLogger{},
		replicaSet: setupReplicaSet(),
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
