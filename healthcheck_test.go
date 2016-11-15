package dvara

import (
	"errors"
	"testing"
	"time"
	"github.com/facebookgo/mgotest"
)

type FakeReplicaSet struct {
	handleFailureCalled bool
	CheckReturnsError   bool
}

func (frs *FakeReplicaSet) Check(timeout time.Duration) error {
	if frs.CheckReturnsError {
		return errors.New("Failed")
	}
	return nil
}

func (frs *FakeReplicaSet) HandleFailure() {
	frs.handleFailureCalled = true
}

func TestEnsureRestartIsCalled(t *testing.T) {

	frs := FakeReplicaSet{CheckReturnsError: true}

	hc := HealthChecker{
		HealthCheckInterval:        time.Millisecond,
		FailedHealthCheckThreshold: 2,
	}

	go hc.HealthCheck(&frs, nil)
	time.Sleep(5 * time.Millisecond)
	hc.Cancel = true

	if frs.handleFailureCalled == false {
		t.Fatalf("Restart function not called :( %s", frs)
	}

}

func TestEnsureRestartIsNotCalled(t *testing.T) {

	frs := FakeReplicaSet{CheckReturnsError: false}

	hc := HealthChecker{
		HealthCheckInterval:        time.Millisecond,
		FailedHealthCheckThreshold: 2,
	}

	go hc.HealthCheck(&frs, nil)
	time.Sleep(5 * time.Millisecond)
	hc.Cancel = true

	if frs.handleFailureCalled == true {
		t.Fatalf("Restart function not called :( %s", frs)
	}

}

func TestChecksWithReplicaSets(t *testing.T) {
	t.Parallel()
	standalone := mgotest.NewStartedServer(t)
	defer standalone.Stop()
	rs := mgotest.NewReplicaSet(3, t)
	defer rs.Stop()

	if err := checkReplSetStatus(rs.Addrs(), "rs"); err != nil {
		t.Error("check should pass if all members are in the replica set:", err)
	}
	if err := checkReplSetStatus([]string{standalone.URL()}, "rs"); err == nil {
		t.Error("expected failure if single server running in standalone")
	}
	if err := checkReplSetStatus(append(rs.Addrs(), standalone.URL()), "rs"); err != nil {
		t.Error("check should ignore standalone if there are other healthy members:", err)
	}
	if err := checkReplSetStatus(rs.Addrs(), "rs-alt"); err == nil {
		t.Error("check should fail if members are in a different replica set")
	}
}
