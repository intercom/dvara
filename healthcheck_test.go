package dvara

import (
	"errors"
	"testing"
	"time"
)

type FakeReplicaSet struct {
	handleFailureCalled bool
	CheckReturnsError   bool
}

func (frs *FakeReplicaSet) Check() error {
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
