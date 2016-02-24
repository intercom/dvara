package dvara

import (
	"errors"
	"testing"
	"time"
)

type FakeReplicaSet struct {
	restartWasCalled  bool
	CheckReturnsError bool
}

func (frs *FakeReplicaSet) Check() error {
	if frs.CheckReturnsError {
		return errors.New("Failed")
	}
	return nil
}

func (frs *FakeReplicaSet) RestartIfFailed() {
	frs.restartWasCalled = true
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

	if frs.restartWasCalled == false {
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

	if frs.restartWasCalled == true {
		t.Fatalf("Restart function not called :( %s", frs)
	}

}
