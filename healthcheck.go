package dvara

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"gopkg.in/mgo.v2"
)

//HealthChecker -> Run health check to verify is dvara still connected to the replica set
type HealthChecker struct {
	consecutiveFailures        uint
	HealthCheckInterval        time.Duration
	FailedHealthCheckThreshold uint
	Cancel                     bool
	syncTryChan                chan<- struct{}
}

func (checker *HealthChecker) HealthCheck(checkable CheckableMongoConnector, syncTryChan chan<- struct{}) {
	ticker := time.NewTicker(checker.HealthCheckInterval)

	if syncTryChan != nil {
		checker.syncTryChan = syncTryChan
	}

	for {
		select {
		case <-ticker.C:
			checker.tryRunReplicaChecker()
			err := checkable.Check(checker.HealthCheckInterval)
			if err != nil {
				checker.consecutiveFailures++
			} else {
				checker.consecutiveFailures = 0
			}
			if checker.consecutiveFailures >= checker.FailedHealthCheckThreshold {
				checker.consecutiveFailures = 0
				checkable.HandleFailure()
			}
		}
		if checker.Cancel {
			return
		}
	}
}

func (checker *HealthChecker) tryRunReplicaChecker() {
	if checker.syncTryChan != nil {
		select {
		case checker.syncTryChan <- struct{}{}:
		default:
		}
	}
}

type CheckableMongoConnector interface {
	Check(timeout time.Duration) error
	HandleFailure()
}

// Attemps to connect to Mongo through Dvara, with timeout.
func (r *ReplicaSet) Check(timeout time.Duration) error {
	errChan := make(chan error)
	go r.runCheck(r.PortStart, errChan)
	// blocking wait
	select {
	case err := <-errChan:
		if err != nil {
			r.Stats.BumpSum("healthcheck.failed", 1)
			r.Log.Errorf("Failed healthcheck due to %s", err)
		} else {
			r.Stats.BumpSum("healthcheck.connected", 1)
		}
		return err
	case <-time.After(timeout):
		r.Stats.BumpSum("healthcheck.failed", 1)
		r.Log.Errorf("Failed healtcheck due to timeout %s", timeout)
		return errors.New("Failed due to timeout")
	}
}

func (r *ReplicaSet) HandleFailure() {
	r.Log.Error("Crashing dvara due to consecutive failed healthchecks")
	r.Stats.BumpSum("healthcheck.failed.panic", 1)
	panic("failed healthchecks")
}

// Attemps to connect to Mongo through Dvara. Blocking call.
func (r *ReplicaSet) runCheck(portStart int, errChan chan<- error) {
	t := r.Stats.BumpTime("healthcheck.time")
	t.End()
	// dvara opens a port per member of replica set, we don't expect to run more than 5 members in replica set
	dvaraConnectionString := fmt.Sprintf("127.0.0.1:%d,127.0.0.1:%d,127.0.0.1:%d,127.0.0.1:%d,127.0.0.1:%d", portStart, portStart+1, portStart+2, portStart+3, portStart+4)

	info := &mgo.DialInfo{
		Addrs:    strings.Split(dvaraConnectionString, ","),
		FailFast: true,
		// Without direct option, healthcheck fails in case there are only secondaries in the replica set
		Direct: true,
	}

	session, err := mgo.DialWithInfo(info)
	if err == nil {
		defer session.Close()
		session.SetMode(mgo.PrimaryPreferred, true)
		_, isMasterErr := isMaster(session)
		err = isMasterErr
	}
	select {
	case errChan <- err:
	default:
		return
	}
}
