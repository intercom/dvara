package dvara

import (
	"errors"
	"fmt"
	"time"

	"gopkg.in/mgo.v2"
)

// health check timeout
const TIMEOUT = 500 * time.Millisecond

//HealthChecker -> Run health check to verify is dvara still connected to the replica set
type HealthChecker struct {
	consecutiveFailures        uint
	HealthCheckInterval        time.Duration
	FailedHealthCheckThreshold uint
	Cancel                     bool
}

func (checker *HealthChecker) HealthCheck(checkable CheckableMongoConnector) {
	ticker := time.NewTicker(checker.HealthCheckInterval)

	for {
		select {
		case <-ticker.C:
			err := checkable.Check()
			if err != nil {
				checker.consecutiveFailures++
			} else {
				checker.consecutiveFailures = 0
			}
			if checker.consecutiveFailures >= checker.FailedHealthCheckThreshold {
				checker.consecutiveFailures = 0
				checkable.RestartIfFailed()
			}
		}
		if checker.Cancel {
			return
		}
	}
}

type CheckableMongoConnector interface {
	Check() error
	RestartIfFailed()
}

// Attemps to connect to Mongo through Dvara, with timeout.
func (r *ReplicaSet) Check() error {
	errChan := make(chan error)
	go r.runCheck(r.PortStart, errChan)
	// blocking wait
	select {
	case err := <-errChan:
		if err != nil {
			r.Stats.BumpSum("healthcheck.failed", 1)
			r.Log.Errorf("Failed healtcheck due to %s", err)
		} else {
			r.Stats.BumpSum("healthcheck.connected", 1)
		}
		return err
	case <-time.After(TIMEOUT):
		r.Stats.BumpSum("healthcheck.failed", 1)
		r.Log.Errorf("Failed healtcheck due to timeout %s", TIMEOUT)
		return errors.New("Failed due to timeout")
	}
}

func (r *ReplicaSet) RestartIfFailed() {
	r.Log.Error("Restarting replica set due to consecutive failed healthchecks ")
	go r.Restart()
}

// Attemps to connect to Mongo through Dvara. Blocking call.
func (r *ReplicaSet) runCheck(portStart int, errChan chan<- error) {
	// dvara opens a port per member of replica set, we don't expect to run more than 5 members in replica set
	dvaraConnectionString := fmt.Sprintf("127.0.0.1:%d,127.0.0.1:%d,127.0.0.1:%d,127.0.0.1:%d,127.0.0.1:%d", portStart, portStart+1, portStart+2, portStart+3, portStart+4)

	session, err := mgo.DialWithTimeout(dvaraConnectionString, TIMEOUT)
	if err == nil {
		defer session.Close()
		session.SetMode(mgo.Monotonic, true)
		_, isMasterErr := isMaster(session)
		err = isMasterErr
	}
	select {
	case errChan <- err:
	default:
		return
	}
}
