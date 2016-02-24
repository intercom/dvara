package dvara

import (
	"errors"
	"strings"
)

type ReplicaSetChecker struct {
	Log                 Logger
	ReplicaSet          *ReplicaSet
	ReplicaCheckTryChan chan struct{}
}

type ReplicaSetComparison struct {
	// Extra members that are in this state, but not in new state
	ExtraMembers map[string]*Proxy
	// Missing members that aren't in this state, but are in new
	MissingMembers map[string]*Proxy
}

// Run the ReplicSetChecker asynchronously, watching for requests to perform a check on the channel
func (checker *ReplicaSetChecker) Run() {
	for {
		select {
		case <-checker.ReplicaCheckTryChan:
			checker.Check()
		}
	}
}

func (checker *ReplicaSetChecker) Check() error {
	t := checker.ReplicaSet.Stats.BumpTime("replica.checker.time")
	defer t.End()
	addrs := strings.Split(checker.ReplicaSet.Addrs, ",")
	r, err := checker.ReplicaSet.ReplicaSetStateCreator.FromAddrs(checker.ReplicaSet.Username, checker.ReplicaSet.Password, addrs, checker.ReplicaSet.Name)
	if err != nil {
		checker.ReplicaSet.Stats.BumpSum("replica.checker.failed_state_check", 1)
		checker.Log.Errorf("all nodes possibly down?: %s", err)
		return err
	}

	comparison, err := checker.getComparison(checker.ReplicaSet.lastState.lastRS, r.lastRS)
	if err != nil {
		checker.ReplicaSet.Stats.BumpSum("replica.checker.failed_comparison", 1)
		checker.Log.Errorf("Checker failed comparison %s", err)
		return err
	}

	checker.ReplicaSet.Mutex.Lock()
	defer checker.ReplicaSet.Mutex.Unlock()
	if err = checker.addRemoveProxies(comparison); err != nil {
		checker.ReplicaSet.Stats.BumpSum("replica.checker.failed_proxy_update", 1)
		checker.Log.Errorf("Checker failed proxy update %s", err)
		return err
	}

	if err = checker.stopStartProxies(comparison); err != nil {
		checker.ReplicaSet.Stats.BumpSum("replica.checker.failed_proxy_start_stop", 1)
		checker.Log.Errorf("Checker failed proxy start stop %s", err)
		return err
	}
	return nil
}

func (checker *ReplicaSetChecker) getComparison(oldResp, newResp *replSetGetStatusResponse) (*ReplicaSetComparison, error) {
	comparison := &ReplicaSetComparison{
		ExtraMembers:   make(map[string]*Proxy),
		MissingMembers: make(map[string]*Proxy),
	}

	if (oldResp == nil || len(oldResp.Members) == 0) && (newResp == nil || len(newResp.Members) == 0) {
		return nil, errors.New("No members found")
	}
	if oldResp == nil || newResp == nil {
		return nil, errors.New("No members found")
	}

	for _, m := range oldResp.Members {
		comparison.ExtraMembers[m.Name] = checker.findProxyForMember(m)
	}

	for _, m := range newResp.Members {
		if _, ok := comparison.ExtraMembers[m.Name]; ok {
			// if we've got the same thing in new then it's not extra
			delete(comparison.ExtraMembers, m.Name)
		} else {
			// otherwise it's missing
			comparison.MissingMembers[m.Name] = nil // we don't have a proxy to add just yet
		}
	}
	return comparison, nil
}

func (checker *ReplicaSetChecker) addRemoveProxies(comparison *ReplicaSetComparison) error {
	for _, proxy := range comparison.ExtraMembers {
		checker.ReplicaSet.RemoveProxy(proxy)
	}

	for name, _ := range comparison.MissingMembers {
		p, err := checker.ReplicaSet.AddProxy(name)
		if err != nil {
			return err
		}
		comparison.MissingMembers[name] = p
	}
	return nil
}

func (checker *ReplicaSetChecker) stopStartProxies(comparison *ReplicaSetComparison) error {
	for _, proxy := range comparison.ExtraMembers {
		proxy.stop(true)
	}

	for _, p := range comparison.MissingMembers {
		if err := p.Start(); err != nil {
			return err
		}
	}
	return nil
}

func (checker *ReplicaSetChecker) findProxyForMember(member statusMember) *Proxy {
	proxyName, ok := checker.ReplicaSet.realToProxy[member.Name]
	if !ok {
		return nil
	}
	return checker.ReplicaSet.proxies[proxyName]
}
