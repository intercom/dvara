package dvara

import (
	"errors"
	"strings"
)

type ReplicaSetChecker struct {
	Log        Logger
	replicaSet *ReplicaSet
}

type ReplicaSetComparison struct {
	// Extra members that are in this state, but not in new state
	ExtraMembers map[string]*Proxy
	// Missing members that aren't in this state, but are in new
	MissingMembers map[string]*Proxy
}

func (checker *ReplicaSetChecker) Check() error {
	t := checker.replicaSet.Stats.BumpTime("replica.checker.time")
	defer t.End()
	addrs := strings.Split(checker.replicaSet.Addrs, ",")
	r, err := checker.replicaSet.ReplicaSetStateCreator.FromAddrs(checker.replicaSet.Username, checker.replicaSet.Password, addrs, checker.replicaSet.Name)
	if err != nil {
		checker.replicaSet.Stats.BumpSum("replica.checker.failed_state_check", 1)
		checker.Log.Errorf("all nodes possibly down?: %s", err)
		return err
	}

	comparison, err := checker.getComparison(checker.replicaSet.lastState.lastRS, r.lastRS)
	if err != nil {
		return err
	}

	checker.replicaSet.Mutex.Lock()
	defer checker.replicaSet.Mutex.Unlock()
	if err = checker.addRemoveProxies(comparison); err != nil {
		return err
	}
	go checker.stopStartProxies(comparison)
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
		checker.replicaSet.RemoveProxy(proxy)
	}

	for name, _ := range comparison.MissingMembers {
		p, err := checker.replicaSet.AddProxy(name)
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
	proxyName, ok := checker.replicaSet.realToProxy[member.Name]
	if !ok {
		return nil
	}
	return checker.replicaSet.proxies[proxyName]
}
