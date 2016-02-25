package dvara

import (
	"errors"
	"flag"
	"fmt"
	"net"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/facebookgo/gangliamr"
	"github.com/facebookgo/metrics"
	"github.com/facebookgo/stackerr"
	"github.com/facebookgo/stats"
	"github.com/streamrail/concurrent-map"
)

var hardRestart = flag.Bool(
	"hard_restart",
	true,
	"if true will drop clients on restart",
)

// Logger allows for simple text logging.
type Logger interface {
	Error(args ...interface{})
	Errorf(format string, args ...interface{})
	Warn(args ...interface{})
	Warnf(format string, args ...interface{})
	Info(args ...interface{})
	Infof(format string, args ...interface{})
	Debug(args ...interface{})
	Debugf(format string, args ...interface{})
}

var errNoAddrsGiven = errors.New("dvara: no seed addresses given for ReplicaSet")

// ReplicaSet manages the real => proxy address mapping.
// NewReplicaSet returns the ReplicaSet given the list of seed servers. It is
// required for the seed servers to be a strict subset of the actual members if
// they are reachable. That is, if two of the addresses are members of
// different replica sets, it will be considered an error.
type ReplicaSet struct {
	Log                    Logger                  `inject:""`
	ReplicaSetStateCreator *ReplicaSetStateCreator `inject:""`
	ProxyQuery             *ProxyQuery             `inject:""`

	// Stats if provided will be used to record interesting stats.
	Stats stats.Client `inject:""`

	// Comma separated list of mongo addresses. This is the list of "seed"
	// servers, and one of two conditions must be met for each entry here -- it's
	// either alive and part of the same replica set as all others listed, or is
	// not reachable.
	Addrs string

	// PortStart and PortEnd define the port range within which proxies will be
	// allocated.
	PortStart int
	PortEnd   int

	// Where to listen for clients.
	// "0.0.0.0" means public service, "127.0.0.1" means localhost only.
	ListenAddr string

	// Maximum number of connections that will be established to each mongo node.
	MaxConnections uint

	// MinIdleConnections is the number of idle server connections we'll keep
	// around.
	MinIdleConnections uint

	// ServerIdleTimeout is the duration after which a server connection will be
	// considered idle.
	ServerIdleTimeout time.Duration

	// ServerClosePoolSize is the number of goroutines that will handle closing
	// server connections.
	ServerClosePoolSize uint

	// ClientIdleTimeout is how long until we'll consider a client connection
	// idle and disconnect and release it's resources.
	ClientIdleTimeout time.Duration

	// MaxPerClientConnections is how many client connections are allowed from a
	// single client.
	MaxPerClientConnections uint

	// GetLastErrorTimeout is how long we'll hold on to an acquired server
	// connection expecting a possibly getLastError call.
	GetLastErrorTimeout time.Duration

	// MessageTimeout is used to determine the timeout for a single message to be
	// proxied.
	MessageTimeout time.Duration

	// Name is the name of the replica set to connect to. Nodes that are not part
	// of this replica set will be ignored. If this is empty, the first replica set
	// will be used
	Name string

	// Username is the username used to connect to the server for retrieving replica state.
	Username string

	// Password is the password used to connect to the server for retrieving replica state.
	Password string

	ClientsConnected metrics.Counter

	Mutex *sync.RWMutex

	proxyToReal cmap.ConcurrentMap
	realToProxy cmap.ConcurrentMap
	ignoredReal map[string]ReplicaState
	proxies     map[string]*Proxy
	restarter   *sync.Once
	lastState   *ReplicaSetState
}

// RegisterMetrics registers the available metrics.
func (r *ReplicaSet) RegisterMetrics(registry *gangliamr.Registry) {
	gangliaGroup := []string{"dvara"}
	r.ClientsConnected = &gangliamr.Counter{
		Name:   "clients_connected",
		Title:  "Client Connected",
		Units:  "conn",
		Groups: gangliaGroup,
	}
	registry.Register(r.ClientsConnected)
}

// Start starts proxies to support this ReplicaSet.
func (r *ReplicaSet) Start() error {
	r.Mutex.Lock()
	defer r.Mutex.Unlock()
	r.proxyToReal = cmap.New()
	r.realToProxy = cmap.New()
	r.ignoredReal = make(map[string]ReplicaState)
	r.proxies = make(map[string]*Proxy)

	if r.Addrs == "" {
		return errNoAddrsGiven
	}

	var err error
	if err = r.generateState(); err != nil {
		return err
	}

	r.restarter = new(sync.Once)

	// Start all the proxies
	var wg sync.WaitGroup
	wg.Add(len(r.proxies))
	errch := make(chan error, len(r.proxies))
	for _, p := range r.proxies {
		go func(p *Proxy) {
			defer wg.Done()
			if err := p.Start(); err != nil {
				r.Log.Error(err)
				errch <- stackerr.Wrap(err)
			}
		}(p)
	}

	wg.Wait()
	select {
	default:
		return nil
	case err := <-errch:
		return err
	}
}

func (r *ReplicaSet) generateState() error {
	rawAddrs := strings.Split(r.Addrs, ",")
	var err error
	r.lastState, err = r.ReplicaSetStateCreator.FromAddrs(r.Username, r.Password, rawAddrs, r.Name)
	if err != nil {
		r.Stats.BumpSum("replica.start.failed_state_creation", 1)
		return err
	}

	healthyAddrs := r.lastState.Addrs()

	// Ensure we have at least one health address.
	if len(healthyAddrs) == 0 {
		return stackerr.Newf("no healthy primaries or secondaries: %s", r.Addrs)
	}

	// Add discovered nodes to seed address list. Over time if the original seed
	// nodes have gone away and new nodes have joined this ensures that we'll
	// still be able to connect.
	r.Addrs = strings.Join(uniq(append(rawAddrs, healthyAddrs...)), ",")

	if err = r.attachProxies(healthyAddrs); err != nil {
		return err
	}

	// add the ignored hosts, unless lastRS is nil (single node mode)
	if r.lastState.lastRS != nil {
		for _, member := range r.lastState.lastRS.Members {
			if _, ok := r.realToProxy.Get(member.Name); !ok {
				r.ignoredReal[member.Name] = member.State
			}
		}
	}

	return nil
}

func (r *ReplicaSet) attachProxies(healthyAddrs []string) error {
	for _, addr := range healthyAddrs {
		if _, err := r.AddProxy(addr); err != nil {
			return err
		}
	}
	return nil
}

func (r *ReplicaSet) AddProxy(address string) (*Proxy, error) {
	listener, err := r.newListener()
	if err != nil {
		return nil, err
	}

	p := &Proxy{
		Log:            r.Log,
		ReplicaSet:     r,
		ClientListener: listener,
		ProxyAddr:      r.proxyAddr(listener),
		Username:       r.Username,
		Password:       r.Password,
		MongoAddr:      address,
	}
	if err := r.add(p); err != nil {
		return nil, err
	}
	return p, nil
}

func (r *ReplicaSet) RemoveProxy(proxy *Proxy) error {
	return r.remove(proxy)
}

// Stop stops all the associated proxies for this ReplicaSet.
func (r *ReplicaSet) Stop() error {
	return r.stop(false)
}

func (r *ReplicaSet) stop(hard bool) error {
	r.Mutex.RLock()
	defer r.Mutex.RUnlock()
	r.Stats.BumpSum("replica.stop", 1)
	var wg sync.WaitGroup
	wg.Add(len(r.proxies))
	errch := make(chan error, len(r.proxies))
	for _, p := range r.proxies {
		go func(p *Proxy) {
			defer wg.Done()
			if err := p.stop(hard); err != nil {
				r.Log.Error(err)
				errch <- stackerr.Wrap(err)
			}
		}(p)
	}
	wg.Wait()
	select {
	default:
		return nil
	case err := <-errch:
		return err
	}
}

// Restart stops all the proxies and restarts them. This is used when we detect
// an RS config change, like when an election happens.
func (r *ReplicaSet) Restart() {
	r.restarter.Do(func() {
		r.Log.Info("restart triggered")
		r.Stats.BumpSum("replica.restart", 1)
		if err := r.stop(*hardRestart); err != nil {
			// We log and ignore this hoping for a successful start anyways.
			r.Log.Errorf("stop failed for restart: %s", err)
		} else {
			r.Log.Info("successfully stopped for restart")
		}

		if err := r.Start(); err != nil {
			// We panic here because we can't repair from here and are pretty much
			// fucked.
			panic(fmt.Errorf("start failed for restart: %s", err))
		}

		r.Log.Info("successfully restarted")
	})
}

func (r *ReplicaSet) proxyAddr(l net.Listener) string {
	_, port, err := net.SplitHostPort(l.Addr().String())
	if err != nil {
		panic(err)
	}

	return fmt.Sprintf("%s:%s", r.proxyHostname(), port)
}

func (r *ReplicaSet) proxyHostname() string {
	const home = "127.0.0.1"

	hostname, err := os.Hostname()
	if err != nil {
		r.Log.Error(err)
		return home
	}

	// The follow logic ensures that the hostname resolves to a local address.
	// If it doesn't we don't use it since it probably wont work anyways.
	hostnameAddrs, err := net.LookupHost(hostname)
	if err != nil {
		r.Log.Error(err)
		return home
	}

	interfaceAddrs, err := net.InterfaceAddrs()
	if err != nil {
		r.Log.Error(err)
		return home
	}

	for _, ia := range interfaceAddrs {
		sa := ia.String()
		for _, ha := range hostnameAddrs {
			// check for an exact match or a match ignoring the suffix bits
			if sa == ha || strings.HasPrefix(sa, ha+"/") {
				return hostname
			}
		}
	}
	r.Log.Warnf("hostname %s doesn't resolve to the current host", hostname)
	return home
}

func (r *ReplicaSet) newListener() (net.Listener, error) {
	for i := r.PortStart; i <= r.PortEnd; i++ {
		listener, err := net.Listen("tcp", fmt.Sprintf("%s:%d", r.ListenAddr, i))
		if err == nil {
			return listener, nil
		}
	}
	return nil, fmt.Errorf(
		"could not find a free port in range %d-%d",
		r.PortStart,
		r.PortEnd,
	)
}

// add a proxy/mongo mapping.
func (r *ReplicaSet) add(p *Proxy) error {
	if _, ok := r.proxyToReal.Get(p.ProxyAddr); ok {
		return fmt.Errorf("proxy %s already used in ReplicaSet", p.ProxyAddr)
	}
	if _, ok := r.realToProxy.Get(p.MongoAddr); ok {
		return fmt.Errorf("mongo %s already exists in ReplicaSet", p.MongoAddr)
	}
	r.Log.Infof("added %s", p)
	r.proxyToReal.Set(p.ProxyAddr, p.MongoAddr)
	r.realToProxy.Set(p.MongoAddr, p.ProxyAddr)
	r.proxies[p.ProxyAddr] = p
	return nil
}

func (r *ReplicaSet) remove(p *Proxy) error {
	if _, ok := r.proxyToReal.Get(p.ProxyAddr); !ok {
		return fmt.Errorf("proxy %s does not exist in ReplicaSet", p.ProxyAddr)
	}
	if _, ok := r.realToProxy.Get(p.MongoAddr); !ok {
		return fmt.Errorf("mongo %s does not exist in ReplicaSet", p.MongoAddr)
	}
	r.Log.Infof("removed %s", p)
	r.proxyToReal.Remove(p.ProxyAddr)
	r.realToProxy.Remove(p.MongoAddr)
	delete(r.proxies, p.ProxyAddr)
	return nil
}

// Proxy returns the corresponding proxy address for the given real mongo
// address.
func (r *ReplicaSet) Proxy(h string) (string, error) {
	p, ok := r.realToProxy.Get(h)
	if !ok {
		if s, ok := r.ignoredReal[h]; ok {
			return "", &ProxyMapperError{
				RealHost: h,
				State:    s,
			}
		}
		return "", fmt.Errorf("mongo %s is not in ReplicaSet", h)
	}
	return p.(string), nil
}

// ProxyMembers returns the list of proxy members in this ReplicaSet.
func (r *ReplicaSet) ProxyMembers() []string {
	members := make([]string, 0, len(r.proxyToReal))
	for r := range r.proxyToReal.Iter() {
		members = append(members, r.Val.(string))
	}
	return members
}

// SameRS checks if the given replSetGetStatusResponse is the same as the last
// state.
func (r *ReplicaSet) SameRS(o *replSetGetStatusResponse) bool {
	return r.lastState.SameRS(o)
}

// SameIM checks if the given isMasterResponse is the same as the last state.
func (r *ReplicaSet) SameIM(o *isMasterResponse) bool {
	return r.lastState.SameIM(o)
}

// ProxyMapperError occurs when a known host is being ignored and does not have
// a corresponding proxy address.
type ProxyMapperError struct {
	RealHost string
	State    ReplicaState
}

func (p *ProxyMapperError) Error() string {
	return fmt.Sprintf("error mapping host %s in state %s", p.RealHost, p.State)
}

// uniq takes a slice of strings and returns a new slice with duplicates
// removed.
func uniq(set []string) []string {
	m := make(map[string]struct{}, len(set))
	for _, s := range set {
		m[s] = struct{}{}
	}
	news := make([]string, 0, len(m))
	for s := range m {
		news = append(news, s)
	}
	return news
}
