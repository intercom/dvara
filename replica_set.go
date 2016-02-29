// Dvara is a connection pool manager/proxy for MongoDB.
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
	"github.com/facebookgo/stats"
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

	restarter *sync.Once
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

func (r *ReplicaSet) Start() error {
	if r.Addrs == "" {
		return errNoAddrsGiven
	}

	r.restarter = new(sync.Once)
	return nil
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
