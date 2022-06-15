package jsonrpc

import (
	"context"
	"errors"
	"net"
	"net/rpc"
	"net/rpc/jsonrpc"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const DefaultMaxConnsPerHost = 512
const DefaultDialTimeout = 3 * time.Second
const DefaultMaxIdleConnDuration = 10 * time.Second

var ErrNoFreeConns = errors.New("no free connections available to host")

type Client struct {
	ReadTimeout         time.Duration
	WriteTimeout        time.Duration
	MaxConnsPerHost     int
	MaxConnWaitTimeout  time.Duration
	MaxIdleConnDuration time.Duration
	Dial                DialFunc

	mLock sync.Mutex
	m     map[string]*HostClient
}

func (c *Client) Call(ctx context.Context, addr, serviceMethod string, args, reply any) error {
	startCleaner := false

	c.mLock.Lock()
	m := c.m
	if m == nil {
		m = make(map[string]*HostClient)
		c.m = m
	}
	hc := m[addr]
	if hc == nil {
		hc = &HostClient{
			Addr:                addr,
			Dial:                c.Dial,
			MaxConns:            c.MaxConnsPerHost,
			MaxIdleConnDuration: c.MaxIdleConnDuration,
			ReadTimeout:         c.ReadTimeout,
			WriteTimeout:        c.WriteTimeout,
			MaxConnWaitTimeout:  c.MaxConnWaitTimeout,
		}

		m[addr] = hc
		if len(m) == 1 {
			startCleaner = true
		}
	}

	atomic.AddInt32(&hc.pendingClientRequests, 1)
	defer atomic.AddInt32(&hc.pendingClientRequests, -1)

	c.mLock.Unlock()

	if startCleaner {
		go c.mCleaner(m)
	}

	return hc.Call(ctx, serviceMethod, args, reply)
}

func (c *Client) CloseIdleConnections() {
	c.mLock.Lock()
	for _, v := range c.m {
		v.CloseIdleConnections()
	}
	c.mLock.Unlock()
}

func (c *Client) mCleaner(m map[string]*HostClient) {
	mustStop := false

	sleep := c.MaxIdleConnDuration
	if sleep < time.Second {
		sleep = time.Second
	} else if sleep > 10*time.Second {
		sleep = 10 * time.Second
	}

	for {
		c.mLock.Lock()
		for k, v := range m {
			v.connsLock.Lock()
			if v.connsCount == 0 && atomic.LoadInt32(&v.pendingClientRequests) == 0 {
				delete(m, k)
			}
			v.connsLock.Unlock()
		}
		if len(m) == 0 {
			mustStop = true
		}
		c.mLock.Unlock()

		if mustStop {
			break
		}
		time.Sleep(sleep)
	}
}

type HostClient struct {
	Addr                string
	ReadTimeout         time.Duration
	WriteTimeout        time.Duration
	MaxConns            int
	MaxConnWaitTimeout  time.Duration
	MaxIdleConnDuration time.Duration
	Dial                DialFunc

	connsLock  sync.Mutex
	connsCount int
	conns      []*rpcClient
	connsWait  *wantConnQueue

	addrsLock sync.Mutex
	addrs     []string
	addrIdx   uint32

	connsCleanerRun bool

	pendingClientRequests int32
}

func (c *HostClient) Call(ctx context.Context, serviceMethod string, args, reply any) error {
	var err error
	var retry bool
	var attempts int
	maxAttempts := 10

	for {
		retry, err = c.call(ctx, serviceMethod, args, reply)
		if err != nil && retry {
			attempts++
			if attempts >= maxAttempts {
				break
			}
			continue
		} else if err != nil && !retry {
			return err
		}

		return nil
	}

	return err
}

func (c *HostClient) call(ctx context.Context, serviceMethod string, args, reply any) (bool, error) {
	var reqTimeout time.Duration
	if deadline, ok := ctx.Deadline(); ok {
		reqTimeout = deadline.Sub(time.Now())
	}

	cc, err := c.acquireConn(reqTimeout, false)
	if err != nil {
		return false, err
	}

	conn := cc.conn

	if c.WriteTimeout > 0 {
		// Set Deadline every time, since golang has fixed the performance issue
		// See https://github.com/golang/go/issues/15133#issuecomment-271571395 for details
		if err = conn.SetWriteDeadline(time.Now().Add(c.WriteTimeout)); err != nil {
			c.closeConn(cc)
			return true, err
		}
	}

	if c.ReadTimeout > 0 {
		// Set Deadline every time, since golang has fixed the performance issue
		// See https://github.com/golang/go/issues/15133#issuecomment-271571395 for details
		if err = conn.SetReadDeadline(time.Now().Add(c.ReadTimeout)); err != nil {
			c.closeConn(cc)
			return true, err
		}
	}

	resCh := make(chan error, 1)
	go func() {
		if err = cc.client.Call(serviceMethod, args, reply); err != nil {
			if srvErr, ok := err.(rpc.ServerError); ok {
				c.releaseConn(cc)
				resCh <- srvErr
				return
			}

			c.closeConn(cc)
			resCh <- err
			return
		}

		resCh <- nil

		if c.WriteTimeout > 0 || c.ReadTimeout > 0 {
			if err = conn.SetDeadline(time.Time{}); err != nil {
				c.closeConn(cc)
				return
			}
		}
		c.releaseConn(cc)
	}()

	select {
	case <-ctx.Done():
		return false, ctx.Err()
	case res := <-resCh:
		return false, res
	}
}

func (c *HostClient) CloseIdleConnections() {
	c.connsLock.Lock()
	scratch := append([]*rpcClient{}, c.conns...)
	for i := range c.conns {
		c.conns[i] = nil
	}
	c.conns = c.conns[:0]
	c.connsLock.Unlock()

	for _, cc := range scratch {
		c.closeConn(cc)
	}
}

func (c *HostClient) acquireConn(reqTimeout time.Duration, connectionClose bool) (cc *rpcClient, err error) {
	createConn := false
	startCleaner := false

	var n int
	c.connsLock.Lock()
	n = len(c.conns)
	if n == 0 {
		maxConns := c.MaxConns
		if maxConns <= 0 {
			maxConns = DefaultMaxConnsPerHost
		}
		if c.connsCount < maxConns {
			c.connsCount++
			createConn = true
			if !c.connsCleanerRun && !connectionClose {
				startCleaner = true
				c.connsCleanerRun = true
			}
		}
	} else {
		n--
		cc = c.conns[n]
		c.conns[n] = nil
		c.conns = c.conns[:n]
	}
	c.connsLock.Unlock()

	if cc != nil {
		return cc, nil
	}
	if !createConn {
		if c.MaxConnWaitTimeout <= 0 {
			return nil, ErrNoFreeConns
		}

		// reqTimeout    c.MaxConnWaitTimeout   wait duration
		//     d1                 d2            min(d1, d2)
		//  0(not set)            d2            d2
		//     d1            0(don't wait)      0(don't wait)
		//  0(not set)            d2            d2
		timeout := c.MaxConnWaitTimeout
		timeoutOverridden := false
		// reqTimeout == 0 means not set
		if reqTimeout > 0 && reqTimeout < timeout {
			timeout = reqTimeout
			timeoutOverridden = true
		}

		// wait for a free connection
		tc := AcquireTimer(timeout)
		defer ReleaseTimer(tc)

		w := &wantConn{
			ready: make(chan struct{}, 1),
		}
		defer func() {
			if err != nil {
				w.cancel(c, err)
			}
		}()

		c.queueForIdle(w)

		select {
		case <-w.ready:
			return w.conn, w.err
		case <-tc.C:
			if timeoutOverridden {
				return nil, ErrTimeout
			}
			return nil, ErrNoFreeConns
		}
	}

	if startCleaner {
		go c.connsCleaner()
	}

	conn, err := c.dialHostHard()
	if err != nil {
		c.decConnsCount()
		return nil, err
	}
	//cc = acquireClientConn(conn)
	cc = &rpcClient{
		conn:        conn,
		client:      jsonrpc.NewClient(conn),
		lastUseTime: time.Now(),
	}

	return cc, nil
}

func (c *HostClient) releaseConn(cc *rpcClient) {
	cc.lastUseTime = time.Now()
	if c.MaxConnWaitTimeout <= 0 {
		c.connsLock.Lock()
		c.conns = append(c.conns, cc)
		c.connsLock.Unlock()
		return
	}

	// try to deliver an idle connection to a *wantConn
	c.connsLock.Lock()
	defer c.connsLock.Unlock()
	delivered := false
	if q := c.connsWait; q != nil && q.len() > 0 {
		for q.len() > 0 {
			w := q.popFront()
			if w.waiting() {
				delivered = w.tryDeliver(cc, nil)
				break
			}
		}
	}
	if !delivered {
		c.conns = append(c.conns, cc)
	}
}

func (c *HostClient) queueForIdle(w *wantConn) {
	c.connsLock.Lock()
	defer c.connsLock.Unlock()
	if c.connsWait == nil {
		c.connsWait = &wantConnQueue{}
	}
	c.connsWait.clearFront()
	c.connsWait.pushBack(w)
}

func (c *HostClient) decConnsCount() {
	if c.MaxConnWaitTimeout <= 0 {
		c.connsLock.Lock()
		c.connsCount--
		c.connsLock.Unlock()
		return
	}

	c.connsLock.Lock()
	defer c.connsLock.Unlock()
	dialed := false
	if q := c.connsWait; q != nil && q.len() > 0 {
		for q.len() > 0 {
			w := q.popFront()
			if w.waiting() {
				go c.dialConnFor(w)
				dialed = true
				break
			}
		}
	}
	if !dialed {
		c.connsCount--
	}
}

func (c *HostClient) dialConnFor(w *wantConn) {
	conn, err := c.dialHostHard()
	if err != nil {
		w.tryDeliver(nil, err)
		c.decConnsCount()
		return
	}

	cc := &rpcClient{
		conn:        conn,
		client:      jsonrpc.NewClient(conn),
		lastUseTime: time.Now(),
	}
	delivered := w.tryDeliver(cc, nil)
	if !delivered {
		// not delivered, return idle connection
		c.releaseConn(cc)
	}
}

func (c *HostClient) dialHostHard() (conn net.Conn, err error) {
	// attempt to dial all the available hosts before giving up.

	c.addrsLock.Lock()
	n := len(c.addrs)
	c.addrsLock.Unlock()

	if n == 0 {
		// It looks like c.addrs isn't initialized yet.
		n = 1
	}

	timeout := c.ReadTimeout + c.WriteTimeout
	if timeout <= 0 {
		timeout = DefaultDialTimeout
	}
	deadline := time.Now().Add(timeout)
	for n > 0 {
		addr := c.nextAddr()
		conn, err = dialAddr(addr, c.Dial)
		if err == nil {
			return conn, nil
		}
		if time.Since(deadline) >= 0 {
			break
		}
		n--
	}
	return nil, err
}

func (c *HostClient) nextAddr() string {
	c.addrsLock.Lock()
	if c.addrs == nil {
		c.addrs = strings.Split(c.Addr, ",")
	}
	addr := c.addrs[0]
	if len(c.addrs) > 1 {
		addr = c.addrs[c.addrIdx%uint32(len(c.addrs))]
		c.addrIdx++
	}
	c.addrsLock.Unlock()
	return addr
}

func (c *HostClient) closeConn(cc *rpcClient) {
	c.decConnsCount()
	cc.client.Close()
}

func (c *HostClient) connsCleaner() {
	var (
		scratch             []*rpcClient
		maxIdleConnDuration = c.MaxIdleConnDuration
	)
	if maxIdleConnDuration <= 0 {
		maxIdleConnDuration = DefaultMaxIdleConnDuration
	}
	for {
		currentTime := time.Now()

		// Determine idle connections to be closed.
		c.connsLock.Lock()
		conns := c.conns
		n := len(conns)
		i := 0
		for i < n && currentTime.Sub(conns[i].lastUseTime) > maxIdleConnDuration {
			i++
		}
		sleepFor := maxIdleConnDuration
		if i < n {
			// + 1 so we actually sleep past the expiration time and not up to it.
			// Otherwise the > check above would still fail.
			sleepFor = maxIdleConnDuration - currentTime.Sub(conns[i].lastUseTime) + 1
		}
		scratch = append(scratch[:0], conns[:i]...)
		if i > 0 {
			m := copy(conns, conns[i:])
			for i = m; i < n; i++ {
				conns[i] = nil
			}
			c.conns = conns[:m]
		}
		c.connsLock.Unlock()

		// Close idle connections.
		for i, cc := range scratch {
			c.closeConn(cc)
			scratch[i] = nil
		}

		// Determine whether to stop the connsCleaner.
		c.connsLock.Lock()
		mustStop := c.connsCount == 0
		if mustStop {
			c.connsCleanerRun = false
		}
		c.connsLock.Unlock()
		if mustStop {
			break
		}

		time.Sleep(sleepFor)
	}
}

type rpcClient struct {
	conn        net.Conn
	client      *rpc.Client
	lastUseTime time.Time
}

func (rc rpcClient) close() error {
	return rc.client.Close()
}

// A wantConn records state about a wanted connection
// (that is, an active call to getConn).
// The conn may be gotten by dialing or by finding an idle connection,
// or a cancellation may make the conn no longer wanted.
// These three options are racing against each other and use
// wantConn to coordinate and agree about the winning outcome.
//
// inspired by net/http/transport.go
type wantConn struct {
	ready chan struct{}
	mu    sync.Mutex // protects conn, err, close(ready)
	conn  *rpcClient
	err   error
}

// waiting reports whether w is still waiting for an answer (connection or error).
func (w *wantConn) waiting() bool {
	select {
	case <-w.ready:
		return false
	default:
		return true
	}
}

// tryDeliver attempts to deliver conn, err to w and reports whether it succeeded.
func (w *wantConn) tryDeliver(conn *rpcClient, err error) bool {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.conn != nil || w.err != nil {
		return false
	}
	w.conn = conn
	w.err = err
	if w.conn == nil && w.err == nil {
		panic("fasthttp: internal error: misuse of tryDeliver")
	}
	close(w.ready)
	return true
}

// cancel marks w as no longer wanting a result (for example, due to cancellation).
// If a connection has been delivered already, cancel returns it with c.releaseConn.
func (w *wantConn) cancel(c *HostClient, err error) {
	w.mu.Lock()
	if w.conn == nil && w.err == nil {
		close(w.ready) // catch misbehavior in future delivery
	}

	conn := w.conn
	w.conn = nil
	w.err = err
	w.mu.Unlock()

	if conn != nil {
		c.releaseConn(conn)
	}
}

// A wantConnQueue is a queue of wantConns.
//
// inspired by net/http/transport.go
type wantConnQueue struct {
	// This is a queue, not a deque.
	// It is split into two stages - head[headPos:] and tail.
	// popFront is trivial (headPos++) on the first stage, and
	// pushBack is trivial (append) on the second stage.
	// If the first stage is empty, popFront can swap the
	// first and second stages to remedy the situation.
	//
	// This two-stage split is analogous to the use of two lists
	// in Okasaki's purely functional queue but without the
	// overhead of reversing the list when swapping stages.
	head    []*wantConn
	headPos int
	tail    []*wantConn
}

// len returns the number of items in the queue.
func (q *wantConnQueue) len() int {
	return len(q.head) - q.headPos + len(q.tail)
}

// pushBack adds w to the back of the queue.
func (q *wantConnQueue) pushBack(w *wantConn) {
	q.tail = append(q.tail, w)
}

// popFront removes and returns the wantConn at the front of the queue.
func (q *wantConnQueue) popFront() *wantConn {
	if q.headPos >= len(q.head) {
		if len(q.tail) == 0 {
			return nil
		}
		// Pick up tail as new head, clear tail.
		q.head, q.headPos, q.tail = q.tail, 0, q.head[:0]
	}

	w := q.head[q.headPos]
	q.head[q.headPos] = nil
	q.headPos++
	return w
}

// peekFront returns the wantConn at the front of the queue without removing it.
func (q *wantConnQueue) peekFront() *wantConn {
	if q.headPos < len(q.head) {
		return q.head[q.headPos]
	}
	if len(q.tail) > 0 {
		return q.tail[0]
	}
	return nil
}

// cleanFront pops any wantConns that are no longer waiting from the head of the
// queue, reporting whether any were popped.
func (q *wantConnQueue) clearFront() (cleaned bool) {
	for {
		w := q.peekFront()
		if w == nil || w.waiting() {
			return cleaned
		}
		q.popFront()
		cleaned = true
	}
}

type timeoutError struct{}

func (e *timeoutError) Error() string {
	return "timeout"
}

// Timeout only implement the Timeout() function of the net.Error interface.
// This allows for checks like:
//
//   if x, ok := err.(interface{ Timeout() bool }); ok && x.Timeout() {
func (e *timeoutError) Timeout() bool {
	return true
}

// ErrTimeout is returned from timed out calls.
var ErrTimeout = &timeoutError{}

type DialFunc func(addr string) (net.Conn, error)

func Dial(addr string) (net.Conn, error) {
	return net.Dial("tcp", addr)
}

func dialAddr(addr string, dial DialFunc) (net.Conn, error) {
	if dial == nil {
		dial = Dial
	}
	conn, err := dial(addr)
	if err != nil {
		return nil, err
	}
	if conn == nil {
		panic("BUG: DialFunc returned (nil, nil)")
	}
	return conn, nil
}
