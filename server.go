package jsonrpc

import (
	_ "embed" // make linter happy
	"net/rpc/jsonrpc"
	"sync/atomic"

	"context"
	"fmt"
	"net"
	"net/rpc"
	"strings"
	"sync"
	"time"
)

type Server struct {
	IdleTimeout    time.Duration
	WriteTimeout   time.Duration
	ReadTimeout    time.Duration
	Receivers      []interface{}
	NamedReceivers map[string]interface{}

	s *rpc.Server
	l net.Listener

	connsMux sync.Mutex
	connsWg  sync.WaitGroup
	conns    map[uint64]*serverConn

	closeCh chan struct{}
}

func (s *Server) ListenAndServe(addr string) error {
	l, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}

	return s.Serve(l)
}

func (s *Server) Serve(l net.Listener) error {
	s.connsMux.Lock()
	if s.l != nil {
		return fmt.Errorf("already serving or shutdown")
	}
	s.l = l
	s.connsMux.Unlock()

	if s.IdleTimeout == 0 {
		s.IdleTimeout = time.Second * 60
	}
	if s.WriteTimeout == 0 {
		s.WriteTimeout = time.Second * 5
	}
	if s.ReadTimeout == 0 {
		s.ReadTimeout = time.Second * 5
	}
	s.s = rpc.NewServer()
	s.closeCh = make(chan struct{})
	s.conns = make(map[uint64]*serverConn)

	for _, rcvr := range s.Receivers {
		if err := s.s.Register(rcvr); err != nil {
			return fmt.Errorf("register: %w", err)
		}
	}

	for n, rcvr := range s.NamedReceivers {
		if err := s.s.RegisterName(n, rcvr); err != nil {
			return fmt.Errorf("register name: %w", err)
		}
	}

	if s.IdleTimeout > 0 {
		go s.cleanupConns()
	}

	var index uint64
	for {
		select {
		case <-s.closeCh:
			return nil
		default:
		}

		conn, err := s.l.Accept()
		if err != nil && strings.Contains(err.Error(), "use of closed network connection") {
			return nil
		} else if err != nil {
			return err
		}

		index++
		conn1 := &serverConn{
			conn:         conn,
			codec:        jsonrpc.NewServerCodec(conn),
			lastUsedAt:   time.Now().Unix(),
			writeTimeout: s.WriteTimeout,
			readTimeout:  s.ReadTimeout,
		}

		s.connsMux.Lock()
		s.conns[index] = conn1
		s.connsMux.Unlock()

		s.connsWg.Add(1)
		go func(index uint64, conn net.Conn) {
			defer s.connsWg.Done()
			defer func() {
				s.connsMux.Lock()
				delete(s.conns, index)
				s.connsMux.Unlock()
			}()

			s.s.ServeCodec(conn1)
		}(index, conn)
	}
}

func (s *Server) Shutdown(ctx context.Context) error {
	s.connsMux.Lock()
	if s.l != nil {
		_ = s.l.Close()
	}
	for _, conn := range s.conns {
		conn.Shutdown()
	}
	s.connsMux.Unlock()

	go func() {
		s.connsWg.Wait()
		close(s.closeCh)
	}()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-s.closeCh:
		return nil
	}
}

func (s *Server) cleanupConns() {
	t := time.NewTicker(time.Second * 10)
	defer t.Stop()

	for {
		select {
		case <-t.C:
			s.connsMux.Lock()
			now := time.Now()
			for i, conn := range s.conns {
				t := time.Unix(atomic.LoadInt64(&conn.lastUsedAt), 0)
				if t.Add(s.IdleTimeout).Before(now) {
					delete(s.conns, i)
					conn.Shutdown()
				}
			}
			s.connsMux.Unlock()
		case <-s.closeCh:
			return
		}
	}
}

type serverConn struct {
	conn         net.Conn
	codec        rpc.ServerCodec
	lastUsedAt   int64
	writeTimeout time.Duration
	readTimeout  time.Duration
}

func (c *serverConn) ReadRequestHeader(req *rpc.Request) error {
	atomic.StoreInt64(&c.lastUsedAt, time.Now().Unix())
	if err := c.codec.ReadRequestHeader(req); err != nil {
		return err
	}

	return nil
}

func (c *serverConn) ReadRequestBody(args any) error {
	if c.readTimeout > 0 {
		if err := c.conn.SetReadDeadline(time.Now().Add(c.readTimeout)); err != nil {
			return err
		}
	}

	if err := c.codec.ReadRequestBody(args); err != nil {
		return err
	}

	if c.readTimeout > 0 {
		if err := c.conn.SetReadDeadline(time.Time{}); err != nil {
			return err
		}
	}

	return nil
}

func (c *serverConn) WriteResponse(resp *rpc.Response, reply any) error {
	if c.writeTimeout > 0 {
		if err := c.conn.SetWriteDeadline(time.Now().Add(c.writeTimeout)); err != nil {
			return err
		}
	}

	if err := c.codec.WriteResponse(resp, reply); err != nil {
		return err
	}

	if c.writeTimeout > 0 {
		if err := c.conn.SetWriteDeadline(time.Time{}); err != nil {
			return err
		}
	}

	return nil
}

func (c *serverConn) Shutdown() {
	// for graceful task handling we close only read channel
	// so if task is processing right now response will be sent
	// to still open write channel and codec will close on next read
	_ = c.conn.SetReadDeadline(time.Now())
}

func (c *serverConn) Close() error {
	return c.codec.Close()
}
