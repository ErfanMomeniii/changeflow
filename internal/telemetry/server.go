package telemetry

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Health describes whether a stream is fit to serve.
//
// Liveness and readiness are separated deliberately: a stream that is behind is
// still alive and must not be restarted, since restarting it would only make it
// further behind.
type Health struct {
	// Ready reports whether the stream is connected and keeping up. A nil function
	// means always ready.
	Ready func() error
}

// Server exposes metrics and health over HTTP.
type Server struct {
	addr   string
	reg    *prometheus.Registry
	health Health

	mu       sync.Mutex
	listener net.Listener
	server   *http.Server
}

// NewServer prepares the endpoints. Nothing listens until Start is called.
func NewServer(addr string, reg *prometheus.Registry, health Health) *Server {
	return &Server{addr: addr, reg: reg, health: health}
}

// Start begins listening and serves until the context is cancelled.
//
// The listener is opened synchronously so a port already in use fails at startup
// rather than leaving a process running with no way to observe it.
func (s *Server) Start(ctx context.Context) error {
	listener, err := net.Listen("tcp", s.addr)
	if err != nil {
		return fmt.Errorf("telemetry: listen on %s: %w", s.addr, err)
	}

	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(s.reg, promhttp.HandlerOpts{
		// A broken collector should not take the whole endpoint down during an
		// incident, which is exactly when it is being read.
		ErrorHandling: promhttp.ContinueOnError,
	}))

	// Liveness: the process is running and can serve. Deliberately says nothing about
	// lag, so a backlog cannot trigger a restart loop that prevents recovery.
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok\n"))
	})

	// Readiness: connected and keeping up.
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, _ *http.Request) {
		if s.health.Ready == nil {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("ok\n"))
			return
		}
		if err := s.health.Ready(); err != nil {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = fmt.Fprintf(w, "not ready: %v\n", err)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok\n"))
	})

	server := &http.Server{
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second,
	}

	s.mu.Lock()
	s.listener, s.server = listener, server
	s.mu.Unlock()

	done := make(chan error, 1)
	go func() { done <- server.Serve(listener) }()

	select {
	case <-ctx.Done():
		return s.Shutdown()
	case err := <-done:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return fmt.Errorf("telemetry: serve: %w", err)
	}
}

// Addr returns the address actually being listened on, which is how a caller finds
// the port when the configured one was zero.
func (s *Server) Addr() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.listener == nil {
		return s.addr
	}
	return s.listener.Addr().String()
}

// Shutdown stops serving.
func (s *Server) Shutdown() error {
	s.mu.Lock()
	server := s.server
	s.mu.Unlock()

	if server == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := server.Shutdown(ctx); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return fmt.Errorf("telemetry: shut down: %w", err)
	}
	return nil
}
