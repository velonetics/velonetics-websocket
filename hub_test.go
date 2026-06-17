package websocket

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/velonetics/lura/v2/logging"
)

func newTestHub(endpoint string, cfg Config) *Hub {
	h := &Hub{
		endpoint: endpoint,
		cfg:      cfg,
		logger:   logging.NoOp,
		clients:  make(map[string]*ClientSession),
		backoff:  newBackoff(cfg.BackoffStrategy),
	}
	h.lifecycleCtx, h.lifecycleCancel = context.WithCancel(context.Background())
	return h
}

func TestClientSessionQueueInbound(t *testing.T) {
	s := newClientSession("id", "/echo", map[string]interface{}{"uuid": "id"}, nil, 2)
	if !s.queueInbound([]byte("a")) {
		t.Fatal("expected first enqueue to succeed")
	}
	if !s.queueInbound([]byte("b")) {
		t.Fatal("expected second enqueue to succeed")
	}
	if s.queueInbound([]byte("c")) {
		t.Fatal("expected third enqueue to fail when buffer is full")
	}
	got := s.drainInbound()
	if len(got) != 2 {
		t.Fatalf("expected 2 pending messages, got %d", len(got))
	}
}

func TestHubFlushPendingAfterReconnect(t *testing.T) {
	var backendMu sync.Mutex
	acceptCount := 0
	backendDown := true

	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if backendDown {
			http.Error(w, "down", http.StatusServiceUnavailable)
			return
		}
		c, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer c.Close(websocket.StatusNormalClosure, "bye")
		ctx := r.Context()

		backendMu.Lock()
		acceptCount++
		backendMu.Unlock()

		_, data, err := c.Read(ctx)
		if err != nil {
			return
		}
		if string(data) != handshakeMessage {
			t.Errorf("unexpected handshake payload: %s", string(data))
			return
		}
		_ = c.Write(ctx, websocket.MessageText, []byte(handshakeOK))

		_, data, err = c.Read(ctx)
		if err != nil {
			return
		}
		if !strings.Contains(string(data), "cXVldWVk") {
			t.Errorf("expected queued message envelope, got %s", string(data))
		}
	}))
	defer backend.Close()

	wsURL := "ws" + strings.TrimPrefix(backend.URL, "http")
	cfg := Config{
		MaxRetries:        3,
		BackoffStrategy:   "fallback",
		MessageBufferSize: 4,
		MaxMessageSize:    1024,
		WriteWait:         5 * time.Second,
	}
	hub := newTestHub("/echo", cfg)
	defer hub.lifecycleCancel()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	hub.backendURL = wsURL
	hub.markBackendDown()

	clientSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = websocket.Accept(w, r, nil)
	}))
	defer clientSrv.Close()
	clientConn, _, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(clientSrv.URL, "http"), nil)
	if err != nil {
		t.Fatalf("client dial: %v", err)
	}
	defer clientConn.Close(websocket.StatusNormalClosure, "bye")

	s := newClientSession("sess-1", "/echo", map[string]interface{}{"uuid": "sess-1"}, clientConn, cfg.MessageBufferSize)
	hub.registerClient(s)

	env := NewOutboundEnvelope("/echo", s.session, []byte("queued"))
	data, _ := EncodeEnvelope(env)
	if !s.queueInbound(data) {
		t.Fatal("expected queue to accept message while backend is down")
	}

	backendDown = false
	if err := hub.connectBackend(ctx); err != nil {
		t.Fatalf("connect backend: %v", err)
	}
	hub.flushAllPending(ctx)

	deadline := time.Now().Add(2 * time.Second)
	for {
		backendMu.Lock()
		count := acceptCount
		backendMu.Unlock()
		if count > 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("backend never received flushed message")
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func TestConnectBackendSerialized(t *testing.T) {
	var dials atomic.Int32
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		dials.Add(1)
		c, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer c.Close(websocket.StatusNormalClosure, "bye")
		ctx := r.Context()
		_, data, err := c.Read(ctx)
		if err != nil {
			return
		}
		if string(data) == handshakeMessage {
			_ = c.Write(ctx, websocket.MessageText, []byte(handshakeOK))
		}
		time.Sleep(200 * time.Millisecond)
	}))
	defer backend.Close()

	wsURL := "ws" + strings.TrimPrefix(backend.URL, "http")
	cfg := Config{MaxRetries: 1, BackoffStrategy: "fallback", WriteWait: time.Second}
	hub := newTestHub("/echo", cfg)
	defer hub.lifecycleCancel()
	hub.backendURL = wsURL

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = hub.connectBackend(ctx)
		}()
	}
	wg.Wait()

	if got := dials.Load(); got != 1 {
		t.Fatalf("expected exactly one backend dial, got %d", got)
	}
}

func TestBackendReadSurvivesClientContextCancellation(t *testing.T) {
	backendReady := make(chan struct{})
	pushMsg := make(chan struct{})

	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer c.Close(websocket.StatusNormalClosure, "bye")
		ctx := r.Context()

		_, data, err := c.Read(ctx)
		if err != nil || string(data) != handshakeMessage {
			return
		}
		if err := c.Write(ctx, websocket.MessageText, []byte(handshakeOK)); err != nil {
			return
		}
		close(backendReady)

		select {
		case <-pushMsg:
			_ = c.Write(ctx, websocket.MessageText, []byte("broadcast-payload"))
		case <-ctx.Done():
		}
		<-ctx.Done()
	}))
	defer backend.Close()

	wsURL := "ws" + strings.TrimPrefix(backend.URL, "http")
	cfg := Config{
		Timeout:         5 * time.Second,
		WriteWait:       5 * time.Second,
		MaxMessageSize:  1024,
		MessageBufferSize: 4,
	}
	hub := newTestHub("/echo", cfg)
	defer hub.lifecycleCancel()
	hub.backendURL = wsURL

	clientCtx, clientCancel := context.WithCancel(context.Background())
	if err := hub.connectBackend(clientCtx); err != nil {
		t.Fatalf("connect backend: %v", err)
	}

	select {
	case <-backendReady:
	case <-time.After(2 * time.Second):
		t.Fatal("backend never became ready")
	}
	clientCancel()

	client2 := newClientSession("sess-2", "/echo", map[string]interface{}{"uuid": "sess-2"}, nil, cfg.MessageBufferSize)
	hub.registerClient(client2)

	close(pushMsg)

	select {
	case msg := <-client2.outbox:
		if string(msg) != "broadcast-payload" {
			t.Fatalf("unexpected payload: %q", string(msg))
		}
	case <-time.After(2 * time.Second):
		t.Fatal("backend read loop stopped after client context was cancelled")
	}
}
