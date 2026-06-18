package websocket

import (
	"context"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/coder/websocket"
	"github.com/velonetics/lura/v2/logging"
)

const (
	handshakeMessage = `{"msg":"Velonetics WS proxy starting"}`
	handshakeOK      = "OK"
)

type Hub struct {
	endpoint string
	cfg      Config
	logger   logging.Logger
	metrics  *Metrics

	mu        sync.RWMutex
	clients   map[string]*ClientSession
	backendMu sync.Mutex

	backendURL string
	headers    http.Header
	backend    *websocket.Conn
	alive      bool
	retries    int
	backoff    backoffFunc
	writeMu    sync.Mutex

	connectMu   sync.Mutex
	reconnectMu sync.Mutex

	lifecycleCtx    context.Context
	lifecycleCancel context.CancelFunc
}

var hubRegistry sync.Map

func GetHub(endpoint string, cfg Config, logger logging.Logger) *Hub {
	if existing, ok := hubRegistry.Load(endpoint); ok {
		return existing.(*Hub)
	}
	h := &Hub{
		endpoint: endpoint,
		cfg:      cfg,
		logger:   logger,
		metrics:  getMetrics(cfg.DisableOTELMetrics),
		clients:  make(map[string]*ClientSession),
		backoff:  newBackoff(cfg.BackoffStrategy),
	}
	h.lifecycleCtx, h.lifecycleCancel = context.WithCancel(context.Background())
	actual, _ := hubRegistry.LoadOrStore(endpoint, h)
	return actual.(*Hub)
}

func (h *Hub) registerClient(s *ClientSession) {
	h.mu.Lock()
	h.clients[s.id] = s
	h.mu.Unlock()
	if h.metrics != nil {
		h.metrics.connectionOpened(context.Background(), h.endpoint)
	}
}

func (h *Hub) unregisterClient(id string) {
	h.mu.Lock()
	if s, ok := h.clients[id]; ok {
		delete(h.clients, id)
		s.close()
	}
	h.mu.Unlock()
	if h.metrics != nil {
		h.metrics.connectionClosed(context.Background(), h.endpoint)
	}
}

func (h *Hub) EnsureBackend(ctx context.Context, url string, headers http.Header) error {
	h.backendMu.Lock()
	h.backendURL = url
	h.headers = cloneHeader(headers)
	h.backendMu.Unlock()

	if err := h.connectBackend(ctx); err != nil {
		return err
	}
	h.flushAllPending(ctx)
	return nil
}

func (h *Hub) RegisterClient(s *ClientSession) {
	h.registerClient(s)
}

func (h *Hub) HandleClient(ctx context.Context, s *ClientSession, endpointURL string) {
	h.handleClient(ctx, s, endpointURL)
}

func (h *Hub) connectBackend(ctx context.Context) error {
	h.connectMu.Lock()
	defer h.connectMu.Unlock()

	h.backendMu.Lock()
	url := h.backendURL
	headers := cloneHeader(h.headers)
	h.backendMu.Unlock()

	for {
		if h.isAlive() {
			return nil
		}

		conn, err := h.dialBackend(ctx, url, headers)
		if err != nil {
			h.retries++
			if h.cfg.MaxRetries > 0 && h.retries > h.cfg.MaxRetries {
				h.logger.Critical("[SERVICE: Websocket]", "Unable to reconnect to the backend:", err.Error())
				return err
			}
			h.logger.Error("[SERVICE: Websocket]", "Unable to renew the connection:", err.Error())
			if h.metrics != nil {
				h.metrics.reconnect(ctx, h.endpoint)
			}
			delay := h.backoff(h.retries)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(delay):
			}
			continue
		}

		if err := h.handshake(ctx, conn); err != nil {
			_ = conn.Close(websocket.StatusPolicyViolation, "handshake failed")
			h.retries++
			if h.cfg.MaxRetries > 0 && h.retries > h.cfg.MaxRetries {
				h.logger.Critical("[SERVICE: Websocket]", "Handshake failed:", err.Error())
				return err
			}
			delay := h.backoff(h.retries)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(delay):
			}
			continue
		}

		h.setBackend(conn)
		h.retries = 0
		go h.readBackend(h.lifecycleCtx, conn)
		return nil
	}
}

func (h *Hub) isAlive() bool {
	h.backendMu.Lock()
	defer h.backendMu.Unlock()
	return h.alive && h.backend != nil
}

func (h *Hub) setBackend(conn *websocket.Conn) {
	h.backendMu.Lock()
	h.backend = conn
	h.alive = true
	h.backendMu.Unlock()
}

func (h *Hub) markBackendDown() {
	h.backendMu.Lock()
	if h.backend != nil {
		_ = h.backend.Close(websocket.StatusGoingAway, "backend unavailable")
		h.backend = nil
	}
	h.alive = false
	h.backendMu.Unlock()
}

func (h *Hub) scheduleReconnect() {
	if !h.reconnectMu.TryLock() {
		return
	}
	go func() {
		defer h.reconnectMu.Unlock()
		if err := h.connectBackend(h.lifecycleCtx); err != nil {
			return
		}
		h.flushAllPending(h.lifecycleCtx)
	}()
}

func (h *Hub) dialBackend(ctx context.Context, url string, headers http.Header) (*websocket.Conn, error) {
	opts := dialOptions(h.cfg, headers)
	conn, _, err := websocket.Dial(ctx, url, opts)
	return conn, err
}

func (h *Hub) handshake(ctx context.Context, conn *websocket.Conn) error {
	if err := conn.Write(ctx, websocket.MessageText, []byte(handshakeMessage)); err != nil {
		return err
	}
	readCtx, cancel := readContext(ctx, h.cfg.WriteWait)
	defer cancel()
	_, data, err := conn.Read(readCtx)
	if err != nil {
		return err
	}
	if string(data) != handshakeOK {
		return fmt.Errorf("expected %q handshake response, got %q", handshakeOK, string(data))
	}
	return nil
}

func (h *Hub) readBackend(ctx context.Context, conn *websocket.Conn) {
	defer func() {
		h.backendMu.Lock()
		if h.backend == conn {
			h.backend = nil
			h.alive = false
		}
		h.backendMu.Unlock()
		_ = conn.Close(websocket.StatusNormalClosure, "closed")
		h.logger.Warning("[SERVICE: Websocket][Client]", "Reading from the connection: backend closed")
		h.scheduleReconnect()
	}()

	for {
		readCtx, cancel := readContext(ctx, h.cfg.Timeout)
		_, data, err := conn.Read(readCtx)
		cancel()
		if err != nil {
			h.logger.Warning("[SERVICE: Websocket][Client]", "Reading from the connection:", err.Error())
			return
		}
		h.routeFromBackend(data)
	}
}

func (h *Hub) routeFromBackend(data []byte) {
	env, err := DecodeEnvelope(data)
	if err != nil {
		h.broadcast(data)
		return
	}
	payload, err := env.Payload()
	if err != nil {
		h.broadcast(data)
		return
	}
	if len(env.URL) == 0 && len(env.Session) == 0 {
		h.broadcast(payload)
		return
	}

	h.mu.RLock()
	defer h.mu.RUnlock()
	for _, client := range h.clients {
		if env.URL != "" && client.url != env.URL {
			continue
		}
		if !matchSession(env.Session, client.session) {
			continue
		}
		if h.metrics != nil {
			h.metrics.messageOut(context.Background(), h.endpoint)
		}
		h.deliverToClient(client, payload)
	}
}

func (h *Hub) broadcast(msg []byte) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	for _, client := range h.clients {
		if h.metrics != nil {
			h.metrics.messageOut(context.Background(), h.endpoint)
		}
		h.deliverToClient(client, msg)
	}
}

func (h *Hub) deliverToClient(client *ClientSession, payload []byte) {
	if client.enqueue(payload) {
		return
	}
	h.logger.Warning("[SERVICE: Websocket][Client]", "Client outbox full; message dropped")
}

func (h *Hub) sendToBackend(ctx context.Context, s *ClientSession, env Envelope) error {
	data, err := EncodeEnvelope(env)
	if err != nil {
		return err
	}
	if err := h.writeBackend(ctx, data); err == nil {
		return nil
	} else if err.Error() != errEmptyConnection {
		return err
	}

	h.markBackendDown()
	if s != nil && s.queueInbound(data) {
		h.scheduleReconnect()
		return nil
	}
	return err
}

const errEmptyConnection = "empty connection"

func (h *Hub) writeBackend(ctx context.Context, data []byte) error {
	h.backendMu.Lock()
	conn := h.backend
	alive := h.alive
	h.backendMu.Unlock()
	if !alive || conn == nil {
		return fmt.Errorf(errEmptyConnection)
	}
	h.writeMu.Lock()
	defer h.writeMu.Unlock()
	writeCtx, cancel := readContext(ctx, h.cfg.WriteWait)
	defer cancel()
	return conn.Write(writeCtx, websocket.MessageText, data)
}

func (h *Hub) flushAllPending(ctx context.Context) {
	h.mu.RLock()
	clients := make([]*ClientSession, 0, len(h.clients))
	for _, c := range h.clients {
		clients = append(clients, c)
	}
	h.mu.RUnlock()

	for _, client := range clients {
		pending := client.drainInbound()
		for i, data := range pending {
			if err := h.writeBackend(ctx, data); err != nil {
				if err.Error() == errEmptyConnection {
					for j := i; j < len(pending); j++ {
						if !client.requeueInbound(pending[j]) {
							h.logger.Warning("[SERVICE: Websocket]", "Unable to requeue pending message; inbox full")
						}
					}
					h.markBackendDown()
					h.scheduleReconnect()
					return
				}
				h.logger.Error("[SERVICE: Websocket]", "Writing queued request:", err.Error())
			}
		}
	}
}

func (h *Hub) handleClient(ctx context.Context, s *ClientSession, endpointURL string) {
	defer h.unregisterClient(s.id)

	if h.cfg.ConnectEvent {
		env := NewOutboundEnvelope(endpointURL, s.session, nil)
		if err := h.sendToBackend(ctx, s, env); err != nil && h.cfg.ReturnErrorDetails {
			_ = s.enqueue(clientErrorJSON(err.Error()))
		}
	}

	go s.writePump(ctx, h.cfg.MaxMessageSize, h.cfg.WriteWait)

	for {
		readCtx, cancel := readContext(ctx, h.cfg.Timeout)
		typ, data, err := s.conn.Read(readCtx)
		cancel()
		if err != nil {
			h.logger.Warning("[SERVICE: Websocket][Client]", "Reading from the connection:", err.Error())
			break
		}
		if h.cfg.MaxMessageSize > 0 && int64(len(data)) > h.cfg.MaxMessageSize {
			_ = s.conn.Close(websocket.StatusMessageTooBig, "message too big")
			break
		}
		if h.metrics != nil {
			h.metrics.messageIn(ctx, h.endpoint)
		}

		if typ != websocket.MessageText && typ != websocket.MessageBinary {
			continue
		}
		s.setOutboundFrameType(typ)

		env := NewOutboundEnvelope(endpointURL, s.session, data)
		if err := h.sendToBackend(ctx, s, env); err != nil {
			h.logger.Error("[SERVICE: Websocket]", "Writing request:", err.Error())
			if h.cfg.ReturnErrorDetails {
				_ = s.enqueue(clientErrorJSON(err.Error()))
			}
		}
	}

	if h.cfg.DisconnectEvent {
		env := NewOutboundEnvelope(endpointURL, s.session, nil)
		_ = h.sendToBackend(h.lifecycleCtx, s, env)
	}
}

func cloneHeader(headers http.Header) http.Header {
	if len(headers) == 0 {
		return make(http.Header)
	}
	h := make(http.Header, len(headers))
	for k, vs := range headers {
		h[k] = append([]string(nil), vs...)
	}
	return h
}
