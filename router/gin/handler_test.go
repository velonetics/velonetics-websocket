package gin

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/gin-gonic/gin"
	"github.com/pucora/lura/v2/config"
	"github.com/pucora/lura/v2/logging"
	"github.com/pucora/lura/v2/proxy"
	pucoragin "github.com/pucora/lura/v2/router/gin"
	ws "github.com/pucora/pucora-websocket/v2"
)

func TestDirectWebSocketProxy(t *testing.T) {
	gin.SetMode(gin.TestMode)

	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := websocket.Accept(w, r, nil)
		if err != nil {
			t.Errorf("backend accept: %v", err)
			return
		}
		defer c.Close(websocket.StatusNormalClosure, "bye")
		ctx := r.Context()
		for {
			typ, msg, err := c.Read(ctx)
			if err != nil {
				return
			}
			if err := c.Write(ctx, typ, msg); err != nil {
				return
			}
		}
	}))
	defer backend.Close()

	wsBackend := "ws" + strings.TrimPrefix(backend.URL, "http")

	engine := gin.New()
	endpoint := &config.EndpointConfig{
		Endpoint: "/echo",
		Method:   http.MethodGet,
		Backend: []*config.Backend{{
			Host:                     []string{wsBackend},
			URLPattern:               "/",
			HostSanitizationDisabled: true,
		}},
		ExtraConfig: config.ExtraConfig{
			"websocket": map[string]interface{}{
				"enable_direct_communication": true,
				"max_message_size":            float64(1024),
			},
		},
	}

	hf := HandlerFactory(logging.NoOp, pucoragin.EndpointHandler)
	engine.GET(endpoint.Endpoint, hf(endpoint, proxy.NoopProxy))

	gw := httptest.NewServer(engine)
	defer gw.Close()
	wsGateway := "ws" + strings.TrimPrefix(gw.URL, "http")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	client, _, err := websocket.Dial(ctx, wsGateway+"/echo", nil)
	if err != nil {
		t.Fatalf("dial gateway: %v", err)
	}
	defer client.Close(websocket.StatusNormalClosure, "bye")

	if err := client.Write(ctx, websocket.MessageText, []byte("ping")); err != nil {
		t.Fatalf("write: %v", err)
	}
	_, msg, err := client.Read(ctx)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if string(msg) != "ping" {
		t.Fatalf("unexpected response: %s", string(msg))
	}
}

func TestMultiplexHandshakeAndRelay(t *testing.T) {
	gin.SetMode(gin.TestMode)
	t.Cleanup(ws.ResetHubRegistry)

	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := websocket.Accept(w, r, nil)
		if err != nil {
			t.Errorf("backend accept: %v", err)
			return
		}
		defer c.Close(websocket.StatusNormalClosure, "bye")
		ctx := r.Context()

		_, data, err := c.Read(ctx)
		if err != nil {
			t.Errorf("handshake read: %v", err)
			return
		}
		if string(data) != `{"msg":"Pucora WS proxy starting"}` {
			t.Errorf("unexpected handshake: %s", string(data))
			return
		}
		if err := c.Write(ctx, websocket.MessageText, []byte("OK")); err != nil {
			t.Errorf("handshake write: %v", err)
			return
		}

		_, data, err = c.Read(ctx)
		if err != nil {
			t.Errorf("envelope read: %v", err)
			return
		}
		if !strings.Contains(string(data), "aGVsbG8=") {
			t.Errorf("unexpected envelope: %s", string(data))
			return
		}
		if err := c.Write(ctx, websocket.MessageText, []byte(`{"body":"aGVsbG8="}`)); err != nil {
			t.Errorf("backend write: %v", err)
		}
	}))
	defer backend.Close()

	wsBackend := "ws" + strings.TrimPrefix(backend.URL, "http")

	engine := gin.New()
	endpoint := &config.EndpointConfig{
		Endpoint: "/echo",
		Method:   http.MethodGet,
		Backend: []*config.Backend{{
			Host:                     []string{wsBackend},
			URLPattern:               "/",
			HostSanitizationDisabled: true,
		}},
		ExtraConfig: config.ExtraConfig{
			"websocket": map[string]interface{}{
				"max_message_size": float64(1024),
			},
		},
	}

	hf := HandlerFactory(logging.NoOp, pucoragin.EndpointHandler)
	engine.GET(endpoint.Endpoint, hf(endpoint, proxy.NoopProxy))

	gw := httptest.NewServer(engine)
	defer gw.Close()
	wsGateway := "ws" + strings.TrimPrefix(gw.URL, "http")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	client, _, err := websocket.Dial(ctx, wsGateway+"/echo", nil)
	if err != nil {
		t.Fatalf("dial gateway: %v", err)
	}
	defer client.Close(websocket.StatusNormalClosure, "bye")

	if err := client.Write(ctx, websocket.MessageText, []byte("hello")); err != nil {
		t.Fatalf("write: %v", err)
	}
	_, msg, err := client.Read(ctx)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if string(msg) != "hello" {
		t.Fatalf("unexpected response: %s", string(msg))
	}
}

func TestMultiplexQueuesUntilBackendReturns(t *testing.T) {
	gin.SetMode(gin.TestMode)
	t.Cleanup(ws.ResetHubRegistry)

	var mu sync.Mutex
	connCount := 0

	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		ctx := r.Context()

		_, data, err := c.Read(ctx)
		if err != nil {
			_ = c.Close(websocket.StatusNormalClosure, "bye")
			return
		}
		if string(data) != `{"msg":"Pucora WS proxy starting"}` {
			_ = c.Close(websocket.StatusNormalClosure, "bye")
			return
		}
		if err := c.Write(ctx, websocket.MessageText, []byte("OK")); err != nil {
			_ = c.Close(websocket.StatusNormalClosure, "bye")
			return
		}

		mu.Lock()
		connCount++
		n := connCount
		mu.Unlock()

		if n == 1 {
			_ = c.Close(websocket.StatusNormalClosure, "bye")
			return
		}

		defer c.Close(websocket.StatusNormalClosure, "bye")
		_, data, err = c.Read(ctx)
		if err != nil {
			return
		}
		if !strings.Contains(string(data), "cXVldWVk") {
			t.Errorf("expected queued payload, got %s", string(data))
			return
		}
		_ = c.Write(ctx, websocket.MessageText, []byte(`{"body":"cXVldWVk"}`))
	}))
	defer backend.Close()

	wsBackend := "ws" + strings.TrimPrefix(backend.URL, "http")

	engine := gin.New()
	endpoint := &config.EndpointConfig{
		Endpoint: "/queue",
		Method:   http.MethodGet,
		Backend: []*config.Backend{{
			Host:                     []string{wsBackend},
			URLPattern:               "/",
			HostSanitizationDisabled: true,
		}},
		ExtraConfig: config.ExtraConfig{
			"websocket": map[string]interface{}{
				"max_message_size":    float64(1024),
				"message_buffer_size": float64(8),
				"max_retries":         float64(10),
				"backoff_strategy":    "fallback",
			},
		},
	}

	hf := HandlerFactory(logging.NoOp, pucoragin.EndpointHandler)
	engine.GET(endpoint.Endpoint, hf(endpoint, proxy.NoopProxy))

	gw := httptest.NewServer(engine)
	defer gw.Close()
	wsGateway := "ws" + strings.TrimPrefix(gw.URL, "http")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	client, _, err := websocket.Dial(ctx, wsGateway+"/queue", nil)
	if err != nil {
		t.Fatalf("dial gateway: %v", err)
	}
	defer client.Close(websocket.StatusNormalClosure, "bye")

	time.Sleep(100 * time.Millisecond)

	if err := client.Write(ctx, websocket.MessageText, []byte("queued")); err != nil {
		t.Fatalf("write: %v", err)
	}

	deadline := time.Now().Add(5 * time.Second)
	for {
		readCtx, readCancel := context.WithTimeout(ctx, 500*time.Millisecond)
		_, msg, err := client.Read(readCtx)
		readCancel()
		if err == nil {
			if string(msg) != "queued" {
				t.Fatalf("unexpected response: %s", string(msg))
			}
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for queued message delivery: %v", err)
		}
	}
}

func TestMaxMessageSizeDisconnect(t *testing.T) {
	gin.SetMode(gin.TestMode)

	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer c.Close(websocket.StatusNormalClosure, "bye")
		ctx := r.Context()
		for {
			if _, _, err := c.Read(ctx); err != nil {
				return
			}
		}
	}))
	defer backend.Close()

	wsBackend := "ws" + strings.TrimPrefix(backend.URL, "http")
	engine := gin.New()
	endpoint := &config.EndpointConfig{
		Endpoint: "/echo",
		Method:   http.MethodGet,
		Backend: []*config.Backend{{
			Host:                     []string{wsBackend},
			URLPattern:               "/",
			HostSanitizationDisabled: true,
		}},
		ExtraConfig: config.ExtraConfig{
			"websocket": map[string]interface{}{
				"enable_direct_communication": true,
				"max_message_size":            float64(8),
			},
		},
	}

	hf := HandlerFactory(logging.NoOp, pucoragin.EndpointHandler)
	engine.GET(endpoint.Endpoint, hf(endpoint, proxy.NoopProxy))
	gw := httptest.NewServer(engine)
	defer gw.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	client, _, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(gw.URL, "http")+"/echo", nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer client.Close(websocket.StatusNormalClosure, "bye")

	large := []byte("this message is definitely longer than eight bytes")
	if err := client.Write(ctx, websocket.MessageText, large); err != nil {
		t.Fatalf("write: %v", err)
	}
	_, _, err = client.Read(ctx)
	if err == nil {
		t.Fatal("expected connection to close after oversized message")
	}
}

func TestRejectNonUpgradeRequest(t *testing.T) {
	gin.SetMode(gin.TestMode)
	engine := gin.New()
	endpoint := &config.EndpointConfig{
		Endpoint: "/echo",
		Method:   http.MethodGet,
		Backend: []*config.Backend{{
			Host:                     []string{"ws://127.0.0.1:9"},
			URLPattern:               "/",
			HostSanitizationDisabled: true,
		}},
		ExtraConfig: config.ExtraConfig{
			"websocket": map[string]interface{}{},
		},
	}
	hf := HandlerFactory(logging.NoOp, pucoragin.EndpointHandler)
	engine.GET(endpoint.Endpoint, hf(endpoint, proxy.NoopProxy))
	gw := httptest.NewServer(engine)
	defer gw.Close()

	resp, err := http.Get(gw.URL + "/echo")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", resp.StatusCode)
	}
}
