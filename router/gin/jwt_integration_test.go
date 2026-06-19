package gin

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/gin-gonic/gin"
	jose "github.com/pucora/pucora-jose/v2"
	ginjose "github.com/pucora/pucora-jose/v2/gin"
	"github.com/pucora/lura/v2/config"
	"github.com/pucora/lura/v2/logging"
	"github.com/pucora/lura/v2/proxy"
	pucoragin "github.com/pucora/lura/v2/router/gin"
)

const testJWT = "eyJhbGciOiJSUzI1NiIsImtpZCI6IjIwMTEtMDQtMjkiLCJ0eXAiOiJKV1QifQ.eyJhdWQiOiJodHRwOi8vYXBpLmV4YW1wbGUuY29tIiwiZXhwIjoyMDUxODgyNzU1LCJpc3MiOiJodHRwOi8vZXhhbXBsZS5jb20iLCJqdGkiOiJtbmIyM3Zjc3J0NzU2eXVpb21uYnZjeDk4ZXJ0eXVpb3AiLCJyb2xlcyI6WyJyb2xlX2EiLCJyb2xlX2IiXSwic3ViIjoiMTIzNDU2Nzg5MHF3ZXJ0eXVpbyJ9.u1fK05FpXctB-VkhhT3xu2WSIkEr1_VM71ald-yeKTesxhxg68TsHFEOBCgoXPuCviOP8QnUKNuVSeyMJh9z3nnrfQIjo9VZ2yicZu6ImYptSQ2DJbR80GDSPp-H7KnjaR9AAY0HZ0M-KUTaHdLABZFr307nkOeaJn_5jMpav7pqa7nrU3sI1CLX5pYVTggG6t7Zoqj2ebzzqdRxQEtdmZkD_NfH-3w3t-H0ylVdeBnPh-RvlspxC_mJzyUIJ0BwPlZpabppHm1ISySa4kwnwxEYnux0oZcb3PSoOZZZA467JySZ69PRlenNPdfGPL6E3uL1nqPHcxhte7ikSG4Q6Q"

func TestWebSocketJWTRejectsMissingToken(t *testing.T) {
	gin.SetMode(gin.TestMode)

	jwkSrv := httptest.NewServer(jwkTestEndpoint())
	defer jwkSrv.Close()

	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := websocket.Accept(w, r, nil)
		if err != nil {
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
	endpoint := jwtWebSocketEndpoint("/secure-echo", jwkSrv.URL, wsBackend)

	engine := gin.New()
	hf := ginjose.HandlerFactory(HandlerFactory(logging.NoOp, pucoragin.EndpointHandler), logging.NoOp, nil)
	engine.GET(endpoint.Endpoint, hf(endpoint, proxy.NoopProxy))

	gw := httptest.NewServer(engine)
	defer gw.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, _, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(gw.URL, "http")+endpoint.Endpoint, nil)
	if err == nil {
		t.Fatal("expected dial to fail without JWT")
	}
}

func TestWebSocketJWTAllowsValidToken(t *testing.T) {
	gin.SetMode(gin.TestMode)

	jwkSrv := httptest.NewServer(jwkTestEndpoint())
	defer jwkSrv.Close()

	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := websocket.Accept(w, r, nil)
		if err != nil {
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
	endpoint := jwtWebSocketEndpoint("/secure-echo-ok", jwkSrv.URL, wsBackend)

	engine := gin.New()
	hf := ginjose.HandlerFactory(HandlerFactory(logging.NoOp, pucoragin.EndpointHandler), logging.NoOp, nil)
	engine.GET(endpoint.Endpoint, hf(endpoint, proxy.NoopProxy))

	gw := httptest.NewServer(engine)
	defer gw.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	opts := &websocket.DialOptions{
		HTTPHeader: http.Header{
			"Authorization": []string{"Bearer " + testJWT},
		},
	}
	client, _, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(gw.URL, "http")+endpoint.Endpoint, opts)
	if err != nil {
		t.Fatalf("dial with JWT: %v", err)
	}
	defer client.Close(websocket.StatusNormalClosure, "bye")

	if err := client.Write(ctx, websocket.MessageText, []byte("auth-ok")); err != nil {
		t.Fatalf("write: %v", err)
	}
	_, msg, err := client.Read(ctx)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if string(msg) != "auth-ok" {
		t.Fatalf("unexpected echo: %s", string(msg))
	}
}

func jwtWebSocketEndpoint(path, jwkURL, wsBackend string) *config.EndpointConfig {
	return &config.EndpointConfig{
		Endpoint: path,
		Method:   http.MethodGet,
		Backend: []*config.Backend{{
			Host:                     []string{wsBackend},
			URLPattern:               "/",
			HostSanitizationDisabled: true,
		}},
		ExtraConfig: config.ExtraConfig{
			jose.ValidatorNamespace: map[string]interface{}{
				"alg":                    "RS256",
				"jwk_url":                jwkURL,
				"audience":               []string{"http://api.example.com"},
				"issuer":                 "http://example.com",
				"roles":                  []string{"role_a"},
				"disable_jwk_security":   true,
				"cache":                  true,
			},
			"websocket": map[string]interface{}{
				"enable_direct_communication": true,
				"max_message_size":            float64(1024),
			},
		},
	}
}

func jwkTestEndpoint() http.HandlerFunc {
	data, err := os.ReadFile("../../../pucora-jose/fixtures/public.json")
	return func(rw http.ResponseWriter, _ *http.Request) {
		if err != nil {
			rw.WriteHeader(http.StatusInternalServerError)
			return
		}
		rw.Header().Set("Content-Type", "application/json")
		_, _ = rw.Write(data)
	}
}
