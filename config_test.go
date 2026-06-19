package websocket

import (
	"testing"
	"time"

	"github.com/pucora/lura/v2/config"
)

func TestParseDefaults(t *testing.T) {
	cfg, err := Parse(config.ExtraConfig{
		Namespace: map[string]interface{}{},
	})
	if err != nil {
		t.Fatal(err)
	}
	if cfg.BackoffStrategy != defaultBackoffStrategy {
		t.Fatalf("unexpected backoff: %s", cfg.BackoffStrategy)
	}
	if cfg.MaxMessageSize != defaultMaxMessageSize {
		t.Fatalf("unexpected max message size: %d", cfg.MaxMessageSize)
	}
	if cfg.PingPeriod != defaultPingPeriod {
		t.Fatalf("unexpected ping period: %s", cfg.PingPeriod)
	}
}

func TestParseCustom(t *testing.T) {
	cfg, err := Parse(config.ExtraConfig{
		Namespace: map[string]interface{}{
			"enable_direct_communication": true,
			"max_message_size":            float64(1024),
			"ping_period":                   "10s",
			"input_headers":                 []interface{}{"Authorization"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !cfg.EnableDirectCommunication {
		t.Fatal("expected direct communication")
	}
	if cfg.MaxMessageSize != 1024 {
		t.Fatalf("unexpected max message size: %d", cfg.MaxMessageSize)
	}
	if cfg.PingPeriod != 10*time.Second {
		t.Fatalf("unexpected ping period: %s", cfg.PingPeriod)
	}
	if len(cfg.InputHeaders) != 1 || cfg.InputHeaders[0] != "Authorization" {
		t.Fatalf("unexpected headers: %#v", cfg.InputHeaders)
	}
}

func TestEnvelopeRoundTrip(t *testing.T) {
	env := NewOutboundEnvelope("/chat/room", SessionFromParams(map[string]string{"room": "general"}, "id-1"), []byte("hello"))
	data, err := EncodeEnvelope(env)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := DecodeEnvelope(data)
	if err != nil {
		t.Fatal(err)
	}
	payload, err := decoded.Payload()
	if err != nil {
		t.Fatal(err)
	}
	if string(payload) != "hello" {
		t.Fatalf("unexpected payload: %s", string(payload))
	}
	if decoded.Session["uuid"] != "id-1" {
		t.Fatalf("unexpected session: %#v", decoded.Session)
	}
	if decoded.Session["Room"] != "general" {
		t.Fatalf("unexpected room: %#v", decoded.Session)
	}
}

func TestMatchSession(t *testing.T) {
	target := map[string]interface{}{"uuid": "a", "Room": "x"}
	if !matchSession(map[string]interface{}{"uuid": "a"}, target) {
		t.Fatal("expected match on uuid")
	}
	if matchSession(map[string]interface{}{"uuid": "b"}, target) {
		t.Fatal("expected no match")
	}
}

func TestBackoffStrategies(t *testing.T) {
	linear := newBackoff("linear")
	if linear(2) != 2*time.Second {
		t.Fatalf("unexpected linear backoff: %s", linear(2))
	}
	fallback := newBackoff("fallback")
	if fallback(5) != time.Second {
		t.Fatalf("unexpected fallback backoff: %s", fallback(5))
	}
}
