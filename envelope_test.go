package websocket

import "testing"

func TestToStringFloat64(t *testing.T) {
	if got := toString(float64(42)); got != "42" {
		t.Fatalf("toString(42) = %q, want 42", got)
	}
	if got := toString(float64(3.14)); got != "3.14" {
		t.Fatalf("toString(3.14) = %q, want 3.14", got)
	}
}

func TestMatchSessionNumericValues(t *testing.T) {
	filter := map[string]interface{}{"room": float64(42)}
	target := map[string]interface{}{"room": float64(42), "uuid": "u1"}
	if !matchSession(filter, target) {
		t.Fatal("expected numeric session values to match")
	}
	target["room"] = float64(99)
	if matchSession(filter, target) {
		t.Fatal("expected mismatched numeric session values to fail")
	}
}
