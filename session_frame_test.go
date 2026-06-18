package websocket

import (
	"testing"

	"github.com/coder/websocket"
)

func TestOutboundFrameTypePreservesBinary(t *testing.T) {
	s := newClientSession("id", "/echo", nil, nil, 2)
	s.setOutboundFrameType(websocket.MessageBinary)
	if got := s.outboundFrameType(); got != websocket.MessageBinary {
		t.Fatalf("expected binary frame type, got %v", got)
	}
}

func TestOutboundFrameTypeDefaultsToText(t *testing.T) {
	s := newClientSession("id", "/echo", nil, nil, 2)
	if got := s.outboundFrameType(); got != websocket.MessageText {
		t.Fatalf("expected text frame type by default, got %v", got)
	}
}
