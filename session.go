package websocket

import (
	"context"
	"net/http"
	"sync"
	"time"

	"github.com/coder/websocket"
)

type ClientSession struct {
	id        string
	url       string
	session   map[string]interface{}
	conn      *websocket.Conn
	outbox    chan []byte
	inbox     chan []byte
	closed    chan struct{}
	closeOnce sync.Once

	frameMu      sync.Mutex
	outboundType websocket.MessageType
}

func NewClientSession(id, url string, session map[string]interface{}, conn *websocket.Conn, buffer int) *ClientSession {
	return newClientSession(id, url, session, conn, buffer)
}

func newClientSession(id, url string, session map[string]interface{}, conn *websocket.Conn, buffer int) *ClientSession {
	if buffer <= 0 {
		buffer = defaultMessageBufferSize
	}
	return &ClientSession{
		id:      id,
		url:     url,
		session: session,
		conn:    conn,
		outbox:  make(chan []byte, buffer),
		inbox:   make(chan []byte, buffer),
		closed:  make(chan struct{}),
	}
}

func (s *ClientSession) close() {
	s.closeOnce.Do(func() {
		close(s.closed)
		if s.conn != nil {
			_ = s.conn.Close(websocket.StatusNormalClosure, "closed")
		}
	})
}

func (s *ClientSession) setOutboundFrameType(typ websocket.MessageType) {
	s.frameMu.Lock()
	s.outboundType = typ
	s.frameMu.Unlock()
}

func (s *ClientSession) outboundFrameType() websocket.MessageType {
	s.frameMu.Lock()
	defer s.frameMu.Unlock()
	if s.outboundType == 0 {
		return websocket.MessageText
	}
	return s.outboundType
}

func (s *ClientSession) writePump(ctx context.Context, maxSize int64, writeWait time.Duration) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-s.closed:
			return
		case msg, ok := <-s.outbox:
			if !ok {
				return
			}
			if maxSize > 0 && int64(len(msg)) > maxSize {
				_ = s.conn.Close(websocket.StatusMessageTooBig, "message too big")
				return
			}
			wctx, cancel := readContext(ctx, writeWait)
			err := s.conn.Write(wctx, s.outboundFrameType(), msg)
			cancel()
			if err != nil {
				return
			}
		}
	}
}

func (s *ClientSession) enqueue(msg []byte) bool {
	select {
	case s.outbox <- msg:
		return true
	default:
		return false
	}
}

func (s *ClientSession) queueInbound(data []byte) bool {
	select {
	case s.inbox <- data:
		return true
	default:
		return false
	}
}

func (s *ClientSession) drainInbound() [][]byte {
	pending := make([][]byte, 0, len(s.inbox))
	for {
		select {
		case data := <-s.inbox:
			pending = append(pending, data)
		default:
			return pending
		}
	}
}

func (s *ClientSession) requeueInbound(data []byte) bool {
	select {
	case s.inbox <- data:
		return true
	default:
		return false
	}
}

func copyHeadersToDialOpts(headers http.Header) *websocket.DialOptions {
	if len(headers) == 0 {
		return &websocket.DialOptions{}
	}
	h := make(http.Header, len(headers))
	for k, vs := range headers {
		h[k] = append([]string(nil), vs...)
	}
	return &websocket.DialOptions{HTTPHeader: h}
}
