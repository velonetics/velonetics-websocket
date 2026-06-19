package websocket

import (
	"context"
	"time"

	"github.com/coder/websocket"
	"github.com/pucora/lura/v2/logging"
)

func RunDirect(
	ctx context.Context,
	client *websocket.Conn,
	backendURL string,
	headers map[string][]string,
	cfg Config,
	logger logging.Logger,
	endpoint string,
	metrics *Metrics,
) {
	runDirect(ctx, client, backendURL, headers, cfg, logger, endpoint, metrics)
}

func runDirect(
	ctx context.Context,
	client *websocket.Conn,
	backendURL string,
	headers map[string][]string,
	cfg Config,
	logger logging.Logger,
	endpoint string,
	metrics *Metrics,
) {
	opts := dialOptions(cfg, headers)
	backend, _, err := websocket.Dial(ctx, backendURL, opts)
	if err != nil {
		logger.Error("[SERVICE: Websocket][Client]", "Unable to connect to backend:", err.Error())
		_ = client.Close(websocket.StatusInternalError, "backend unavailable")
		return
	}
	defer backend.Close(websocket.StatusNormalClosure, "closed")

	errCh := make(chan error, 2)

	go relay(ctx, client, backend, cfg.MaxMessageSize, cfg.Timeout, true, endpoint, metrics, cfg.WriteWait, errCh)
	go relay(ctx, backend, client, cfg.MaxMessageSize, cfg.Timeout, false, endpoint, metrics, cfg.WriteWait, errCh)

	select {
	case <-ctx.Done():
	case <-errCh:
	}
}

func relay(
	ctx context.Context,
	src, dst *websocket.Conn,
	maxSize int64,
	readTimeout time.Duration,
	inbound bool,
	endpoint string,
	metrics *Metrics,
	writeWait time.Duration,
	errCh chan<- error,
) {
	for {
		readCtx, cancel := readContext(ctx, readTimeout)
		typ, data, err := src.Read(readCtx)
		cancel()
		if err != nil {
			errCh <- err
			return
		}
		if maxSize > 0 && int64(len(data)) > maxSize {
			_ = dst.Close(websocket.StatusMessageTooBig, "message too big")
			errCh <- websocket.CloseError{Code: websocket.StatusMessageTooBig}
			return
		}
		if metrics != nil {
			if inbound {
				metrics.messageIn(ctx, endpoint)
			} else {
				metrics.messageOut(ctx, endpoint)
			}
		}
		wctx := ctx
		if writeWait > 0 {
			var cancel context.CancelFunc
			wctx, cancel = context.WithTimeout(ctx, writeWait)
			err = dst.Write(wctx, typ, data)
			cancel()
		} else {
			err = dst.Write(wctx, typ, data)
		}
		if err != nil {
			errCh <- err
			return
		}
	}
}
