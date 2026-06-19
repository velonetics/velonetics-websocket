package gin

import (
	"context"
	"net/http"

	"github.com/coder/websocket"
	"github.com/gin-gonic/gin"
	uuid "github.com/go-contrib/uuid"
	"github.com/pucora/lura/v2/config"
	"github.com/pucora/lura/v2/logging"
	"github.com/pucora/lura/v2/proxy"
	pucoragin "github.com/pucora/lura/v2/router/gin"
	ws "github.com/pucora/pucora-websocket/v2"
)

const logPrefix = "[SERVICE: Websocket]"

// HandlerFactory returns a gin HandlerFactory that serves websocket endpoints.
func HandlerFactory(logger logging.Logger, next pucoragin.HandlerFactory) pucoragin.HandlerFactory {
	return func(remote *config.EndpointConfig, p proxy.Proxy) gin.HandlerFunc {
		if !ws.IsConfigured(remote.ExtraConfig) {
			return next(remote, p)
		}

		wsCfg, err := ws.Parse(remote.ExtraConfig)
		if err != nil {
			logger.Error(logPrefix, "Invalid websocket config:", err.Error())
			return func(c *gin.Context) {
				c.AbortWithStatus(http.StatusInternalServerError)
			}
		}

		metrics := ws.GetMetrics(wsCfg.DisableOTELMetrics)

		return func(c *gin.Context) {
			if !ws.IsWebSocketUpgrade(c.Request) {
				c.AbortWithStatus(http.StatusBadRequest)
				return
			}

			backendURL, err := ws.BackendWSURL(remote, c)
			if err != nil {
				logger.Error(logPrefix, err.Error())
				c.AbortWithStatus(http.StatusBadRequest)
				return
			}

			headers := ws.ExtractHeaders(c, wsCfg.InputHeaders)

			clientConn, err := websocket.Accept(c.Writer, c.Request, ws.AcceptOptions(wsCfg))
			if err != nil {
				logger.Error(logPrefix, "Upgrade failed:", err.Error())
				return
			}

			ctx := c.Request.Context()
			if wsCfg.PingPeriod > 0 {
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				defer cancel()
				go ws.PingLoop(ctx, clientConn, wsCfg.PingPeriod, wsCfg.PongWait)
			}

			endpointPath := c.FullPath()
			if endpointPath == "" {
				endpointPath = remote.Endpoint
			}

			if wsCfg.EnableDirectCommunication {
				if metrics != nil {
					metrics.ConnectionOpened(ctx, endpointPath)
					defer metrics.ConnectionClosed(ctx, endpointPath)
				}
				headerMap := make(map[string][]string, len(headers))
				for k, v := range headers {
					headerMap[k] = v
				}
				ws.RunDirect(ctx, clientConn, backendURL, headerMap, wsCfg, logger, endpointPath, metrics)
				return
			}

			hub := ws.GetHub(endpointPath, wsCfg, logger)
			if err := hub.EnsureBackend(ctx, backendURL, headers); err != nil {
				logger.Critical(logPrefix, err.Error())
				_ = clientConn.Close(websocket.StatusInternalError, "backend unavailable")
				return
			}

			sessionID := uuid.NewV4().String()
			params := ws.ExtractParams(c)
			session := ws.SessionFromParams(params, sessionID)
			clientURL := c.Request.URL.Path
			if clientURL == "" {
				clientURL = remote.Endpoint
			}

			clientSession := ws.NewClientSession(sessionID, clientURL, session, clientConn, wsCfg.MessageBufferSize)
			hub.RegisterClient(clientSession)
			hub.HandleClient(ctx, clientSession, clientURL)
		}
	}
}
