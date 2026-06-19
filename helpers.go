package websocket

import (
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"net/http"
	"net/textproto"
	"net/url"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/pucora/lura/v2/config"
)

var blockedHeaders = map[string]struct{}{
	"upgrade":                  {},
	"connection":               {},
	"sec-websocket-extensions": {},
	"sec-websocket-version":    {},
	"sec-websocket-key":        {},
}

func IsWebSocketUpgrade(r *http.Request) bool {
	return strings.EqualFold(r.Header.Get("Upgrade"), "websocket")
}

func ExtractParams(c *gin.Context) map[string]string {
	params := make(map[string]string, len(c.Params))
	for _, param := range c.Params {
		params[param.Key] = param.Value
	}
	return params
}

func ExtractHeaders(c *gin.Context, allowed []string) http.Header {
	h := make(http.Header)
	if len(allowed) == 0 {
		return h
	}
	for _, k := range allowed {
		if k == "*" {
			for key, vals := range c.Request.Header {
				if isBlockedHeader(key) {
					continue
				}
				h[key] = append([]string(nil), vals...)
			}
			return h
		}
		key := textproto.CanonicalMIMEHeaderKey(k)
		if isBlockedHeader(strings.ToLower(key)) {
			continue
		}
		if vals, ok := c.Request.Header[key]; ok {
			h[key] = append([]string(nil), vals...)
		}
	}
	return h
}

func isBlockedHeader(key string) bool {
	_, ok := blockedHeaders[strings.ToLower(key)]
	return ok
}

func BackendWSURL(endpoint *config.EndpointConfig, c *gin.Context) (string, error) {
	if len(endpoint.Backend) == 0 {
		return "", errors.New("websocket: endpoint has no backend")
	}
	backend := endpoint.Backend[0]
	if len(backend.Host) == 0 {
		return "", errors.New("websocket: backend host is required")
	}
	if !backend.HostSanitizationDisabled {
		return "", errors.New("websocket: disable_host_sanitize must be true")
	}

	host := backend.Host[rand.Intn(len(backend.Host))]
	if !strings.HasPrefix(host, "ws://") && !strings.HasPrefix(host, "wss://") {
		return "", fmt.Errorf("websocket: backend host must use ws:// or wss://, got %q", host)
	}

	params := ExtractParams(c)
	path := backend.URLPattern
	for k, v := range params {
		path = strings.ReplaceAll(path, "{"+k+"}", v)
	}

	u, err := url.Parse(host)
	if err != nil {
		return "", err
	}
	if path != "" {
		if !strings.HasPrefix(path, "/") {
			path = "/" + path
		}
		u.Path = path
	}

	if len(c.Request.URL.RawQuery) > 0 {
		u.RawQuery = c.Request.URL.RawQuery
	}

	return u.String(), nil
}

func clientErrorJSON(reason string) []byte {
	b, _ := json.Marshal(map[string]string{"error": reason})
	return b
}
