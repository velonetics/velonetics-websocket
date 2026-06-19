package websocket

import (
	"errors"
	"time"

	"github.com/pucora/lura/v2/config"
)

const Namespace = "websocket"

const (
	defaultBackoffStrategy   = "fallback"
	defaultMaxMessageSize    = 512
	defaultMessageBufferSize = 256
	defaultReadBufferSize    = 1024
	defaultWriteBufferSize   = 1024
	defaultPingPeriod        = 54 * time.Second
	defaultPongWait          = 60 * time.Second
	defaultWriteWait         = 10 * time.Second
	defaultBackendTimeout    = 5 * time.Minute
)

var ErrNoConfig = errors.New("websocket: no config")

type Config struct {
	BackoffStrategy           string
	ConnectEvent              bool
	DisconnectEvent           bool
	DisableOTELMetrics        bool
	EnableDirectCommunication bool
	InputHeaders              []string
	MaxMessageSize            int64
	MaxRetries                int
	MessageBufferSize         int
	PingPeriod                time.Duration
	PongWait                  time.Duration
	ReadBufferSize            int
	WriteBufferSize           int
	ReturnErrorDetails        bool
	Subprotocols              []string
	Timeout                   time.Duration
	WriteWait                 time.Duration
}

func IsConfigured(extra config.ExtraConfig) bool {
	_, ok := extra[Namespace]
	return ok
}

func Parse(extra config.ExtraConfig) (Config, error) {
	raw, ok := extra[Namespace]
	if !ok {
		return Config{}, ErrNoConfig
	}
	cfgMap, ok := raw.(map[string]interface{})
	if !ok {
		return Config{}, errors.New("websocket: invalid config type")
	}

	cfg := Config{
		BackoffStrategy:    defaultBackoffStrategy,
		MaxMessageSize:   defaultMaxMessageSize,
		MessageBufferSize: defaultMessageBufferSize,
		ReadBufferSize:   defaultReadBufferSize,
		WriteBufferSize:  defaultWriteBufferSize,
		PingPeriod:       defaultPingPeriod,
		PongWait:         defaultPongWait,
		WriteWait:        defaultWriteWait,
		Timeout:          defaultBackendTimeout,
	}

	if v, ok := cfgMap["backoff_strategy"].(string); ok && v != "" {
		cfg.BackoffStrategy = v
	}
	if v, ok := cfgMap["connect_event"].(bool); ok {
		cfg.ConnectEvent = v
	}
	if v, ok := cfgMap["disconnect_event"].(bool); ok {
		cfg.DisconnectEvent = v
	}
	if v, ok := cfgMap["disable_otel_metrics"].(bool); ok {
		cfg.DisableOTELMetrics = v
	}
	if v, ok := cfgMap["enable_direct_communication"].(bool); ok {
		cfg.EnableDirectCommunication = v
	}
	if v, ok := cfgMap["input_headers"].([]interface{}); ok {
		cfg.InputHeaders = make([]string, 0, len(v))
		for _, h := range v {
			if s, ok := h.(string); ok {
				cfg.InputHeaders = append(cfg.InputHeaders, s)
			}
		}
	}
	if v, ok := cfgMap["max_message_size"].(float64); ok {
		cfg.MaxMessageSize = int64(v)
	}
	if v, ok := cfgMap["max_retries"].(float64); ok {
		cfg.MaxRetries = int(v)
	}
	if v, ok := cfgMap["message_buffer_size"].(float64); ok {
		cfg.MessageBufferSize = int(v)
	}
	if v, ok := cfgMap["read_buffer_size"].(float64); ok {
		cfg.ReadBufferSize = int(v)
	}
	if v, ok := cfgMap["write_buffer_size"].(float64); ok {
		cfg.WriteBufferSize = int(v)
	}
	if v, ok := cfgMap["return_error_details"].(bool); ok {
		cfg.ReturnErrorDetails = v
	}
	if v, ok := cfgMap["subprotocols"].([]interface{}); ok {
		cfg.Subprotocols = make([]string, 0, len(v))
		for _, p := range v {
			if s, ok := p.(string); ok {
				cfg.Subprotocols = append(cfg.Subprotocols, s)
			}
		}
	}
	if v, ok := cfgMap["ping_period"].(string); ok {
		if d, err := time.ParseDuration(v); err == nil {
			cfg.PingPeriod = d
		}
	}
	if v, ok := cfgMap["pong_wait"].(string); ok {
		if d, err := time.ParseDuration(v); err == nil {
			cfg.PongWait = d
		}
	}
	if v, ok := cfgMap["write_wait"].(string); ok {
		if d, err := time.ParseDuration(v); err == nil {
			cfg.WriteWait = d
		}
	}
	if v, ok := cfgMap["timeout"].(string); ok {
		if d, err := time.ParseDuration(v); err == nil {
			cfg.Timeout = d
		}
	}

	return cfg, nil
}
