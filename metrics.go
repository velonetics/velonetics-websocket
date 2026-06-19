package websocket

import (
	"context"
	"sync"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

const meterName = "github.com/pucora/velonetics-websocket/v2"

type Metrics struct {
	connections  metric.Int64UpDownCounter
	messagesIn   metric.Int64Counter
	messagesOut  metric.Int64Counter
	reconnects   metric.Int64Counter
}

var (
	metricsOnce sync.Once
	globalMetrics *Metrics
)

func GetMetrics(disabled bool) *Metrics {
	return getMetrics(disabled)
}

func (m *Metrics) ConnectionOpened(ctx context.Context, endpoint string) {
	m.connectionOpened(ctx, endpoint)
}

func (m *Metrics) ConnectionClosed(ctx context.Context, endpoint string) {
	m.connectionClosed(ctx, endpoint)
}

func getMetrics(disabled bool) *Metrics {
	if disabled {
		return nil
	}
	metricsOnce.Do(func() {
		m := otel.Meter(meterName)
		connections, _ := m.Int64UpDownCounter("pucora.websocket.connections",
			metric.WithDescription("Active websocket client connections"))
		messagesIn, _ := m.Int64Counter("pucora.websocket.messages.in",
			metric.WithDescription("Messages received from clients"))
		messagesOut, _ := m.Int64Counter("pucora.websocket.messages.out",
			metric.WithDescription("Messages sent to clients"))
		reconnects, _ := m.Int64Counter("pucora.websocket.reconnects",
			metric.WithDescription("Backend websocket reconnect attempts"))
		globalMetrics = &Metrics{
			connections: connections,
			messagesIn:  messagesIn,
			messagesOut: messagesOut,
			reconnects:  reconnects,
		}
	})
	return globalMetrics
}

func (m *Metrics) connectionOpened(ctx context.Context, endpoint string) {
	if m == nil {
		return
	}
	m.connections.Add(ctx, 1, metric.WithAttributes(attribute.String("endpoint", endpoint)))
}

func (m *Metrics) connectionClosed(ctx context.Context, endpoint string) {
	if m == nil {
		return
	}
	m.connections.Add(ctx, -1, metric.WithAttributes(attribute.String("endpoint", endpoint)))
}

func (m *Metrics) messageIn(ctx context.Context, endpoint string) {
	if m == nil {
		return
	}
	m.messagesIn.Add(ctx, 1, metric.WithAttributes(attribute.String("endpoint", endpoint)))
}

func (m *Metrics) messageOut(ctx context.Context, endpoint string) {
	if m == nil {
		return
	}
	m.messagesOut.Add(ctx, 1, metric.WithAttributes(attribute.String("endpoint", endpoint)))
}

func (m *Metrics) reconnect(ctx context.Context, endpoint string) {
	if m == nil {
		return
	}
	m.reconnects.Add(ctx, 1, metric.WithAttributes(attribute.String("endpoint", endpoint)))
}
