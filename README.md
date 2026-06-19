# pucora-websocket

WebSocket gateway support for [Pucora CE](https://github.com/pucora/pucora-ce).

Implements RFC-6455 WebSocket proxying with two modes:

- **Multiplexing** — many clients, one backend connection, JSON envelope protocol
- **Direct** — transparent per-client backend connection

## User documentation

See [docs/websockets.md](../../docs/websockets.md) in the main repository for configuration, envelope format, JWT, and examples.

To publish this module as a standalone repository, see [PUBLISHING.md](PUBLISHING.md).

## Package layout

```
config.go       # Parse extra_config["websocket"]
envelope.go     # Multiplex JSON envelope encode/decode
session.go      # Per-client session and write pump
hub.go          # Multiplex hub, backend handshake, routing
direct.go       # Direct mode bidirectional relay
backoff.go      # Reconnect delay strategies
ping.go         # Client ping loop
metrics.go      # OpenTelemetry counters
transport.go    # Accept/dial options and read timeouts
helpers.go      # URL building, header propagation
hub_registry.go # Test-only hub reset helper
router/gin/     # HandlerFactory integration with Gin
```

## Integration

Registered in `pucora-ce` via `handler_factory.go`:

```go
handlerFactory = wsgin.HandlerFactory(logger, handlerFactory) // before JWT
handlerFactory = ginjose.HandlerFactory(handlerFactory, logger, rejecter)
```

JWT runs on the HTTP upgrade; the WebSocket handler runs after validation.

## Dependencies

- [`github.com/coder/websocket`](https://github.com/coder/websocket) — WebSocket client/server
- [`github.com/pucora/lura/v2`](../pucora-lura) — endpoint config and Gin router types

## Tests

```bash
go test ./...
```

From the CE repository root: `make test-websocket` or `make check-fixtures`.

## License

Apache 2.0
