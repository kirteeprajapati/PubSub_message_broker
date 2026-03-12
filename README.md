# PubSub Message Broker

A simplified in-memory Pub/Sub system built in Go with WebSocket and REST API support.

## Features

- **In-Memory Only**: No external dependencies (Redis, Kafka, etc.)
- **WebSocket Protocol**: Real-time publish/subscribe via `/ws`
- **REST API**: Topic management and observability
- **Thread-Safe**: Concurrent publishers/subscribers handled safely
- **Fan-Out**: Every subscriber receives each message once
- **Backpressure**: Bounded per-subscriber queues (drops oldest on overflow)
- **Message Replay**: Ring buffer with `last_n` support
- **Graceful Shutdown**: Clean connection termination on SIGINT/SIGTERM

## Quick Start

### Run with Docker

```bash
# Build image
docker build -t pubsub-broker .

# Run container
docker run -p 8080:8080 pubsub-broker
```

### Run Locally

```bash
# Install dependencies
go mod tidy

# Run server
go run ./cmd/server -port 8080
```

## API Reference

### REST Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | `/topics` | Create a new topic |
| DELETE | `/topics/{name}` | Delete a topic |
| GET | `/topics` | List all topics |
| GET | `/health` | Health check |
| GET | `/stats` | Detailed statistics |

#### Create Topic
```bash
curl -X POST http://localhost:8080/topics \
  -H "Content-Type: application/json" \
  -d '{"name": "orders"}'
```
Response: `201 Created`
```json
{"status": "created", "topic": "orders"}
```

#### Delete Topic
```bash
curl -X DELETE http://localhost:8080/topics/orders
```
Response: `200 OK`
```json
{"status": "deleted", "topic": "orders"}
```

#### List Topics
```bash
curl http://localhost:8080/topics
```
Response:
```json
{"topics": [{"name": "orders", "subscribers": 3}]}
```

#### Health Check
```bash
curl http://localhost:8080/health
```
Response:
```json
{"uptime_sec": 123, "topics": 2, "subscribers": 4}
```

#### Statistics
```bash
curl http://localhost:8080/stats
```
Response:
```json
{"topics": {"orders": {"messages": 42, "subscribers": 3}}}
```

### WebSocket Protocol

Connect to `ws://localhost:8080/ws`

#### Client → Server Messages

**Subscribe**
```json
{
  "type": "subscribe",
  "topic": "orders",
  "client_id": "client-1",
  "last_n": 5,
  "request_id": "req-123"
}
```

**Unsubscribe**
```json
{
  "type": "unsubscribe",
  "topic": "orders",
  "client_id": "client-1",
  "request_id": "req-456"
}
```

**Publish**
```json
{
  "type": "publish",
  "topic": "orders",
  "message": {
    "id": "msg-uuid",
    "payload": {"order_id": "ORD-123", "amount": 99.5}
  },
  "request_id": "req-789"
}
```

**Ping**
```json
{
  "type": "ping",
  "request_id": "req-ping"
}
```

#### Server → Client Messages

**Ack** (success confirmation)
```json
{
  "type": "ack",
  "request_id": "req-123",
  "topic": "orders",
  "status": "ok",
  "ts": "2025-08-25T10:00:00Z"
}
```

**Event** (published message)
```json
{
  "type": "event",
  "topic": "orders",
  "message": {
    "id": "msg-uuid",
    "payload": {"order_id": "ORD-123", "amount": 99.5}
  },
  "ts": "2025-08-25T10:01:00Z"
}
```

**Error**
```json
{
  "type": "error",
  "request_id": "req-123",
  "error": {
    "code": "TOPIC_NOT_FOUND",
    "message": "topic not found: invalid-topic"
  },
  "ts": "2025-08-25T10:02:00Z"
}
```

**Pong** (ping response)
```json
{
  "type": "pong",
  "request_id": "req-ping",
  "ts": "2025-08-25T10:03:00Z"
}
```

**Info** (server notifications)
```json
{
  "type": "info",
  "topic": "orders",
  "msg": "topic_deleted",
  "ts": "2025-08-25T10:04:00Z"
}
```

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                        PubSub Broker                                │
│                                                                     │
│   ┌───────────────┐      ┌───────────────────────────────────────┐ │
│   │   REST API    │      │           WebSocket /ws               │ │
│   │               │      │                                       │ │
│   │ POST /topics  │      │  subscribe → fan-out                  │ │
│   │ DELETE /topics│      │  publish → all subscribers            │ │
│   │ GET /health   │      │  ping/pong (heartbeat)                │ │
│   └───────┬───────┘      └───────────────────┬───────────────────┘ │
│           │                                  │                     │
│           └──────────────┬───────────────────┘                     │
│                          ▼                                         │
│   ┌─────────────────────────────────────────────────────────────┐  │
│   │                      Broker                                 │  │
│   │   - Topics (sync.Map): thread-safe topic storage            │  │
│   │   - Subscribers: per-topic fan-out                          │  │
│   │   - Ring Buffer: message history for replay                 │  │
│   │   - Backpressure: bounded queues per subscriber             │  │
│   └─────────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────────┘
```

## Design Decisions

### Concurrency
- Uses `sync.Map` for thread-safe topic and connection storage
- No global locks - scales with number of topics
- Each subscriber has its own message queue (channel)

### Backpressure Policy
- Each subscriber has a bounded message queue (100 messages default)
- On queue overflow: **drop oldest message** and add new one
- This prevents slow consumers from blocking publishers
- Alternative policy (not implemented): disconnect with `SLOW_CONSUMER` error

### Message Replay
- Each topic maintains a ring buffer of last 100 messages
- New subscribers can request `last_n` messages on subscribe
- Ring buffer automatically overwrites oldest messages

### Graceful Shutdown
- On SIGINT/SIGTERM:
  1. Stop accepting new connections
  2. Notify all clients with `info` message
  3. Close all WebSocket connections
  4. Shutdown HTTP server with 10s timeout

### No Persistence
- All state is in-memory
- Data is lost on restart (as per requirements)

## Testing

### Using websocat (CLI WebSocket client)

```bash
# Install websocat
brew install websocat  # macOS

# Connect
websocat ws://localhost:8080/ws

# Send subscribe message
{"type":"subscribe","topic":"orders","client_id":"test-1"}

# In another terminal, publish
{"type":"publish","topic":"orders","message":{"id":"msg-1","payload":{"test":"hello"}}}
```

### Using wscat

```bash
# Install wscat
npm install -g wscat

# Connect
wscat -c ws://localhost:8080/ws
```

## Configuration

| Flag/Env | Default | Description |
|----------|---------|-------------|
| `-port` / `PORT` | 8080 | Server port |

## Error Codes

| Code | Description |
|------|-------------|
| `BAD_REQUEST` | Invalid message format |
| `TOPIC_NOT_FOUND` | Topic does not exist |
| `SLOW_CONSUMER` | Subscriber queue overflow |
| `ALREADY_EXISTS` | Topic already exists |
| `NOT_SUBSCRIBED` | Client not subscribed to topic |
| `INTERNAL` | Server error |

## License

MIT
