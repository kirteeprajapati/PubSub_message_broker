package models

import (
	"encoding/json"
	"time"
)

// ============================================================================
// MESSAGE TYPES - WebSocket Protocol ke saare message formats
// ============================================================================
//
// =========================================
// JSON serialize/deserialize hote hain yeh
//
//         2. Request ID for correlation (request-response matching)
//         3. Optional fields with omitempty
//         4. Consistent error format
//
// Client → Server: subscribe, unsubscribe, publish, ping
// Server → Client: ack, event, error, pong, info
//
// Har message mein optional request_id hota hai for correlation
//         Server echoes it in response
//         Client matches response to original request
//

// 3. Easy refactoring - ek jagah change, sab jagah effect
type MessageType string

const (
	TypePing MessageType = "ping" // Heartbeat - "main zinda hoon"

	TypeAck   MessageType = "ack"   // Acknowledgment - "haan mil gaya"
	TypeEvent MessageType = "event" // Published message delivery
	TypeError MessageType = "error" // Error response
	TypePong  MessageType = "pong"  // Heartbeat response - "haan tu zinda hai"
	TypeInfo  MessageType = "info"  // Informational (topic deleted, server shutdown)
)

// ErrorCode defines standard error codes
type ErrorCode string

const (
	ErrBadRequest    ErrorCode = "BAD_REQUEST"
	ErrTopicNotFound ErrorCode = "TOPIC_NOT_FOUND"
	ErrSlowConsumer  ErrorCode = "SLOW_CONSUMER"
	ErrUnauthorized  ErrorCode = "UNAUTHORIZED"
	ErrInternal      ErrorCode = "INTERNAL"
	ErrAlreadyExists ErrorCode = "ALREADY_EXISTS"
	ErrNotSubscribed ErrorCode = "NOT_SUBSCRIBED"
)

// ============================================================================
// CLIENT → SERVER MESSAGES
// ============================================================================

//  3. Optional fields with omitempty - jo nahi bheja woh null/0
// `json:"type"` = JSON mein "type" key se map hoga
type ClientMessage struct {
	Type      MessageType     `json:"type"`                 // Required: message type
	Topic     string          `json:"topic,omitempty"`      // Topic name (subscribe/publish)
	Message   *PublishPayload `json:"message,omitempty"`    // Message content (publish only)
	ClientID  string          `json:"client_id,omitempty"`  // Unique client identifier
	LastN     int             `json:"last_n,omitempty"`     // Replay last N messages (subscribe)
	RequestID string          `json:"request_id,omitempty"` // Correlation ID (optional but recommended!)
}

// Useful jab message content ka structure unknown ho
// Humein content ka structure pata nahi - kuch bhi ho sakta hai!
type PublishPayload struct {
	ID      string          `json:"id"`      // UUID - unique message identifier
	Payload json.RawMessage `json:"payload"` // Any valid JSON - we don't care what's inside
}

// ============================================================================
// SERVER → CLIENT MESSAGES
// ============================================================================

// Same approach - ek struct, multiple message types
type ServerMessage struct {
	Type      MessageType   `json:"type"`                 // Message type (ack/event/error/pong/info)
	RequestID string        `json:"request_id,omitempty"` // Client ka request_id echo back
	Topic     string        `json:"topic,omitempty"`      // Topic name
	Status    string        `json:"status,omitempty"`     // "ok" for acknowledgments
	Message   *EventPayload `json:"message,omitempty"`    // Actual event data (for event type)
	Error     *ErrorPayload `json:"error,omitempty"`      // Error details (for error type)
	Msg       string        `json:"msg,omitempty"`        // Info message (for info type)
	Timestamp string        `json:"ts,omitempty"`         // ISO 8601 timestamp (2024-01-15T10:30:00Z)
	//   1. Debugging - kab message generate hua
	//   2. Ordering - agar messages out of order aaye
	//   3. Audit trail - logging/compliance
}

// EventPayload is the message delivered to subscribers
type EventPayload struct {
	ID      string          `json:"id"`
	Payload json.RawMessage `json:"payload"`
}

// ErrorPayload contains error details
type ErrorPayload struct {
	Code    ErrorCode `json:"code"`
	Message string    `json:"message"`
}

// ============================================================================
// REST API MODELS
// ============================================================================

// CreateTopicRequest for POST /topics
type CreateTopicRequest struct {
	Name string `json:"name"`
}

// TopicResponse for topic operations
type TopicResponse struct {
	Status string `json:"status"`
	Topic  string `json:"topic"`
}

// TopicInfo for GET /topics list
type TopicInfo struct {
	Name        string `json:"name"`
	Subscribers int    `json:"subscribers"`
}

// TopicsListResponse for GET /topics
type TopicsListResponse struct {
	Topics []TopicInfo `json:"topics"`
}

// HealthResponse for GET /health
type HealthResponse struct {
	UptimeSec   int64 `json:"uptime_sec"`
	Topics      int   `json:"topics"`
	Subscribers int   `json:"subscribers"`
}

// TopicStats for individual topic statistics
type TopicStats struct {
	Messages    int64 `json:"messages"`
	Subscribers int   `json:"subscribers"`
}

// StatsResponse for GET /stats
type StatsResponse struct {
	Topics map[string]TopicStats `json:"topics"`
}

// ============================================================================
// HELPER FUNCTIONS - Factory functions for creating messages
// ============================================================================
//         Hide construction complexity, ensure consistency

// ISO 8601 = international standard (2024-01-15T10:30:00Z)
func NewTimestamp() string {
	return time.Now().UTC().Format(time.RFC3339)
}

// ACK = "haan bhai, tera message mila, kaam ho gaya"
func NewAckMessage(requestID, topic string) *ServerMessage {
	return &ServerMessage{
		Type:      TypeAck,
		RequestID: requestID, // Echo back for correlation
		Topic:     topic,
		Status:    "ok",
		Timestamp: NewTimestamp(),
	}
}

func NewEventMessage(topic string, payload *EventPayload) *ServerMessage {
	return &ServerMessage{
		Type:      TypeEvent,
		Topic:     topic,
		Message:   payload, // Actual message content
		Timestamp: NewTimestamp(),
	}
}

// Jab kuch galat ho - topic not found, bad request, etc.
func NewErrorMessage(requestID string, code ErrorCode, message string) *ServerMessage {
	return &ServerMessage{
		Type:      TypeError,
		RequestID: requestID,
		Error: &ErrorPayload{
			Code:    code,    // Machine-readable (BAD_REQUEST)
			Message: message, // Human-readable (descriptive error)
		},
		Timestamp: NewTimestamp(),
	}
}

// Ping ka response - heartbeat acknowledgment
func NewPongMessage(requestID string) *ServerMessage {
	return &ServerMessage{
		Type:      TypePong,
		RequestID: requestID,
		Timestamp: NewTimestamp(),
	}
}

// Server se client ko info bhejni ho (topic deleted, server shutdown)
func NewInfoMessage(topic, msg string) *ServerMessage {
	return &ServerMessage{
		Type:      TypeInfo,
		Topic:     topic,
		Msg:       msg,
		Timestamp: NewTimestamp(),
	}
}
