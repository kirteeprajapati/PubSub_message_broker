package broker

import (
	"sync"

	"github.com/PubSub_message_broker/internal/models"
	"github.com/gorilla/websocket"
)

// ============================================================================
// ============================================================================
//
// Architecture:
//   ┌─────────────────────────────────────────────────────────────────┐
//   │                      Subscriber                                 │
//   │  ┌──────────────┐    ┌──────────────────┐    ┌──────────────┐  │
//   │  │   Conn       │    │   MessageQueue   │    │   Topics     │  │
//   │  │  (WebSocket) │◄───│   (Buffered)     │◄───│   Subscribed │  │
//   │  └──────────────┘    └──────────────────┘    └──────────────┘  │
//   └─────────────────────────────────────────────────────────────────┘
//
//   - Queue size bounded (default 100 messages)
//   - On overflow: Drop oldest message (configurable)
//   - Alternative: Disconnect slow consumer with SLOW_CONSUMER error
//

const (
	DefaultQueueSize = 100
)

type Subscriber struct {
	ID   string
	Conn *websocket.Conn

	queue chan *models.ServerMessage

	// Map for O(1) lookup - quickly check "is topic subscribed?"
	//   - Map: O(1) lookup, O(1) add/remove
	//   - Slice: O(n) lookup (linear search)
	topics map[string]bool

	mu     sync.RWMutex
	active bool
	done   chan struct{}
}

func NewSubscriber(id string, conn *websocket.Conn) *Subscriber {
	s := &Subscriber{
		ID:     id,
		Conn:   conn,
		queue:  make(chan *models.ServerMessage, DefaultQueueSize), // Buffered channel
		topics: make(map[string]bool),                              // Empty map
		done:   make(chan struct{}),                                // Signal channel
	}

	//   - Publisher (topic.Publish) block nahi hoga
	//   - Async delivery = better throughput
	//   - Each subscriber independently processes at its own speed
	go s.deliverMessages()

	return s
}

func (s *Subscriber) deliverMessages() {
	for {
		// Jis channel pe pehle data aaye, woh case execute hoga
		//   - Multiplexing over channels
		//   - Non-deterministic if multiple ready (Go randomly picks one)
		select {
		case msg := <-s.queue:
			// WriteJSON automatically JSON serialize karega
			if err := s.Conn.WriteJSON(msg); err != nil {
				s.Close()
				return
			}
		case <-s.done:
			return
		}
	}
}

// Options:
// 1. Block producer (bad - cascading slowdown)
// 2. Drop oldest messages (what we do - good for real-time)
// 3. Drop newest messages (not as good - user misses latest)
// 4. Disconnect slow consumer (aggressive but safe)
func (s *Subscriber) Send(msg *models.ServerMessage) bool {
	s.mu.RLock()
	if !s.active {
		s.mu.RUnlock()
		return false // Subscriber dead, don't send
	}
	s.mu.RUnlock()

	//   - Try the cases, if none ready, execute default
	//   - Makes channel operation non-blocking
	select {
	case s.queue <- msg:
		return true

	default:
		// Policy: Drop oldest (purana hatao, naya daalo)
		// This is good for real-time data where latest matters most

		select {
		case <-s.queue: // Remove oldest message
			s.queue <- msg // Add new message
			return true
		default:
			// Queue was full, then suddenly empty? Race condition?
			return false
		}
	}
}

// SendDirect sends a message directly (bypassing queue)
// Used for ack/error responses that need immediate delivery
func (s *Subscriber) SendDirect(msg *models.ServerMessage) error {
	s.mu.RLock()
	if !s.active {
		s.mu.RUnlock()
		return nil
	}
	s.mu.RUnlock()

	return s.Conn.WriteJSON(msg)
}

// Subscribe adds a topic to subscriber's subscription list
func (s *Subscriber) Subscribe(topic string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.topics[topic] = true
}

// Unsubscribe removes a topic from subscriber's subscription list
func (s *Subscriber) Unsubscribe(topic string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.topics, topic)
}

// IsSubscribed checks if subscriber is subscribed to a topic
func (s *Subscriber) IsSubscribed(topic string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.topics[topic]
}

// GetTopics returns list of subscribed topics
func (s *Subscriber) GetTopics() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	topics := make([]string, 0, len(s.topics))
	for t := range s.topics {
		topics = append(topics, t)
	}
	return topics
}

// 2. Signal goroutine via channel (close)
// 3. Cleanup resources (close connection)
//
// Without this check, close(done) twice = panic!
func (s *Subscriber) Close() {
	s.mu.Lock()
	if !s.active {
		s.mu.Unlock()
		return // Already closed, nothing to do
	}
	s.active = false // Mark as inactive
	s.mu.Unlock()

	//   - close() = sabko signal milta hai (broadcast)
	//   - close() is idempotent for receivers (multiple <-done work fine)
	close(s.done)

	s.Conn.Close()
}

func (s *Subscriber) IsActive() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.active
}
