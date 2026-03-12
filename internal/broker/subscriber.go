package broker

import (
	"sync"

	"github.com/PubSub_message_broker/internal/models"
	"github.com/gorilla/websocket"
)

// ============================================================================
// SUBSCRIBER - Ek WebSocket client jo topics subscribe karta hai
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
// Backpressure Policy (Very Important Interview Topic!):
//   - Queue size bounded (default 100 messages)
//   - On overflow: Drop oldest message (configurable)
//   - Alternative: Disconnect slow consumer with SLOW_CONSUMER error
//

const (
	// HINGLISH: Queue size = kitne messages buffer mein rakh sakte hain
	// 100 matlab 100 messages queue mein reh sakte hain
	// 101st message aaye toh purana drop hoga (backpressure)
	DefaultQueueSize = 100
)

// HINGLISH: Subscriber struct - ek connected client ki saari info
//
// Interview mein poochenge: "Why use channels for message queue?"
// Answer: Channels = Go ka built-in concurrent-safe queue
//         Buffered channel = producer block nahi hota jab tak buffer full nahi
//         Goroutines ke beech safe communication
//
type Subscriber struct {
	ID   string          // HINGLISH: Unique client ID (client khud bhejta hai subscribe mein)
	Conn *websocket.Conn // HINGLISH: WebSocket connection pointer

	// HINGLISH: Message Queue - BUFFERED CHANNEL!
	// Interview: "What is a buffered channel?"
	//   - make(chan Type, size) = buffered channel
	//   - Sender block nahi hota jab tak buffer full nahi
	//   - Receiver block hota hai jab channel empty hai
	//
	// Interview: "Why buffer size 100?"
	//   - Trade-off: Memory vs Latency
	//   - Small buffer = frequent blocking, low memory
	//   - Large buffer = smooth flow, more memory
	//   - 100 is reasonable for most use cases
	queue chan *models.ServerMessage

	// HINGLISH: Topics list - yeh subscriber kin topics pe hai
	// Map for O(1) lookup - quickly check "is topic subscribed?"
	// Interview: "Why map[string]bool instead of []string?"
	//   - Map: O(1) lookup, O(1) add/remove
	//   - Slice: O(n) lookup (linear search)
	topics map[string]bool

	// HINGLISH: Mutex for topics map (normal map thread-safe nahi hai)
	// RWMutex use kiya for better read performance
	mu sync.RWMutex

	// HINGLISH: Active flag - subscriber alive hai ya nahi
	active bool

	// HINGLISH: Done channel - shutdown signal ke liye
	// Interview: "How do you signal goroutine to stop?"
	//   - close(done) karoge toh sabko signal mil jaayega
	//   - select { case <-done: return } pattern use karo
	done chan struct{}
}

// HINGLISH: NewSubscriber - naya subscriber banao
// Factory pattern - struct initialize karke goroutine start karo
func NewSubscriber(id string, conn *websocket.Conn) *Subscriber {
	s := &Subscriber{
		ID:     id,
		Conn:   conn,
		queue:  make(chan *models.ServerMessage, DefaultQueueSize), // Buffered channel
		topics: make(map[string]bool),                              // Empty map
		active: true,                                               // Abhi alive hai
		done:   make(chan struct{}),                                // Signal channel
	}

	// HINGLISH: IMPORTANT - Background goroutine start karo
	// Yeh goroutine continuously queue se messages uthake WebSocket pe bhejega
	// Interview: "Why separate goroutine for delivery?"
	//   - Publisher (topic.Publish) block nahi hoga
	//   - Async delivery = better throughput
	//   - Each subscriber independently processes at its own speed
	go s.deliverMessages()

	return s
}

// HINGLISH: deliverMessages - background mein continuously messages deliver karo
// Yeh subscriber ki lifetime tak chalta hai
//
// Interview: "How does the delivery loop work?"
// Answer: Infinite loop with select statement
//         Either: message aaye queue se → send karo
//         Or: done signal aaye → exit karo
//
func (s *Subscriber) deliverMessages() {
	for {
		// HINGLISH: Select statement = multiple channels pe simultaneously wait karo
		// Jis channel pe pehle data aaye, woh case execute hoga
		// Interview: "What is select in Go?"
		//   - Multiplexing over channels
		//   - Non-deterministic if multiple ready (Go randomly picks one)
		select {
		case msg := <-s.queue:
			// HINGLISH: Queue se message aaya - WebSocket pe bhejo
			// WriteJSON automatically JSON serialize karega
			if err := s.Conn.WriteJSON(msg); err != nil {
				// HINGLISH: Connection error = client disconnect ho gaya
				// Cleanup karo aur goroutine exit karo
				s.Close()
				return
			}
		case <-s.done:
			// HINGLISH: Done signal = shutdown ho raha hai
			// Gracefully exit karo
			return
		}
	}
}

// HINGLISH: Send - message ko queue mein daalo (NON-BLOCKING!)
// Interview: "How do you implement non-blocking send?"
// Answer: select with default case!
//
// Interview: "What is backpressure? How do you handle it?"
// Answer: Jab consumer slow ho aur producer fast ho = backpressure
//         Options:
//         1. Block producer (bad - cascading slowdown)
//         2. Drop oldest messages (what we do - good for real-time)
//         3. Drop newest messages (not as good - user misses latest)
//         4. Disconnect slow consumer (aggressive but safe)
//
func (s *Subscriber) Send(msg *models.ServerMessage) bool {
	// HINGLISH: Pehle check karo subscriber active hai ya nahi
	s.mu.RLock()
	if !s.active {
		s.mu.RUnlock()
		return false // Subscriber dead, don't send
	}
	s.mu.RUnlock()

	// HINGLISH: Non-blocking send with select-default
	// Interview: "What does select-default do?"
	//   - Try the cases, if none ready, execute default
	//   - Makes channel operation non-blocking
	select {
	case s.queue <- msg:
		// HINGLISH: Queue mein jagah thi, message daal diya
		return true

	default:
		// HINGLISH: Queue FULL! Backpressure situation!
		// Policy: Drop oldest (purana hatao, naya daalo)
		// This is good for real-time data where latest matters most

		// HINGLISH: Nested select - purana message nikalo
		select {
		case <-s.queue: // Remove oldest message
			s.queue <- msg // Add new message
			return true
		default:
			// HINGLISH: Edge case - shouldn't happen normally
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

// HINGLISH: Close - subscriber ko gracefully band karo
// Interview: "How do you safely close a goroutine?"
// Answer: 1. Set flag to prevent new operations
//         2. Signal goroutine via channel (close)
//         3. Cleanup resources (close connection)
//
// Interview: "What happens if Close() is called twice?"
// Answer: Check active flag first - idempotent operation
//         Without this check, close(done) twice = panic!
//
func (s *Subscriber) Close() {
	// HINGLISH: Lock lo aur check karo already closed toh nahi
	s.mu.Lock()
	if !s.active {
		s.mu.Unlock()
		return // Already closed, nothing to do
	}
	s.active = false // Mark as inactive
	s.mu.Unlock()

	// HINGLISH: Done channel close karo = delivery goroutine ko signal
	// Interview: "Why close() instead of done <- struct{}{}?"
	//   - close() = sabko signal milta hai (broadcast)
	//   - send = sirf ek receiver ko milta hai
	//   - close() is idempotent for receivers (multiple <-done work fine)
	close(s.done)

	// HINGLISH: WebSocket connection band karo
	// Yeh client ko bhi signal bhej deta hai
	s.Conn.Close()
}

// HINGLISH: IsActive - subscriber alive hai ya nahi
func (s *Subscriber) IsActive() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.active
}
