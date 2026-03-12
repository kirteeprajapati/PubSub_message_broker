package broker

import (
	"sync"

	"github.com/PubSub_message_broker/internal/models"
)

// ============================================================================
// ============================================================================
//
// =========================================
// - YouTube channel = Topic
// - Subscribers = Channel ke subscribers
// - Video upload = Publish
//
// Example topics: "orders", "notifications", "chat-room-123"
//
//         then fans out message to all subscribers of that topic.
//
// Architecture:
//   ┌─────────────────────────────────────────────────────────────────────┐
//   │                           Topic "orders"                            │
//   │                                                                     │
//   │  Publishers ──► ┌────────────────┐ ──► Subscribers (Fan-out)       │
//   │                 │  Ring Buffer   │     ┌─────────────┐             │
//   │   publish() ──► │  [msg1, msg2,  │ ──► │ Subscriber1 │             │
//   │                 │   msg3, ...]   │     └─────────────┘             │
//   │                 └────────────────┘ ──► │ Subscriber2 │             │
//   │                        ▲               └─────────────┘             │
//   │                        │                                           │
//   │                  Message History                                   │
//   │                  (for last_n replay)                               │
//   └─────────────────────────────────────────────────────────────────────┘
//
// Ring Buffer:
//   - Fixed size (default 100 messages)
//   - Oldest messages automatically overwritten
//   - Supports last_n replay for new subscribers
//

const (
	DefaultRingBufferSize = 100
)

type Topic struct {
	Name        string
	subscribers sync.Map

	//         Perfect for: "last N items" scenarios, fixed memory
	//         Used in: Logging systems, audio/video buffers, undo history
	//
	// No resizing: Array size fixed hai, no memory allocations at runtime
	history     []*models.EventPayload // The actual buffer
	historyHead int                    // Next write position (0 to 99, then wraps to 0)
	historySize int                    // Current number of messages (0 to 100)
	historyMu   sync.RWMutex           // Mutex for thread-safe access

	messageCount int64        // Lifetime messages published to this topic
	statsMu      sync.RWMutex // Mutex for counter (RWMutex for read-heavy)
}

// NewTopic creates a new topic with empty subscribers
func NewTopic(name string) *Topic {
	return &Topic{
		Name:    name,
		history: make([]*models.EventPayload, DefaultRingBufferSize),
	}
}

// ============================================================================
// SUBSCRIBER MANAGEMENT
// ============================================================================

// AddSubscriber adds a subscriber to this topic
func (t *Topic) AddSubscriber(sub *Subscriber) {
	t.subscribers.Store(sub.ID, sub)
	sub.Subscribe(t.Name)
}

// RemoveSubscriber removes a subscriber from this topic
func (t *Topic) RemoveSubscriber(clientID string) {
	if sub, ok := t.subscribers.Load(clientID); ok {
		sub.(*Subscriber).Unsubscribe(t.Name)
		t.subscribers.Delete(clientID)
	}
}

// GetSubscriber returns a subscriber by ID
func (t *Topic) GetSubscriber(clientID string) (*Subscriber, bool) {
	if sub, ok := t.subscribers.Load(clientID); ok {
		return sub.(*Subscriber), true
	}
	return nil, false
}

// HasSubscriber checks if a client is subscribed
func (t *Topic) HasSubscriber(clientID string) bool {
	_, ok := t.subscribers.Load(clientID)
	return ok
}

// SubscriberCount returns number of active subscribers
func (t *Topic) SubscriberCount() int {
	count := 0
	t.subscribers.Range(func(_, _ interface{}) bool {
		count++
		return true
	})
	return count
}

// GetAllSubscribers returns all subscribers (for cleanup)
func (t *Topic) GetAllSubscribers() []*Subscriber {
	var subs []*Subscriber
	t.subscribers.Range(func(_, value interface{}) bool {
		subs = append(subs, value.(*Subscriber))
		return true
	})
	return subs
}

// ============================================================================
// PUBLISH & FAN-OUT
// ============================================================================
//         O(n) where n = number of subscribers

func (t *Topic) Publish(payload *models.EventPayload) {
	// Agar pehle fan-out karte aur beech mein crash hua toh message lost
	// Store first = durability (in-memory hai toh restart pe lost, but still better)
	t.addToHistory(payload)

	t.statsMu.Lock()
	t.messageCount++
	t.statsMu.Unlock()

	// Message object ek baar banao, sabko same object bhejo
	eventMsg := models.NewEventMessage(t.Name, payload)

	// Har subscriber ki queue mein message daalo
	t.subscribers.Range(func(_, value interface{}) bool {
		sub := value.(*Subscriber)

		// Inactive = connection closed, cleanup pending
		if sub.IsActive() {
			sub.Send(eventMsg) // Queue mein daalo (non-blocking)
		}
		return true // Continue to next subscriber
	})
}

// ============================================================================
// RING BUFFER FOR MESSAGE HISTORY (Bahut Important Data Structure!)
// ============================================================================
//
//
//         Jab full ho jaaye, oldest element overwrite ho jaata hai.
//
// Use cases:
//   - Last N items store karna (exactly what we're doing)
//   - Audio/Video streaming buffers
//   - Logging (last N logs)
//   - Undo history (last N operations)
//
// Advantages:
//   - Fixed memory (no growth)
//   - O(1) add operation
//   - No memory allocations at runtime
//
// Ring Buffer kaise kaam karta hai (VISUALLY SAMJHO):
//
//   Buffer: [_, _, _, _, _]  (size=5)
//   Head: 0, Size: 0
//
//   Add msg1: [msg1, _, _, _, _], Head: 1, Size: 1
//   Add msg2: [msg1, msg2, _, _, _], Head: 2, Size: 2
//   Add msg3: [msg1, msg2, msg3, _, _], Head: 3, Size: 3
//   Add msg4: [msg1, msg2, msg3, msg4, _], Head: 4, Size: 4
//   Add msg5: [msg1, msg2, msg3, msg4, msg5], Head: 0, Size: 5 (FULL!)
//   Add msg6: [msg6, msg2, msg3, msg4, msg5], Head: 1, Size: 5 (msg1 overwritten!)
//
//   GetLast(3) returns: [msg4, msg5, msg6] (oldest to newest order)
//

func (t *Topic) addToHistory(payload *models.EventPayload) {
	t.historyMu.Lock()
	defer t.historyMu.Unlock()

	t.history[t.historyHead] = payload

	// Example: head=4, size=5 → (4+1) % 5 = 0 (wrap around!)
	t.historyHead = (t.historyHead + 1) % DefaultRingBufferSize

	// Full hone ke baad size constant rehta hai (always 100)
	if t.historySize < DefaultRingBufferSize {
		t.historySize++
	}
}

func (t *Topic) GetLastN(n int) []*models.EventPayload {
	t.historyMu.RLock()
	defer t.historyMu.RUnlock()

	if n > t.historySize {
		n = t.historySize
	}
	if n <= 0 {
		return nil
	}

	result := make([]*models.EventPayload, n)

	// Head points to NEXT write position
	// So last written is at head-1, last N start at head-n
	// DefaultRingBufferSize add karo to handle negative numbers
	//
	// Example: head=2, n=3, size=100
	//   start = (2 - 3 + 100) % 100 = 99
	//   Positions: 99, 0, 1 (circular!)
	start := (t.historyHead - n + DefaultRingBufferSize) % DefaultRingBufferSize

	for i := 0; i < n; i++ {
		idx := (start + i) % DefaultRingBufferSize
		result[i] = t.history[idx]
	}

	return result // Oldest to newest order
}

// ============================================================================
// STATISTICS
// ============================================================================

// GetMessageCount returns total messages published to this topic
func (t *Topic) GetMessageCount() int64 {
	t.statsMu.RLock()
	defer t.statsMu.RUnlock()
	return t.messageCount
}

// NotifyTopicDeleted sends info message to all subscribers that topic is deleted
func (t *Topic) NotifyTopicDeleted() {
	infoMsg := models.NewInfoMessage(t.Name, "topic_deleted")

	t.subscribers.Range(func(_, value interface{}) bool {
		sub := value.(*Subscriber)
		if sub.IsActive() {
			sub.Send(infoMsg)
			sub.Unsubscribe(t.Name)
		}
		return true
	})
}
