package broker

import (
	"sync"

	"github.com/PubSub_message_broker/internal/models"
)

// ============================================================================
// TOPIC - Ek named channel jisme publishers publish karte hain
// ============================================================================
//
// HINGLISH EXPLANATION (Interview ke liye):
// =========================================
// Topic kya hai? Ek "channel" ya "room" sochlo.
// - YouTube channel = Topic
// - Subscribers = Channel ke subscribers
// - Video upload = Publish
//
// Example topics: "orders", "notifications", "chat-room-123"
//
// Interview mein poochenge: "How does topic routing work?"
// Answer: Publisher specifies topic name, broker looks up topic,
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
	// HINGLISH: Ring buffer size = max kitne messages yaad rakhenge
	// 100 matlab last 100 messages stored rehte hain replay ke liye
	// Purane messages automatically overwrite ho jaate hain
	DefaultRingBufferSize = 100
)

// HINGLISH: Topic struct - ek topic ki saari information
//
// Interview mein poochenge: "What data structures does a topic use?"
// Answer: 1. sync.Map for subscribers (O(1) lookup, thread-safe)
//         2. Ring buffer for message history (fixed memory, O(1) add)
//         3. Counters for statistics
//
type Topic struct {
	Name string // HINGLISH: Topic ka naam (e.g., "orders", "notifications")

	// HINGLISH: Subscribers map - kaun kaun subscribe hai
	// Key = client_id (string), Value = *Subscriber
	// sync.Map isliye ki multiple goroutines access karengi
	subscribers sync.Map

	// HINGLISH: Message history = RING BUFFER (Circular Buffer)
	// Interview: "What is a ring buffer? Why use it?"
	// Answer: Fixed size array jo circularly fill hota hai
	//         Purane data automatically overwrite ho jaata hai
	//         Perfect for: "last N items" scenarios, fixed memory
	//         Used in: Logging systems, audio/video buffers, undo history
	//
	// Memory efficient: 100 messages ke liye 100 slots - more nahi chahiye
	// No resizing: Array size fixed hai, no memory allocations at runtime
	history     []*models.EventPayload // The actual buffer
	historyHead int                    // Next write position (0 to 99, then wraps to 0)
	historySize int                    // Current number of messages (0 to 100)
	historyMu   sync.RWMutex           // Mutex for thread-safe access

	// HINGLISH: Statistics - total messages count
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
// HINGLISH: FAN-OUT pattern - ek message → multiple receivers
// Interview: "What is fan-out? How do you implement it?"
// Answer: Iterate over all subscribers, send message to each
//         O(n) where n = number of subscribers

// HINGLISH: Publish - message bhejo saare subscribers ko
func (t *Topic) Publish(payload *models.EventPayload) {
	// HINGLISH: Step 1 - Pehle history mein store karo (replay ke liye)
	// Agar pehle fan-out karte aur beech mein crash hua toh message lost
	// Store first = durability (in-memory hai toh restart pe lost, but still better)
	t.addToHistory(payload)

	// HINGLISH: Step 2 - Statistics update karo
	// Mutex use kar rahe hain kyunki int64 increment atomic nahi hai Go mein
	// Interview: "Is i++ atomic in Go?" Answer: NO! Mutex/atomic package use karo
	t.statsMu.Lock()
	t.messageCount++
	t.statsMu.Unlock()

	// HINGLISH: Step 3 - FAN-OUT to all subscribers
	// Message object ek baar banao, sabko same object bhejo
	// Memory efficient - 1000 subscribers ke liye 1 object, not 1000
	eventMsg := models.NewEventMessage(t.Name, payload)

	// HINGLISH: Range over all subscribers
	// Har subscriber ki queue mein message daalo
	t.subscribers.Range(func(_, value interface{}) bool {
		sub := value.(*Subscriber)

		// HINGLISH: Sirf active subscribers ko bhejo
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
// HINGLISH: Ring Buffer (Circular Buffer) - Interview favorite!
//
// Interview mein poochenge: "What is a ring buffer? When to use it?"
// Answer: Fixed size array jo circular fashion mein fill hota hai.
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

// HINGLISH: addToHistory - ring buffer mein message add karo
func (t *Topic) addToHistory(payload *models.EventPayload) {
	// HINGLISH: Lock lo kyunki historyHead aur historySize modify kar rahe hain
	t.historyMu.Lock()
	defer t.historyMu.Unlock()

	// HINGLISH: Current head position pe store karo
	t.history[t.historyHead] = payload

	// HINGLISH: Head ko aage badhao (circular wrap with modulo)
	// Example: head=4, size=5 → (4+1) % 5 = 0 (wrap around!)
	// Modulo (%) operator circular behavior deta hai
	t.historyHead = (t.historyHead + 1) % DefaultRingBufferSize

	// HINGLISH: Size badhao jab tak buffer full nahi hua
	// Full hone ke baad size constant rehta hai (always 100)
	if t.historySize < DefaultRingBufferSize {
		t.historySize++
	}
}

// HINGLISH: GetLastN - last N messages nikalo (oldest to newest order)
// Interview: "How do you retrieve from a ring buffer?"
// Answer: Calculate start position using modulo arithmetic
func (t *Topic) GetLastN(n int) []*models.EventPayload {
	// HINGLISH: Read lock - multiple readers OK, but no writer
	t.historyMu.RLock()
	defer t.historyMu.RUnlock()

	// HINGLISH: Agar zyada maange toh jitne hain utne do
	if n > t.historySize {
		n = t.historySize
	}
	if n <= 0 {
		return nil
	}

	result := make([]*models.EventPayload, n)

	// HINGLISH: TRICKY PART - Start position calculate karo
	// Head points to NEXT write position
	// So last written is at head-1, last N start at head-n
	// DefaultRingBufferSize add karo to handle negative numbers
	//
	// Example: head=2, n=3, size=100
	//   start = (2 - 3 + 100) % 100 = 99
	//   Positions: 99, 0, 1 (circular!)
	start := (t.historyHead - n + DefaultRingBufferSize) % DefaultRingBufferSize

	// HINGLISH: Circular manner mein iterate karo
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
