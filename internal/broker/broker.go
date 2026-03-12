package broker

import (
	"sync"
	"time"

	"github.com/PubSub_message_broker/internal/models"
	"github.com/gorilla/websocket"
)

// ============================================================================
// BROKER - Central PubSub Manager
// ============================================================================
//
// Broker is the heart of the PubSub system:
//   - Manages all topics
//   - Tracks all connected subscribers
//   - Handles publish/subscribe operations
//   - Provides stats and health info
//
// Architecture:
//   ┌─────────────────────────────────────────────────────────────────────┐
//   │                           Broker                                    │
//   │                                                                     │
//   │   ┌─────────────────────────────────────────────────────────────┐  │
//   │   │                     Topics (sync.Map)                       │  │
//   │   │   "orders" ──► Topic{subscribers, history}                  │  │
//   │   │   "events" ──► Topic{subscribers, history}                  │  │
//   │   │   "logs"   ──► Topic{subscribers, history}                  │  │
//   │   └─────────────────────────────────────────────────────────────┘  │
//   │                                                                     │
//   │   ┌─────────────────────────────────────────────────────────────┐  │
//   │   │                  Connections (sync.Map)                     │  │
//   │   │   conn_ptr ──► Subscriber{queue, topics}                    │  │
//   │   └─────────────────────────────────────────────────────────────┘  │
//   │                                                                     │
//   │   startTime ──► Server uptime tracking                             │
//   └─────────────────────────────────────────────────────────────────────┘
//

// Why sync.Map: to avoid RACE CONDITION = CRASH!  by handling internal locking
//
// Alternative: map + sync.RWMutex More control!! fast, for read-heavy workload:
// I can forget to clear the lock which can lead to DEAFLOCK!!!!

// sync.Map Cons: No length/iteration without Range, keys interface{} type

type Broker struct {
	topics      sync.Map     // {"topic_name": *topic}
	connections sync.Map     // {"topic": subscriber_connected}
	startTime   time.Time    // to track up time of the server
	shutdown    bool         // when system shutsdown do not take new sub/pub
	shutdownMu  sync.RWMutex // multiple readers OR one writer
}

func NewBroker() *Broker {
	return &Broker{
		startTime: time.Now(),
	}
}

// ============================================================================
// TOPIC MANAGEMENT (REST API operations)
// ============================================================================

func (b *Broker) CreateTopic(name string) error {
	if b.IsShutdown() { //always check if server is shutting down
		return ErrShuttingDown
	}
	//LoadOrStore if exist :return , not-exist : create

	_, loaded := b.topics.LoadOrStore(name, NewTopic(name))
	if loaded {
		return ErrTopicExists // Already exists error NOT IDEMPOTENT
	}
	return nil // created
}

func (b *Broker) DeleteTopic(name string) error { // delete and let the subscriber know
	value, ok := b.topics.LoadAndDelete(name) // in 1 step atomic
	// so that no one can access in between load-and-delete
	if !ok {
		return ErrTopicNotFound // Topic exist nahi karta
	}
	topic := value.(*Topic)
	topic.NotifyTopicDeleted()
	return nil
}

// GetTopic returns a topic by name
func (b *Broker) GetTopic(name string) (*Topic, bool) {
	if value, ok := b.topics.Load(name); ok {
		return value.(*Topic), true
	}
	return nil, false
}

// ListTopics returns all topics with subscriber counts
func (b *Broker) ListTopics() []models.TopicInfo {
	var topics []models.TopicInfo

	b.topics.Range(func(key, value interface{}) bool {
		topic := value.(*Topic)
		topics = append(topics, models.TopicInfo{
			Name:        topic.Name,
			Subscribers: topic.SubscriberCount(),
		})
		return true
	})

	return topics
}

// TopicCount returns number of topics
func (b *Broker) TopicCount() int {
	count := 0
	b.topics.Range(func(_, _ interface{}) bool {
		count++
		return true
	})
	return count
}

// ============================================================================
// WEBSOCKET CONNECTION MANAGEMENT
// ============================================================================
// Connection lifecycle management
//                    Key = connection pointer, Value = Subscriber object

// RegisterConnection - track new WebSocket connection
func (b *Broker) RegisterConnection(conn *websocket.Conn, clientID string) *Subscriber {
	sub := NewSubscriber(clientID, conn)
	// Key = WebSocket connection pointer (memory address)
	// Value = Subscriber object
	b.connections.Store(conn, sub)
	return sub
}

func (b *Broker) UnregisterConnection(conn *websocket.Conn) {
	value, ok := b.connections.LoadAndDelete(conn)
	if !ok {
		return
	}
	sub := value.(*Subscriber)
	for _, topicName := range sub.GetTopics() {
		if topic, ok := b.GetTopic(topicName); ok {
			topic.RemoveSubscriber(sub.ID)
		}
	}
	sub.Close()
}

func (b *Broker) GetSubscriber(conn *websocket.Conn) (*Subscriber, bool) {
	if value, ok := b.connections.Load(conn); ok {
		return value.(*Subscriber), true
	}
	return nil, false
}

func (b *Broker) TotalSubscribers() int {
	count := 0
	b.connections.Range(func(_, _ interface{}) bool {
		count++
		return true
	})
	return count
}

// ============================================================================
// SUBSCRIBE / UNSUBSCRIBE / PUBLISH OPERATIONS
// ============================================================================
func (b *Broker) Subscribe(conn *websocket.Conn, clientID, topicName string, lastN int) (*models.ServerMessage, []*models.EventPayload) {
	if b.IsShutdown() {
		return models.NewErrorMessage("", models.ErrInternal, "server shutting down"), nil
	}
	topic, ok := b.GetTopic(topicName)
	if !ok {
		return models.NewErrorMessage("", models.ErrTopicNotFound, "topic not found: "+topicName), nil
	}
	sub, ok := b.GetSubscriber(conn)
	if !ok {
		sub = b.RegisterConnection(conn, clientID)
	}

	topic.AddSubscriber(sub)
	var replay []*models.EventPayload
	if lastN > 0 { // get the last N message form the ringbuffer
		replay = topic.GetLastN(lastN)
	}

	return nil, replay // nil error = success
}

func (b *Broker) Unsubscribe(conn *websocket.Conn, clientID, topicName string) *models.ServerMessage {
	topic, ok := b.GetTopic(topicName)
	if !ok {
		return models.NewErrorMessage("", models.ErrTopicNotFound, "topic not found: "+topicName)
	}

	if !topic.HasSubscriber(clientID) {
		return models.NewErrorMessage("", models.ErrNotSubscribed, "not subscribed to topic: "+topicName)
	}
	topic.RemoveSubscriber(clientID)

	return nil // Success
}

func (b *Broker) Publish(topicName string, payload *models.EventPayload) *models.ServerMessage {

	if b.IsShutdown() {
		return models.NewErrorMessage("", models.ErrInternal, "server shutting down")
	}
	topic, ok := b.GetTopic(topicName)
	if !ok {
		return models.NewErrorMessage("", models.ErrTopicNotFound, "topic not found: "+topicName)
	}
	topic.Publish(payload)

	return nil // Success
}

// ============================================================================
// HEALTH & STATS
// ============================================================================

// GetHealth returns health information
func (b *Broker) GetHealth() *models.HealthResponse {
	return &models.HealthResponse{
		UptimeSec:   int64(time.Since(b.startTime).Seconds()),
		Topics:      b.TopicCount(),
		Subscribers: b.TotalSubscribers(),
	}
}

// GetStats returns detailed statistics
func (b *Broker) GetStats() *models.StatsResponse {
	stats := &models.StatsResponse{
		Topics: make(map[string]models.TopicStats),
	}

	b.topics.Range(func(key, value interface{}) bool {
		topic := value.(*Topic)
		stats.Topics[topic.Name] = models.TopicStats{
			Messages:    topic.GetMessageCount(),
			Subscribers: topic.SubscriberCount(),
		}
		return true
	})

	return stats
}

// ============================================================================
// GRACEFUL SHUTDOWN
// ============================================================================
func (b *Broker) Shutdown() {
	b.shutdownMu.Lock()
	b.shutdown = true
	b.shutdownMu.Unlock()
	b.connections.Range(func(key, value interface{}) bool {
		sub := value.(*Subscriber)
		sub.SendDirect(models.NewInfoMessage("", "server_shutdown"))
		sub.Close()

		return true
	})
}

func (b *Broker) IsShutdown() bool {
	defer b.shutdownMu.RUnlock() // Function end pe release karo (defer = guarantee)
	return b.shutdown
}

// ============================================================================
// ERROR TYPES
// ============================================================================
type BrokerError string

func (e BrokerError) Error() string { return string(e) }

const (
	ErrTopicExists   BrokerError = "topic already exists"
	ErrTopicNotFound BrokerError = "topic not found"
	ErrShuttingDown  BrokerError = "server is shutting down"
)
