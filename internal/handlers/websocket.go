package handlers

import (
	"encoding/json"
	"log"
	"net/http"
	"time"

	"github.com/PubSub_message_broker/internal/broker"
	"github.com/PubSub_message_broker/internal/models"
	"github.com/gorilla/websocket"
)

// ============================================================================
// WEBSOCKET HANDLER - /ws endpoint
// ============================================================================
//
// HINGLISH EXPLANATION (Interview ke liye):
// =========================================
// WebSocket kya hai? Full-duplex communication protocol.
// HTTP se better kyunki:
//   - Persistent connection (har request pe connect nahi karna)
//   - Bidirectional (server bhi message bhej sakta hai bina client request ke)
//   - Low latency (no HTTP overhead for each message)
//   - Less bandwidth (no HTTP headers repeated)
//
// Interview: "HTTP vs WebSocket kab use karein?"
// Answer: HTTP = Request-Response (REST APIs, forms, static content)
//         WebSocket = Real-time bidirectional (chat, live updates, games, stock prices)
//
// Interview: "How does WebSocket handshake work?"
// Answer: 1. Client sends HTTP request with "Upgrade: websocket" header
//         2. Server responds with "101 Switching Protocols"
//         3. Connection upgraded - now it's WebSocket!
//         (Gorilla websocket library yeh sab handle karti hai automatically)
//
// WebSocket Protocol:
//   Client → Server: subscribe, unsubscribe, publish, ping
//   Server → Client: ack, event, error, pong, info
//
// Connection Lifecycle:
//   1. Client connects to /ws (HTTP → WebSocket upgrade)
//   2. Client sends subscribe message with client_id
//   3. Server registers subscriber in broker
//   4. Client can publish/subscribe/unsubscribe freely
//   5. Client disconnects or server closes
//
// ┌─────────────────────────────────────────────────────────────────────┐
// │  WebSocket Connection Flow                                          │
// │                                                                     │
// │  Client                           Server                            │
// │    │                                │                               │
// │    │──── WebSocket Upgrade ────────►│                               │
// │    │◄─── HTTP 101 Switching ────────│                               │
// │    │                                │                               │
// │    │──── {type: "subscribe"} ──────►│ ─► broker.Subscribe()         │
// │    │◄─── {type: "ack"} ─────────────│                               │
// │    │◄─── {type: "event"} ◄──────────│ ◄─ (replay messages)          │
// │    │                                │                               │
// │    │──── {type: "publish"} ────────►│ ─► broker.Publish()           │
// │    │◄─── {type: "ack"} ─────────────│                               │
// │    │                                │                               │
// │    │◄─── {type: "event"} ◄──────────│ ◄─ (from other publishers)    │
// │    │                                │                               │
// │    │──── {type: "ping"} ───────────►│                               │
// │    │◄─── {type: "pong"} ────────────│                               │
// └─────────────────────────────────────────────────────────────────────┘
//

// HINGLISH: WebSocketHandler - WebSocket connections handle karta hai
type WebSocketHandler struct {
	broker   *broker.Broker     // Reference to our PubSub broker
	upgrader websocket.Upgrader // Gorilla websocket upgrader (HTTP → WS)
}

// HINGLISH: NewWebSocketHandler - handler create karo
func NewWebSocketHandler(b *broker.Broker) *WebSocketHandler {
	return &WebSocketHandler{
		broker: b,
		upgrader: websocket.Upgrader{
			// HINGLISH: CORS check - kaunsi origins se connection allow karein
			// Interview: "What is CORS?"
			//   - Cross-Origin Resource Sharing
			//   - Browser security feature
			//   - By default browser blocks cross-origin requests
			//   - Server explicitly allow karna padta hai
			//
			// Production mein: specific origins allow karo (your frontend domain)
			// Development mein: sab allow karo (return true)
			CheckOrigin: func(r *http.Request) bool {
				return true // Allow all origins (DANGER in production!)
			},
			// HINGLISH: Buffer sizes - kitna data ek baar mein read/write kar sakte hain
			// 1024 bytes = 1 KB reasonable hai for JSON messages
			ReadBufferSize:  1024,
			WriteBufferSize: 1024,
		},
	}
}

// HINGLISH: ServeHTTP - http.Handler interface implement karta hai
// Interview: "What is http.Handler interface?"
//   - Interface with ServeHTTP(w, r) method
//   - Koi bhi type jo yeh implement kare = HTTP handler ban sakta hai
//   - mux.Handle("/ws", wsHandler) isliye kaam karta hai
//
func (h *WebSocketHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// HINGLISH: HTTP connection ko WebSocket mein upgrade karo
	// Internally yeh:
	//   1. "Upgrade: websocket" header check karega
	//   2. "101 Switching Protocols" response bhejega
	//   3. *websocket.Conn return karega
	conn, err := h.upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("WebSocket upgrade failed: %v", err)
		return
	}

	log.Printf("New WebSocket connection from %s", r.RemoteAddr)

	// HINGLISH: Har connection ko separate goroutine mein handle karo
	// Interview: "Why handle each connection in a goroutine?"
	//   - Non-blocking - ek connection doosre ko block nahi karega
	//   - Scalable - thousands of concurrent connections handle kar sakte hain
	//   - Go's strength - cheap goroutines (few KB each)
	go h.handleConnection(conn)
}

// HINGLISH: handleConnection - ek WebSocket connection ka poora lifecycle handle karo
// Yeh function tab tak chalta hai jab tak connection alive hai
//
// Interview: "Explain the WebSocket message handling loop"
// Answer: Infinite loop jo:
//   1. Message read kare (blocking)
//   2. JSON parse kare
//   3. Type ke basis pe handler call kare
//   4. Repeat until error/disconnect
//
func (h *WebSocketHandler) handleConnection(conn *websocket.Conn) {
	var currentClientID string

	// HINGLISH: defer = function exit pe ZAROOR execute hoga
	// Cleanup operations yahin likho - panic bhi ho toh bhi chalega
	// Interview: "What is defer? Why use it?"
	//   - Cleanup guarantee karta hai
	//   - LIFO order mein execute hote hain (stack jaisa)
	defer func() {
		h.broker.UnregisterConnection(conn) // Broker se hatao
		conn.Close()                         // Connection band karo
		log.Printf("WebSocket connection closed: client_id=%s", currentClientID)
	}()

	// HINGLISH: Read deadline = connection timeout
	// Agar 60 seconds tak koi message nahi aaya = dead connection
	// Interview: "How do you detect dead connections?"
	//   - Read deadline + heartbeat (ping/pong)
	//   - Client periodically ping bheje, server pong reply kare
	//   - Deadline reset hota rahe = connection alive
	conn.SetReadDeadline(time.Now().Add(60 * time.Second))

	// HINGLISH: Pong handler - jab client ping ka response (pong) bheje
	// Har pong pe deadline reset karo = connection still alive
	conn.SetPongHandler(func(string) error {
		conn.SetReadDeadline(time.Now().Add(60 * time.Second))
		return nil
	})

	// HINGLISH: MESSAGE PROCESSING LOOP - yeh connection ki poori zindagi hai
	for {
		// HINGLISH: ReadMessage BLOCKS jab tak message nahi aata
		// msgBytes = raw bytes, error = nil if success
		_, msgBytes, err := conn.ReadMessage()
		if err != nil {
			// HINGLISH: Expected close errors silently ignore karo
			// CloseGoingAway = client normally disconnect hua
			// Unexpected errors log karo for debugging
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
				log.Printf("WebSocket read error: %v", err)
			}
			return // Loop se bahar = defer execute = cleanup
		}

		// HINGLISH: Message aaya = connection alive = deadline reset karo
		conn.SetReadDeadline(time.Now().Add(60 * time.Second))

		// HINGLISH: JSON parse karo
		var msg models.ClientMessage
		if err := json.Unmarshal(msgBytes, &msg); err != nil {
			// Invalid JSON - error bhejo but connection mat todo
			h.sendError(conn, "", models.ErrBadRequest, "invalid JSON: "+err.Error())
			continue // Next message ke liye wait karo
		}

		// HINGLISH: Message type ke basis pe handler call karo
		// Switch statement = cleaner than if-else chain
		switch msg.Type {
		case models.TypeSubscribe:
			currentClientID = msg.ClientID // Track kaun hai yeh client
			h.handleSubscribe(conn, &msg)

		case models.TypeUnsubscribe:
			h.handleUnsubscribe(conn, &msg)

		case models.TypePublish:
			h.handlePublish(conn, &msg)

		case models.TypePing:
			h.handlePing(conn, &msg)

		default:
			// Unknown message type - error bhejo
			h.sendError(conn, msg.RequestID, models.ErrBadRequest, "unknown message type: "+string(msg.Type))
		}
	}
}

// ============================================================================
// MESSAGE HANDLERS - Har message type ka apna handler
// ============================================================================
// HINGLISH: Clean code principle - har handler ek kaam karta hai
// Single Responsibility Principle (SRP)

// HINGLISH: handleSubscribe - client ko topic se subscribe karao
// Flow: Validate → Broker.Subscribe() → Send Ack → Send Replay messages
func (h *WebSocketHandler) handleSubscribe(conn *websocket.Conn, msg *models.ClientMessage) {
	// HINGLISH: Input validation - ALWAYS validate user input!
	// Interview: "How do you handle input validation?"
	//   - Validate early, fail fast
	//   - Clear error messages
	//   - Never trust client input
	if msg.Topic == "" {
		h.sendError(conn, msg.RequestID, models.ErrBadRequest, "topic is required")
		return
	}
	if msg.ClientID == "" {
		h.sendError(conn, msg.RequestID, models.ErrBadRequest, "client_id is required")
		return
	}

	// HINGLISH: Broker ko bolo subscribe karao
	// Returns: error message (if any) + replay messages
	errMsg, replay := h.broker.Subscribe(conn, msg.ClientID, msg.Topic, msg.LastN)
	if errMsg != nil {
		errMsg.RequestID = msg.RequestID // Correlation ID set karo
		conn.WriteJSON(errMsg)
		return
	}

	// HINGLISH: Success - acknowledgment bhejo
	// ACK pattern - client ko confirm karo "haan ho gaya"
	ack := models.NewAckMessage(msg.RequestID, msg.Topic)
	conn.WriteJSON(ack)

	// HINGLISH: Replay messages bhejo (agar requested)
	// last_n=10 → last 10 messages bhejo immediately
	for _, payload := range replay {
		event := models.NewEventMessage(msg.Topic, payload)
		conn.WriteJSON(event)
	}

	log.Printf("Client %s subscribed to topic %s (replay=%d)", msg.ClientID, msg.Topic, len(replay))
}

// handleUnsubscribe processes unsubscribe messages
func (h *WebSocketHandler) handleUnsubscribe(conn *websocket.Conn, msg *models.ClientMessage) {
	// Validate required fields
	if msg.Topic == "" {
		h.sendError(conn, msg.RequestID, models.ErrBadRequest, "topic is required")
		return
	}
	if msg.ClientID == "" {
		h.sendError(conn, msg.RequestID, models.ErrBadRequest, "client_id is required")
		return
	}

	// Unsubscribe
	errMsg := h.broker.Unsubscribe(conn, msg.ClientID, msg.Topic)
	if errMsg != nil {
		errMsg.RequestID = msg.RequestID
		conn.WriteJSON(errMsg)
		return
	}

	// Send ack
	ack := models.NewAckMessage(msg.RequestID, msg.Topic)
	conn.WriteJSON(ack)

	log.Printf("Client %s unsubscribed from topic %s", msg.ClientID, msg.Topic)
}

// HINGLISH: handlePublish - message publish karo topic pe
// Interview: "Explain the publish flow"
// Answer: 1. Validate input
//         2. Create event payload
//         3. Broker.Publish() calls Topic.Publish()
//         4. Topic fans out to all subscribers
//         5. Send ACK to publisher
//
func (h *WebSocketHandler) handlePublish(conn *websocket.Conn, msg *models.ClientMessage) {
	// HINGLISH: Strict validation - saari fields check karo
	if msg.Topic == "" {
		h.sendError(conn, msg.RequestID, models.ErrBadRequest, "topic is required")
		return
	}
	if msg.Message == nil {
		h.sendError(conn, msg.RequestID, models.ErrBadRequest, "message is required")
		return
	}
	// HINGLISH: Message ID required - deduplication aur tracking ke liye
	// Client generate karta hai (usually UUID)
	if msg.Message.ID == "" {
		h.sendError(conn, msg.RequestID, models.ErrBadRequest, "message.id is required")
		return
	}

	// HINGLISH: Event payload banao jo subscribers ko jayega
	// ClientMessage → EventPayload transform
	payload := &models.EventPayload{
		ID:      msg.Message.ID,
		Payload: msg.Message.Payload, // Raw JSON - kuch bhi ho sakta hai
	}

	// HINGLISH: Broker ko bolo publish karo
	// Internally yeh fan-out karega sabhi subscribers ko
	errMsg := h.broker.Publish(msg.Topic, payload)
	if errMsg != nil {
		errMsg.RequestID = msg.RequestID
		conn.WriteJSON(errMsg)
		return
	}

	// HINGLISH: ACK bhejo - "message accepted for delivery"
	// Note: ACK matlab "accepted", not "delivered to all"
	// At-most-once delivery guarantee
	ack := models.NewAckMessage(msg.RequestID, msg.Topic)
	conn.WriteJSON(ack)

	log.Printf("Message published to topic %s: id=%s", msg.Topic, msg.Message.ID)
}

// handlePing processes ping messages
func (h *WebSocketHandler) handlePing(conn *websocket.Conn, msg *models.ClientMessage) {
	pong := models.NewPongMessage(msg.RequestID)
	conn.WriteJSON(pong)
}

// sendError sends an error message to the client
func (h *WebSocketHandler) sendError(conn *websocket.Conn, requestID string, code models.ErrorCode, message string) {
	errMsg := models.NewErrorMessage(requestID, code, message)
	conn.WriteJSON(errMsg)
}
