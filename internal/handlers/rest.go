package handlers

import (
	"encoding/json"
	"log"
	"net/http"
	"strings"

	"github.com/PubSub_message_broker/internal/broker"
	"github.com/PubSub_message_broker/internal/models"
)

// ============================================================================
// REST API HANDLERS
// ============================================================================
//
// =========================================
// HTTP methods ka use karke resources pe CRUD operations
//
//         WebSocket = Stateful real-time (subscribe, publish, receive events)
//
//         2. HTTP methods properly use (GET=read, POST=create, DELETE=remove)
//         3. Resource-based URLs (/topics, /topics/{id})
//         4. JSON/XML response format
//         5. Proper HTTP status codes (200=OK, 201=Created, 404=Not Found)
//
// REST Endpoints:
//   POST   /topics       - Create a new topic
//   DELETE /topics/{name} - Delete a topic
//   GET    /topics       - List all topics
//   GET    /health       - Health check (for monitoring/load balancers)
//   GET    /stats        - Detailed statistics (debugging/monitoring)
//
// All endpoints return JSON responses.
// Error responses follow standard format: {"error": {"code": "...", "message": "..."}}
//

// RESTHandler handles REST API requests
type RESTHandler struct {
	broker *broker.Broker
}

// NewRESTHandler creates a new REST handler
func NewRESTHandler(b *broker.Broker) *RESTHandler {
	return &RESTHandler{broker: b}
}

// ============================================================================
// TOPIC MANAGEMENT
// ============================================================================

// HandleTopics handles POST /topics and GET /topics
func (h *RESTHandler) HandleTopics(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodPost:
		h.createTopic(w, r)
	case http.MethodGet:
		h.listTopics(w, r)
	default:
		h.methodNotAllowed(w)
	}
}

// HandleTopicByName handles DELETE /topics/{name}
func (h *RESTHandler) HandleTopicByName(w http.ResponseWriter, r *http.Request) {
	// Extract topic name from path: /topics/{name}
	path := strings.TrimPrefix(r.URL.Path, "/topics/")
	topicName := strings.TrimSpace(path)

	if topicName == "" {
		h.sendError(w, http.StatusBadRequest, models.ErrBadRequest, "topic name is required")
		return
	}

	switch r.Method {
	case http.MethodDelete:
		h.deleteTopic(w, topicName)
	default:
		h.methodNotAllowed(w)
	}
}

// POST /topics {"name": "orders"}
//
//	201 Created   = Successfully created
//	400 Bad Request = Invalid input (missing name, bad JSON)
//	409 Conflict  = Already exists
//	500 Internal  = Server error
func (h *RESTHandler) createTopic(w http.ResponseWriter, r *http.Request) {
	var req models.CreateTopicRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.sendError(w, http.StatusBadRequest, models.ErrBadRequest, "invalid JSON: "+err.Error())
		return
	}

	if req.Name == "" {
		h.sendError(w, http.StatusBadRequest, models.ErrBadRequest, "name is required")
		return
	}

	err := h.broker.CreateTopic(req.Name)
	if err != nil {
		if err == broker.ErrTopicExists {
			// 409 Conflict = resource already exists
			h.sendError(w, http.StatusConflict, models.ErrAlreadyExists, "topic already exists: "+req.Name)
			return
		}
		// 500 Internal Server Error = unexpected error
		h.sendError(w, http.StatusInternalServerError, models.ErrInternal, err.Error())
		return
	}

	log.Printf("Topic created: %s", req.Name)

	// 201 Created = resource successfully created
	// Content-Type header ALWAYS set karo for JSON responses
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated) // 201
	json.NewEncoder(w).Encode(models.TopicResponse{
		Status: "created",
		Topic:  req.Name,
	})
}

// deleteTopic handles DELETE /topics/{name}
// Response: 200 {"status": "deleted", "topic": "orders"}
// Error: 404 if topic not found
func (h *RESTHandler) deleteTopic(w http.ResponseWriter, topicName string) {
	err := h.broker.DeleteTopic(topicName)
	if err != nil {
		if err == broker.ErrTopicNotFound {
			h.sendError(w, http.StatusNotFound, models.ErrTopicNotFound, "topic not found: "+topicName)
			return
		}
		h.sendError(w, http.StatusInternalServerError, models.ErrInternal, err.Error())
		return
	}

	log.Printf("Topic deleted: %s", topicName)

	// Send response
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(models.TopicResponse{
		Status: "deleted",
		Topic:  topicName,
	})
}

// listTopics handles GET /topics
// Response: {"topics": [{"name": "orders", "subscribers": 3}]}
func (h *RESTHandler) listTopics(w http.ResponseWriter, r *http.Request) {
	topics := h.broker.ListTopics()

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(models.TopicsListResponse{
		Topics: topics,
	})
}

// ============================================================================
// ============================================================================
//
//
//         2. Uptime
//         3. Key metrics (connections, queue sizes)
//         4. Dependencies status (DB, cache, etc.)

// Agar 200 OK nahi mila = server unhealthy = traffic hatao
func (h *RESTHandler) HandleHealth(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		h.methodNotAllowed(w)
		return
	}

	health := h.broker.GetHealth()

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(health)
}

// Per-topic message counts, subscriber counts, etc.
func (h *RESTHandler) HandleStats(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		h.methodNotAllowed(w)
		return
	}

	stats := h.broker.GetStats()

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(stats)
}

// ============================================================================
// ERROR RESPONSES
// ============================================================================
//         2. Consistent error response format
//         3. Meaningful error messages
//         4. Error codes for programmatic handling

// {"error": {"code": "BAD_REQUEST", "message": "topic name required"}}
type ErrorResponse struct {
	Error models.ErrorPayload `json:"error"`
}

// DRY principle (Don't Repeat Yourself)
func (h *RESTHandler) sendError(w http.ResponseWriter, statusCode int, code models.ErrorCode, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode) // Status code pehle set karo!
	json.NewEncoder(w).Encode(ErrorResponse{
		Error: models.ErrorPayload{
			Code:    code,    // Machine-readable error code
			Message: message, // Human-readable message
		},
	})
}

// Jab POST expect karte ho aur GET aaya
func (h *RESTHandler) methodNotAllowed(w http.ResponseWriter) {
	h.sendError(w, http.StatusMethodNotAllowed, models.ErrBadRequest, "method not allowed")
}
