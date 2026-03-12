package main

import (
	"context"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/PubSub_message_broker/internal/broker"
	"github.com/PubSub_message_broker/internal/handlers"
)

// "go run main.go"
// Decoupling!

// Architecture Overview:
//
//   ┌─────────────────────────────────────────────────────────────────────┐
//   │                        PubSub Broker                                │
//   │                                                                     │
//   │   ┌───────────────┐      ┌───────────────────────────────────────┐ │
//   │   │   REST API    │      │           WebSocket /ws               │ │
//   │   │               │      │                                       │ │
//   │   │ POST /topics  │      │  subscribe ──► Broker.Subscribe()     │ │
//   │   │ DELETE /topics│      │  unsubscribe ──► Broker.Unsubscribe() │ │
//   │   │ GET /topics   │      │  publish ──► Broker.Publish()         │ │
//   │   │ GET /health   │      │  ping ──► pong                        │ │
//   │   │ GET /stats    │      │                                       │ │
//   │   └───────┬───────┘      └───────────────────┬───────────────────┘ │
//   │           │                                  │                     │
//   │           └──────────────┬───────────────────┘                     │
//   │                          ▼                                         │
//   │   ┌─────────────────────────────────────────────────────────────┐  │
//   │   │                      Broker                                 │  │
//   │   │   Topics: sync.Map  ──► Topic (subscribers, ring buffer)    │  │
//   │   │   Connections: sync.Map ──► Subscriber (queue, delivery)    │  │
//   │   └─────────────────────────────────────────────────────────────┘  │
//   └─────────────────────────────────────────────────────────────────────┘
//

func main() {
	port := flag.String("port", "8080", "Server port")
	flag.Parse()
	if envPort := os.Getenv("PORT"); envPort != "" {
		*port = envPort
	}

	log.Printf("Starting PubSub Message Broker on port %s...", *port)

	b := broker.NewBroker()
	wsHandler := handlers.NewWebSocketHandler(b)
	restHandler := handlers.NewRESTHandler(b)

	mux := http.NewServeMux()
	mux.Handle("/ws", wsHandler)

	//
	// /topics      - POST: topic create, GET: list all topics
	// /stats       - Detailed statistics (messages count, subscribers, etc.)
	mux.HandleFunc("/topics", restHandler.HandleTopics)
	mux.HandleFunc("/topics/", restHandler.HandleTopicByName)
	mux.HandleFunc("/health", restHandler.HandleHealth)
	mux.HandleFunc("/stats", restHandler.HandleStats)

	// Root endpoint - welcome message
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"service":"PubSub Message Broker","version":"1.0.0","endpoints":{"/ws":"WebSocket","/topics":"REST API","/health":"Health Check","/stats":"Statistics"}}`))
	})

	//
	// ReadTimeout: Client se data read karne ka max time (15 sec)
	//              Slow network pe bhi 15 sec mein data aa jana chahiye
	//
	// WriteTimeout: Response bhejne ka max time (15 sec)
	//               Agar slow client hai toh bhi 15 sec mein response bhej do
	//
	// IdleTimeout: Connection idle rehne ka max time (60 sec)
	//
	//                    Middleware = request aane aur handler tak pahunchne ke beech mein processing
	server := &http.Server{
		Addr:         ":" + *port,
		Handler:      loggingMiddleware(mux),
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 15 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	// Goroutine = lightweight thread (Go ka superpower!)
	//                    Agar main thread mein server start kare toh graceful shutdown nahi kar paayenge
	//
	go func() {
		log.Printf("Server listening on http://localhost:%s", *port)
		log.Printf("WebSocket endpoint: ws://localhost:%s/ws", *port)
		log.Printf("REST API: http://localhost:%s/topics, /health, /stats", *port)

		// ListenAndServe blocks until server stops
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("Server error: %v", err)
		}
	}()

	// ========================================================================
	// GRACEFUL SHUTDOWN - Bahut Important Interview Topic!
	// ========================================================================
	//
	//   - Naye connections mat lo
	//
	//   - Abruptly band karna = data loss, broken connections, unhappy users
	//   - Graceful shutdown = sab kuch clean, no data loss, happy users!
	//
	// Signals:
	//   SIGINT  = Ctrl+C (terminal se)
	//   SIGTERM = Docker stop, Kubernetes pod delete, kill command
	//

	// make(chan os.Signal, 1) = buffered channel (size 1)
	quit := make(chan os.Signal, 1)

	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	// <- channel se read karta hai (blocking operation)
	// Jab Ctrl+C karoge, tab yeh unblock hoga
	<-quit

	log.Println("Shutting down server...")

	b.Shutdown()

	// "10 seconds mein shutdown complete karo, warna force kill"
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)

	if err := server.Shutdown(ctx); err != nil {
		log.Printf("Server forced to shutdown: %v", err)
	}

	log.Println("Server stopped gracefully")
}

// Middleware = request aur actual handler ke beech mein processing
//
// Common middleware examples:
//   - Authentication (user logged in hai?)
//   - Rate limiting (zyada requests block karo)
//   - CORS (cross-origin requests allow karo)
//
// Pattern: func(next http.Handler) http.Handler
//   - Input: next handler (jo baad mein call karna hai)
//   - Output: new handler (jo pehle apna kaam karega, phir next call karega)
func loggingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {

		if strings.Contains(strings.ToLower(r.Header.Get("Upgrade")), "websocket") {
			return
		}

		// Problem: http.ResponseWriter directly status code access nahi deta
		// Solution: Apna wrapper banao jo WriteHeader override kare
		wrapped := &responseWriter{ResponseWriter: w, statusCode: http.StatusOK}

		next.ServeHTTP(wrapped, r)

		// Format: "GET /topics 200 1.234ms"
		log.Printf("%s %s %d %s",
			r.Method,           // HTTP method (GET, POST, DELETE, etc.)
			r.URL.Path,         // URL path (/topics, /health, etc.)
			wrapped.statusCode, // Response status code (200, 404, 500, etc.)
			time.Since(start),  // Kitna time laga (latency)
		)
	})
}

// Problem: http.ResponseWriter mein status code directly access nahi hota
// Solution: Wrapper struct banao jo ResponseWriter embed kare
//
//	aur WriteHeader override kare
//
// http.ResponseWriter embed kiya = saare methods automatically inherit
type responseWriter struct {
	http.ResponseWriter     // Embedding - inheritance jaisa but composition
	statusCode          int // Yahan status code store karenge
}

// Phir original WriteHeader call karo (rw.ResponseWriter.WriteHeader)
func (rw *responseWriter) WriteHeader(code int) {
}
