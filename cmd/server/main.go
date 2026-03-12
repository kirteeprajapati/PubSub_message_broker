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
// Features:
//   - In-memory only (no external dependencies)
//   - Thread-safe (sync.Map, channels)
//   - Backpressure (bounded subscriber queues)
//   - Message replay (ring buffer with last_n)
//   - Graceful shutdown (SIGINT/SIGTERM handling)
//
// HINGLISH: Features samjho (Interview mein zaroor poochenge):
//
// 1. In-memory: Saara data RAM mein hai, koi database nahi.
//    Advantage: Super fast! Disadvantage: Server restart pe data gayab.
//
// 2. Thread-safe: Multiple goroutines ek saath kaam kar sakti hain bina
//    data corrupt kiye. sync.Map aur channels use karte hain.
//    Interview: "How do you handle concurrency?" - Yahi batana hai!
//
// 3. Backpressure: Agar subscriber slow hai (messages process nahi kar pa raha),
//    toh system crash nahi karega. Bounded queue hai - purane messages drop ho jayenge.
//    Interview: "What if consumer is slow?" - Backpressure explain karo!
//
// 4. Message Replay: Naya subscriber join kare toh usse last N messages mil sakte hain.
//    Ring buffer use karte hain (circular array).
//
// 5. Graceful Shutdown: Ctrl+C karne pe server seedha band nahi hota,
//    pehle existing connections properly close karta hai.
//

func main() {
	// HINGLISH: main() function - yahin se program shuru hota hai
	// Go mein entry point hamesha main package ka main() function hota hai
	// HINGLISH: Command line flags - terminal se arguments lena
	// Example: go run main.go -port=3000
	// flag.String returns pointer (*string), isliye *port use karte hain
	port := flag.String("port", "8080", "Server port")
	flag.Parse()

	// HINGLISH: Environment variable check (Docker ke liye important!)
	// Docker mein usually PORT env variable set hota hai
	// os.Getenv("PORT") se uski value milti hai
	// Interview: "How do you configure your app for different environments?"
	//            Answer: Env variables use karte hain - 12-factor app principle!
	if envPort := os.Getenv("PORT"); envPort != "" {
		*port = envPort
	}

	log.Printf("Starting PubSub Message Broker on port %s...", *port)

	// HINGLISH: Broker instance create karo - yeh hai hamara "brain"
	// Broker saari topics aur subscribers ko manage karta hai
	// Ek hi broker instance hai poore application mein (Singleton pattern jaisa)
	b := broker.NewBroker()

	// HINGLISH: Handlers create karo - yeh HTTP requests handle karenge
	// WebSocket handler: Real-time connection ke liye (/ws endpoint)
	// REST handler: Normal HTTP API calls ke liye (/topics, /health, etc.)
	// Dono handlers ko same broker instance diya - shared state!
	wsHandler := handlers.NewWebSocketHandler(b)
	restHandler := handlers.NewRESTHandler(b)

	// HINGLISH: HTTP Routes setup karo - URL aur handler mapping
	// ServeMux = Server Multiplexer = URL router
	// Jab request aaye toh kaunsa function handle kare, yeh decide karta hai
	mux := http.NewServeMux()

	// HINGLISH: WebSocket endpoint - real-time bidirectional communication
	// Handle() use karte hain kyunki wsHandler ServeHTTP method implement karta hai
	// Interview: "Why WebSocket instead of HTTP polling?"
	//            Answer: WebSocket persistent connection hai, kam latency, kam overhead
	mux.Handle("/ws", wsHandler)

	// HINGLISH: REST API endpoints - normal HTTP request-response
	// HandleFunc() use karte hain kyunki yeh functions directly pass kar rahe hain
	//
	// /topics      - POST: topic create, GET: list all topics
	// /topics/{name} - DELETE: topic delete karo
	// /health      - Server healthy hai ya nahi (monitoring ke liye)
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

	// HINGLISH: HTTP Server configuration
	// Interview mein zaroor poochenge: "How do you configure timeouts?"
	//
	// ReadTimeout: Client se data read karne ka max time (15 sec)
	//              Slow network pe bhi 15 sec mein data aa jana chahiye
	//
	// WriteTimeout: Response bhejne ka max time (15 sec)
	//               Agar slow client hai toh bhi 15 sec mein response bhej do
	//
	// IdleTimeout: Connection idle rehne ka max time (60 sec)
	//              Keep-alive connections ke liye - 60 sec tak koi request nahi aayi toh close
	//
	// loggingMiddleware: Har request ko log karta hai (debugging ke liye)
	//                    Middleware = request aane aur handler tak pahunchne ke beech mein processing
	server := &http.Server{
		Addr:         ":" + *port,
		Handler:      loggingMiddleware(mux),
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 15 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	// HINGLISH: Server start karo ek GOROUTINE mein
	// Goroutine = lightweight thread (Go ka superpower!)
	// Interview: "Why start server in goroutine?"
	//            Answer: Main goroutine ko free rakhna hai signal handling ke liye
	//                    Agar main thread mein server start kare toh graceful shutdown nahi kar paayenge
	//
	// go func() {} - Anonymous function ko goroutine mein run karo
	// Yeh non-blocking hai - turant next line pe chala jaayega
	go func() {
		log.Printf("Server listening on http://localhost:%s", *port)
		log.Printf("WebSocket endpoint: ws://localhost:%s/ws", *port)
		log.Printf("REST API: http://localhost:%s/topics, /health, /stats", *port)

		// ListenAndServe blocks until server stops
		// ErrServerClosed matlab graceful shutdown hua - yeh error nahi hai
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("Server error: %v", err)
		}
	}()

	// ========================================================================
	// GRACEFUL SHUTDOWN - Bahut Important Interview Topic!
	// ========================================================================
	//
	// HINGLISH: Graceful Shutdown kya hai? (Production mein must-have!)
	//   - Server band karte waqt existing connections properly close karo
	//   - Naye connections mat lo
	//   - Pending messages best-effort deliver karo
	//   - Sab clean up karke exit karo
	//
	// Interview: "How do you handle server shutdown?"
	//   - Abruptly band karna = data loss, broken connections, unhappy users
	//   - Graceful shutdown = sab kuch clean, no data loss, happy users!
	//
	// Signals:
	//   SIGINT  = Ctrl+C (terminal se)
	//   SIGTERM = Docker stop, Kubernetes pod delete, kill command
	//

	// HINGLISH: Channel create karo signals receive karne ke liye
	// make(chan os.Signal, 1) = buffered channel (size 1)
	// Buffered isliye ki agar signal aaye aur hum abhi sun nahi rahe, toh lose na ho
	quit := make(chan os.Signal, 1)

	// HINGLISH: OS ko bolo "yeh signals mujhe bhejo"
	// signal.Notify() registers karta hai ki kaunse signals sunne hain
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	// HINGLISH: Yahan BLOCK ho jaayega jab tak signal nahi aata
	// <- channel se read karta hai (blocking operation)
	// Jab Ctrl+C karoge, tab yeh unblock hoga
	<-quit

	log.Println("Shutting down server...")

	// HINGLISH: Pehle broker shutdown karo - subscribers ko notify karo
	// Yeh sabko "server_shutdown" message bhejega aur connections close karega
	b.Shutdown()

	// HINGLISH: Context with Timeout - bahut important pattern!
	// "10 seconds mein shutdown complete karo, warna force kill"
	// Interview: "What is context in Go?"
	//            Answer: Context carries deadlines, cancellation signals, and request-scoped values
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel() // defer = function end pe zaroor call hoga (cleanup ke liye)

	// HINGLISH: HTTP server ko gracefully shutdown karo
	// Yeh existing requests complete hone dega, naye requests reject karega
	if err := server.Shutdown(ctx); err != nil {
		log.Printf("Server forced to shutdown: %v", err)
	}

	log.Println("Server stopped gracefully")
}

// HINGLISH: Middleware kya hai? (Interview mein zaroor poochenge!)
// Middleware = request aur actual handler ke beech mein processing
// Chain jaisa sochlo: Request → Middleware1 → Middleware2 → Handler → Response
//
// Common middleware examples:
//   - Logging (har request log karo) - yahi kar rahe hain
//   - Authentication (user logged in hai?)
//   - Rate limiting (zyada requests block karo)
//   - CORS (cross-origin requests allow karo)
//
// Pattern: func(next http.Handler) http.Handler
//   - Input: next handler (jo baad mein call karna hai)
//   - Output: new handler (jo pehle apna kaam karega, phir next call karega)
//
func loggingMiddleware(next http.Handler) http.Handler {
	// HINGLISH: HandlerFunc type-cast karke Handler interface satisfy karo
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now() // Request start time note karo (latency calculate ke liye)

		// HINGLISH: WebSocket requests ke liye skip karo (woh apna logging karte hain)
		// HTTP "Upgrade: websocket" header check karo
		if strings.Contains(strings.ToLower(r.Header.Get("Upgrade")), "websocket") {
			next.ServeHTTP(w, r) // Seedha next handler ko call karo
			return
		}

		// HINGLISH: Response wrapper - status code capture karne ke liye
		// Problem: http.ResponseWriter directly status code access nahi deta
		// Solution: Apna wrapper banao jo WriteHeader override kare
		wrapped := &responseWriter{ResponseWriter: w, statusCode: http.StatusOK}

		// HINGLISH: Actual handler ko call karo (yahan request process hogi)
		next.ServeHTTP(wrapped, r)

		// HINGLISH: Request complete hone ke baad log karo
		// Format: "GET /topics 200 1.234ms"
		log.Printf("%s %s %d %s",
			r.Method,           // HTTP method (GET, POST, DELETE, etc.)
			r.URL.Path,         // URL path (/topics, /health, etc.)
			wrapped.statusCode, // Response status code (200, 404, 500, etc.)
			time.Since(start),  // Kitna time laga (latency)
		)
	})
}

// HINGLISH: Custom Response Writer - Decorator Pattern!
// Interview: "How do you capture HTTP status code in Go?"
//
// Problem: http.ResponseWriter mein status code directly access nahi hota
// Solution: Wrapper struct banao jo ResponseWriter embed kare
//           aur WriteHeader override kare
//
// http.ResponseWriter embed kiya = saare methods automatically inherit
// Sirf WriteHeader override kiya = status code capture karne ke liye
type responseWriter struct {
	http.ResponseWriter      // Embedding - inheritance jaisa but composition
	statusCode          int  // Yahan status code store karenge
}

// HINGLISH: WriteHeader override - jab bhi status code set ho, pehle note karo
// Phir original WriteHeader call karo (rw.ResponseWriter.WriteHeader)
func (rw *responseWriter) WriteHeader(code int) {
	rw.statusCode = code                    // Pehle apne paas save karo
	rw.ResponseWriter.WriteHeader(code)     // Phir actual response mein set karo
}
