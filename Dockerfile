# ============================================================================
# MULTI-STAGE DOCKERFILE
# ============================================================================
#
# Stage 1: Build - Go binary compile karo
# Stage 2: Run - Minimal image mein run karo
#
# Benefits:
#   - Small final image (~15MB vs ~1GB)
#   - No build tools in production
#   - Security: minimal attack surface
#

# ============================================================================
# STAGE 1: BUILD
# ============================================================================
FROM golang:1.21-alpine AS builder

# Install git (for go mod download if needed)
RUN apk add --no-cache git

# Set working directory
WORKDIR /app

# Copy go mod files first (for layer caching)
COPY go.mod go.sum ./

# Download dependencies
RUN go mod download

# Copy source code
COPY . .

# Build binary
# CGO_ENABLED=0 = Pure Go binary (no C dependencies)
# -ldflags="-s -w" = Strip debug info (smaller binary)
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /pubsub-broker ./cmd/server

# ============================================================================
# STAGE 2: RUN
# ============================================================================
FROM alpine:3.19

# Add ca-certificates for HTTPS (if needed later)
RUN apk --no-cache add ca-certificates

# Create non-root user for security
RUN adduser -D -g '' appuser

# Set working directory
WORKDIR /app

# Copy binary from builder stage
COPY --from=builder /pubsub-broker .

# Change ownership to non-root user
RUN chown -R appuser:appuser /app

# Switch to non-root user
USER appuser

# Expose port
EXPOSE 8080

# Health check
HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
    CMD wget --no-verbose --tries=1 --spider http://localhost:8080/health || exit 1

# Run the binary
ENTRYPOINT ["./pubsub-broker"]
CMD ["-port", "8080"]
