# Use the official Go image for building
FROM golang:1.24-bookworm AS builder

# Install native build dependencies required by confluent-kafka-go (static build).
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    g++ \
    git \
    pkg-config \
 && rm -rf /var/lib/apt/lists/*

# Set the working directory
WORKDIR /app

# Copy the go.mod and go.sum files
COPY go.mod go.sum ./

# Download dependencies
RUN go mod download

# Copy the source code
COPY . .

# Build the application using the librdkafka bundled with confluent-kafka-go.
RUN go build -o kahouse ./cmd/kahouse

# Use a slim runtime image.
FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    wget \
 && rm -rf /var/lib/apt/lists/*

# Copy the binary from the builder stage
COPY --from=builder /app/kahouse /kahouse

# Expose the metrics port
EXPOSE 9090

# Run the binary
ENTRYPOINT ["/kahouse"]
