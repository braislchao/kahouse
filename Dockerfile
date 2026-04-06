# Use the official Go image for building
FROM golang:1.24-bookworm AS builder

# Install native build dependencies required by confluent-kafka-go.
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    g++ \
    git \
    librdkafka-dev \
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

# Build the application
RUN go build -o kahouse ./cmd/kahouse

# Use a slim runtime image with librdkafka available.
FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    librdkafka1 \
 && rm -rf /var/lib/apt/lists/*

# Copy the binary from the builder stage
COPY --from=builder /app/kahouse /kahouse

# Expose the metrics port
EXPOSE 9090

# Run the binary
ENTRYPOINT ["/kahouse"]
