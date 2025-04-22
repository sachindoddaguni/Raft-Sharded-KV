# Stage 1: Build the logserver binary.
FROM golang:1.20 AS builder
WORKDIR /app

# Copy the source code.
COPY kvserver.go .

ENV CGO_ENABLED=0

# Build the binary.
RUN go build -a -installsuffix cgo -o kvserver kvserver.go

# Stage 2: Create a small final image.
FROM debian:bullseye-slim
WORKDIR /app

# Copy the binary from the builder stage.
COPY --from=builder /app/kvserver .

# Expose port 8080 (the log server's port).
EXPOSE 8080

# Command to run when the container starts.
CMD ["./kvserver"]