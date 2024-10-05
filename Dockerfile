# Start from the official Go image
FROM golang:1.23-alpine AS builder

# Set the working directory
WORKDIR /app

# Copy go mod and sum files
COPY go.mod go.sum ./

# Download all dependencies
RUN go mod download

# Copy the source code
COPY . .

# Build the application
RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o main .

# Start a new stage from scratch
FROM alpine:latest

# Add non-root user
RUN adduser -D appuser

# Set the working directory
WORKDIR /usr/src/app

# Copy the binary from builder
COPY --from=builder /app/main /usr/src/app/

# Use the non-root user
USER appuser

# Command to run the executable
CMD ["/usr/src/app/main"]
