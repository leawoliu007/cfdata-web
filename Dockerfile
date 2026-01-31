FROM --platform=$BUILDPLATFORM golang:1.25-alpine AS builder

WORKDIR /app

# Copy dependency files first for better caching
COPY go.mod go.sum ./
RUN go mod download

# Copy source code
COPY . .

# Arguments automatically provided by docker buildx
ARG TARGETOS
ARG TARGETARCH
ARG TARGETVARIANT

# Build with support for different architectures
# Dynamically set GOARM if building for arm (specifically v7)
RUN if [ "$TARGETARCH" = "arm" ] && [ -n "$TARGETVARIANT" ]; then export GOARM="${TARGETVARIANT#v}"; fi && \
    CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} go build -ldflags="-s -w" -o cfdata-web cfdata.go

# Runtime stage
FROM alpine:latest

WORKDIR /app

# Install certificates and timezone data for network requests and logging
RUN apk add --no-cache ca-certificates tzdata

# Copy the binary from builder
COPY --from=builder /app/cfdata-web .

# Expose the application port
EXPOSE 13335

# Use ENTRYPOINT to allow passing additional flags like -port or -url
ENTRYPOINT ["./cfdata-web"]
