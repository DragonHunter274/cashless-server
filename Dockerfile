# syntax=docker/dockerfile:1

# Stage 1: Build frontend
FROM node:22-alpine AS frontend-builder

WORKDIR /app/frontend
COPY frontend/package.json frontend/package-lock.json ./
RUN npm ci
COPY frontend/ ./
RUN npm run build

# Stage 2: Build Go binary
FROM golang:1.24-alpine AS go-builder

RUN go install github.com/swaggo/swag/cmd/swag@latest

WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY *.go ./
RUN swag init
RUN CGO_ENABLED=0 go build -o cashless-server .

# Stage 3: Final image
FROM gcr.io/distroless/static-debian12:nonroot

WORKDIR /app

COPY --from=go-builder /app/cashless-server /app/cashless-server
COPY --from=frontend-builder /app/static /app/static

EXPOSE 8080

CMD ["/app/cashless-server"]
