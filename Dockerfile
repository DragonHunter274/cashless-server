# syntax=docker/dockerfile:1

FROM gcr.io/distroless/static-debian12:nonroot

# Set working directory
WORKDIR /app

# Copy pre-built binary from build context
COPY cashless-server /app/cashless-server

# Expose port
EXPOSE 8080

# Run the binary
CMD ["/app/cashless-server"]
