FROM golang:1.22 as builder

WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN go build -o main .

FROM debian:bookworm
RUN apt-get update && apt-get install -y iproute2 curl
COPY --from=builder /app/main /app/main
EXPOSE 5030
ENTRYPOINT ["/app/main"]
