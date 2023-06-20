# Stage 1: compile the program
FROM golang:1.20 as build-stage

ENV GOPROXY=direct

WORKDIR /app
COPY go.* .

RUN go mod download
COPY . .
RUN go build -o server server.go

# Stage 2: build the image
FROM alpine:latest

RUN apk --no-cache add ca-certificates libc6-compat gcompat

WORKDIR /app/
COPY --from=build-stage /app/server .

EXPOSE 8080
ENV GIN_MODE=release
CMD ["./server"]