FROM golang:1.22.5-bookworm
COPY . /go/src/github.com/flowerinthenight/hedge/
WORKDIR /go/src/github.com/flowerinthenight/hedge/example/demo/
RUN CGO_ENABLED=0 GOOS=linux go build -v -trimpath -installsuffix cgo -o hedge .

FROM debian:stable-slim
RUN set -x && apt-get update && DEBIAN_FRONTEND=noninteractive apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*
WORKDIR /app/
COPY --from=0 /go/src/github.com/flowerinthenight/hedge/example/demo/hedge .
ENTRYPOINT ["/app/hedge"]
CMD ["-db=projects/{project}/instances/{instance}/databases/{database}"]
