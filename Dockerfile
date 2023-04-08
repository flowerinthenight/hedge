FROM golang:1.17.2-alpine3.14
COPY . /go/src/github.com/flowerinthenight/hedge/
WORKDIR /go/src/github.com/flowerinthenight/hedge/cmd/demo/
RUN CGO_ENABLED=0 GOOS=linux go build -v -trimpath -installsuffix cgo -o hedge .

FROM debian:bullseye-slim
RUN set -x && apt-get update && DEBIAN_FRONTEND=noninteractive apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*
WORKDIR /hedge/
COPY --from=0 /go/src/github.com/flowerinthenight/hedge/cmd/demo/hedge .
ENTRYPOINT ["/hedge/hedge"]
CMD ["-db=projects/{project}/instances/{instance}/databases/{database}"]
