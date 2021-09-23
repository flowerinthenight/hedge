FROM golang:1.17.1-alpine3.14
COPY . /go/src/github.com/flowerinthenight/dstore/
WORKDIR /go/src/github.com/flowerinthenight/dstore/cmd/demo/
RUN CGO_ENABLED=0 GOOS=linux go build -v -trimpath -installsuffix cgo -o dstore .

FROM debian:bullseye-slim
RUN set -x && apt-get update && DEBIAN_FRONTEND=noninteractive apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*
WORKDIR /dstore/
COPY --from=0 /go/src/github.com/flowerinthenight/dstore/cmd/demo/dstore .
ENTRYPOINT ["/dstore/dstore"]
CMD ["-db=projects/{project}/instances/{instance}/databases/{database}"]
