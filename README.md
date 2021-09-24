[![main](https://github.com/flowerinthenight/dstore/actions/workflows/main.yml/badge.svg)](https://github.com/flowerinthenight/dstore/actions/workflows/main.yml)

`dstore` is a slightly opinionated distributed key/value store library built on top of [`spindle`](https://github.com/flowerinthenight/spindle) for Kubernetes deployments. It provides a consistent, append-only, [Spanner](https://cloud.google.com/spanner)-as-backend key/value storage for all pods in a deployment group.
