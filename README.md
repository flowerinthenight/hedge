[![main](https://github.com/flowerinthenight/dstore/actions/workflows/main.yml/badge.svg)](https://github.com/flowerinthenight/dstore/actions/workflows/main.yml)

## dstore
A slightly opinionated distributed key/value store library built on top of [`spindle`](https://github.com/flowerinthenight/spindle) for Kubernetes deployments. It provides a consistent, append-only, [Spanner](https://cloud.google.com/spanner)-backed key/value storage for all pods in a deployment group.

### Why?
In a nutshell, I wanted something much simpler than [Raft](https://raft.github.io/) (my [progress](https://github.com/flowerinthenight/testqrm) on that front is quite slow), and an easily-accessible storage that is a bit decoupled from the code.
