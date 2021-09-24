[![main](https://github.com/flowerinthenight/dstore/actions/workflows/main.yml/badge.svg)](https://github.com/flowerinthenight/dstore/actions/workflows/main.yml)

## dstore
A slightly opinionated distributed key/value store library built on top of [`spindle`](https://github.com/flowerinthenight/spindle) for Kubernetes deployments. It provides a consistent, append-only, [Spanner](https://cloud.google.com/spanner)-backed key/value storage for all pods in a deployment group.

## Why?
In a nutshell, I wanted something much simpler than using [Raft](https://raft.github.io/) (my [progress](https://github.com/flowerinthenight/testqrm) on that front is quite slow), or worse, Paxos. And I wanted an easily-accessible storage that is a bit decoupled from the code (easier to edit, debug, backup, etc). We are already a heavy Spanner user, and `spindle` has been in our production for quite a while now: these two should be able to do it, preferably on a k8s Deployment; StatefulSets or DaemonSets shouldn't be a requirement.

## How does it work?
Leader election is handled by `spindle`. Two APIs are provided, `Put()` and `Get()`. All pods can serve the `Get()` calls, while the leader handles all the `Put()` calls. If a non-leader pod calls `Get()`, that call is forwarded to the leader, who will do the actual write. All `Put()`'s are append-only.

## Prerequisites
* All pods within the group should be able to contact each other via TCP (address:port).
* Each `spindle` instance id should be set using the pod's cluster IP address:port.
* For now, `spindle`'s lock table and `dstore`'s log table are within the same database.
