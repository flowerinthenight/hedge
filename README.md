[![main](https://github.com/flowerinthenight/hedge/actions/workflows/main.yml/badge.svg)](https://github.com/flowerinthenight/hedge/actions/workflows/main.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/flowerinthenight/hedge.svg)](https://pkg.go.dev/github.com/flowerinthenight/hedge)

## hedge
A library built on top of [`spindle`](https://github.com/flowerinthenight/spindle) and [Cloud Spanner](https://cloud.google.com/spanner) that provides rudimentary distributed computing facilities to Kubernetes [deployments](https://kubernetes.io/docs/concepts/workloads/controllers/deployment/). Features include a consistent, append-only, Spanner-backed distributed key/value storage, a distributed locking/leader election mechanism through `spindle`, a simple member-to-leader communication channel, a broadcast (send-to-all) mechanism, and a distributed semaphore. It also works even on single-pod deployments.

## Why?
In a nutshell, I wanted something much simpler than using [Raft](https://raft.github.io/) (my [progress](https://github.com/flowerinthenight/testqrm) on that front is quite slow), or worse, [Paxos](https://en.wikipedia.org/wiki/Paxos_(computer_science)) (Raft maybe as the long-term goal). And I wanted an easily-accessible storage that is a bit decoupled from the code (easier to query, edit, debug, backup, etc). We are already a heavy Spanner user, and `spindle` has been in our production for quite a while now: these two should be able to do it, preferably on a k8s Deployment; StatefulSets or DaemonSets shouldn't be a requirement. Since then, additional features have been added, such as the `Send()` API.

## What does it do?
Leader election is handled by `spindle`. Two APIs are provided for storage: `Put()` and `Get()`. All pods can serve the `Get()` calls, while only the leader handles the `Put()` APIs. If a non-leader pod calls `Put()`, that call is forwarded to the leader, who will do the actual write. All `Put()`'s are append-only.

`spindle`'s `HasLock()` function is also available for distributed locking due to struct embedding, although you can use `spindle` separately for that, if you prefer.

A `Send()` API is also provided for members to be able to send simple request/reply-type messages to the current leader at any time.

A `Broadcast()` API is also available for all pods. Note that due to the nature of k8s deployments (pods come and go) and the internal heartbeat delays, some pods might not receive the broadcast message at call time, although all pods will have the complete broadcast target list eventually. `hedge` uses a combination of heartbeats and broadcasts to propagate member information to all pods; non-leaders send liveness heartbeats to the leader while the leader broadcasts active members to all pods.

Finally, a distributed semaphore is also provided through the `NewSemaphore()`, `[Try]Acquire()`, and `Release()` APIs.

## Prerequisites
* All pods within the group should be able to contact each other via TCP (address:port). You can use [downward APIs](https://kubernetes.io/docs/concepts/workloads/pods/downward-api/) for this.
* Each `hedge`'s instance id should be set using the pod's cluster IP address:port.
* For now, `spindle`'s lock table and `hedge`'s log table are within the same database.
* Tables for `spindle` and `hedge` need to be created beforehand. See [here](https://github.com/flowerinthenight/spindle#usage) for `spindle`'s DDL. For `hedge`, see below:

```sql
-- 'logtable' name is just an example
CREATE TABLE logtable (
    id STRING(MAX),
    key STRING(MAX),
    value STRING(MAX),
    leader STRING(MAX),
    timestamp TIMESTAMP OPTIONS (allow_commit_timestamp=true),
) PRIMARY KEY (key, id)
```

* This library will use the input key/value table (`logtable` in the example above) for its semaphore-related operations with the following reserved keywords:
```
column=key, value=__hedge/semaphore/{name}
column=key, value=__caller={ip:port}
column=id, value=__hedge/semaphore/{name}
column=id, value=limit={num}
```

## How to use
Something like:
```go
client, _ := spanner.NewClient(context.Background(), "your/spanner/database")
defer client.Close()

xdata := "some arbitrary data"
op := hedge.New(
    client,
    "1.2.3.4:8080", // you can use k8s downward API
    "locktable",
    "myspindlelock",
    "logtable",
    hedge.WithLeaderHandler( // if leader only, handles Send()
        xdata,
        func(data interface{}, msg []byte) ([]byte, error) {
            log.Println("[send] xdata:", data.(string))
            log.Println("[send] received:", string(msg))
            return []byte("hello " + string(msg)), nil
        },
    ),
    hedge.WithBroadcastHandler( // handles Broadcast()
        xdata,
        func(data interface{}, msg []byte) ([]byte, error) {
            log.Println("[broadcast] xdata:", data.(string))
            log.Println("[broadcast] received:", string(msg))
            return []byte("broadcast " + string(msg)), nil
        },
    ),
})

ctx, cancel := context.WithCancel(context.Background())
done := make(chan error, 1) // optional wait
go op.Run(ctx, done)

// For storage, any pod should be able to call op.Put(...) or op.Get(...) here.
//
// Any pod can call HasLock() here at any given time to know whether they are leader or not.
//   hl, _ := op.HasLock()
//   if hl {
//     log.Println("leader here!")
//   }
//
// Calling op.Send(...) will be handled by the leader through the WithLeaderHandler callback.
//
// For broadcast, any pod can call op.Broadcast(...) here which will be handled by each
//   pod's WithBroadcastHandler callback, including the caller.
//
// For distributed semaphore, any pod can call the following:
//   sem, _ := op.NewSemaphore(ctx, "semaphore-name", 2)
//   sem.Acquire(ctx)
//   ...
//   sem.Release(ctx)

cancel()
<-done
```

A sample [deployment](./deployment_template.yaml) file for GKE is provided, although it needs a fair bit of editing (for auth) to be usable. It uses [Workload Identity](https://cloud.google.com/kubernetes-engine/docs/how-to/workload-identity) for authentication although you can update it to use other authentication methods as well. The service account needs to have Spanner and PubSub permissions.

Once deployed, you can test by sending PubSub messages to the created topic while checking the logs.
```sh
# Test the Put() API, key=hello, value=world
# Try running multiple times to see leader and non-leader pods handling the messages.
$ gcloud pubsub topics publish hedge-demo-pubctrl --message='put hello world'

# Test the Get() API, key=hello
# Try running multiple times to see leader and non-leader pods handling the messages.
$ gcloud pubsub topics publish hedge-demo-pubctrl --message='get hello'

# Test the Send() API
$ gcloud pubsub topics publish hedge-demo-pubctrl --message='send world'

# Test the Broadcast() API
$ gcloud pubsub topics publish hedge-demo-pubctrl --message='broadcast hello'

# Test the semaphore APIs. If you used the sample deployment yaml, you have 3 running
# pods. This message will cause two pods to create/acquire the 'testsem' semaphore
# (with limit 2) while the remaining pod will block until one of the two will release
# the semaphore after a random timeout.
$ gcloud pubsub topics publish hedge-demo-pubctrl --message='semaphore testsem 2'
```
