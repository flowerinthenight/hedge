[![main](https://github.com/flowerinthenight/hedge/actions/workflows/main.yml/badge.svg)](https://github.com/flowerinthenight/hedge/actions/workflows/main.yml)

## hedge
A library built on top of [`spindle`](https://github.com/flowerinthenight/spindle) that provides rudimentary distributed computing facilities to Kubernetes deployments. Features include a consistent, append-only, [Spanner](https://cloud.google.com/spanner)-backed distributed key/value storage, a distributed locking/leader election mechanism through `spindle`, a simple member-leader communication mechanism, and a distributed semaphore (WIP).

## Why?
In a nutshell, I wanted something much simpler than using [Raft](https://raft.github.io/) (my [progress](https://github.com/flowerinthenight/testqrm) on that front is quite slow), or worse, [Paxos](https://en.wikipedia.org/wiki/Paxos_(computer_science)) (long-term goal). And I wanted an easily-accessible storage that is a bit decoupled from the code (easier to edit, debug, backup, etc). We are already a heavy Spanner user, and `spindle` has been in our production for quite a while now: these two should be able to do it, preferably on a k8s Deployment; StatefulSets or DaemonSets shouldn't be a requirement. Since them, additional features have been added, such as the `Send()` API.

## What does it do?
Leader election is handled by `spindle`. Two APIs are provided for storage: `Put()` and `Get()`. All pods can serve the `Get()` calls, while only the leader handles the `Put()` APIs. If a non-leader pod calls `Put()`, that call is forwarded to the leader, who will do the actual write. All `Put()`'s are append-only.

`spindle`'s `HasLock()` function is also available for distributed locking due to struct embedding, although you can use `spindle` separately for that, if you prefer.

A `Send()` API is also provided for members to be able to send simple request/reply-type messages to the current leader at any time.

Finally, a distributed semaphore is currently in the works and will be available shortly.

## Prerequisites
* All pods within the group should be able to contact each other via TCP (address:port).
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

## How to use
Something like:
```go
client, _ := spanner.NewClient(context.Background(), "your/spanner/database")
defer client.Close()

xdata := "some arbitrary data"
op := hedge.New(
    client,
    "1.2.3.4:8080",
    "locktable",
    "myspindlelock",
    "logtable",
    hedge.WithLeaderHandler(
        xdata,
        func(data interface{}, msg []byte) ([]byte, error) {
            log.Println("xdata:", data.(string))
            log.Println("received:", string(msg))
            return []byte("hello " + string(msg)), nil
        },
    ),
})

ctx, cancel := context.WithCancel(context.Background())
done := make(chan error, 1) // optional wait
go op.Run(ctx, done)

// For storage, any pod should be able to call op.Put(...) or op.Get(...) here.
// For distributed locking, any pod can call op.HasLock() here.
// Calling op.Send(...) will be handled by the leader through the WithLeaderHandler callback.

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
```
