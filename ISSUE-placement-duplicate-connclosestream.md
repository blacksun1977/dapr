# Placement: duplicate ConnCloseStream underflows the namespace connection count, tearing down a live disseminator and forcing every sidecar in the namespace to reconnect

## In what area(s)?

/area placement

## What version of Dapr?

Reproduced on a build from `master` (includes #9689 and #9837). The affected
code is unchanged on `master` as of 2026-08-04, and dates back to the placement
server refactor in #9405.

This is **not** the dissemination-timeout cascade fixed by #9689 / #9639 / #9770.
Those fixes are present in the build where this reproduces. There is no
`Dissemination timeout` or `Closing non-responding stream` log line anywhere near
the incidents below.

## Expected Behavior

When one sidecar's stream fails, only that stream is closed. Every other sidecar
in the namespace keeps its placement stream and its disseminator.

## Actual Behavior

Periodically, the placement server closes **every** stream in the namespace
within the same millisecond, and all sidecars reconnect. In our 30-sidecar
cluster this happened roughly every few minutes during rolling updates:
17:49:43, 17:58:24, 18:02:35, 18:05:12, 18:13:31.

Server side, the cascade always starts with one stream failing while handling a
`DisseminateLock`, immediately followed by all other connections closing:

```
18:02:35.774185 warning Error receiving from stream 10.103.246.186:11971: rpc error: code = Canceled desc = context canceled
18:02:35.774304 error   Error handling stream event *loops.DisseminateLock on 10.103.246.186:11971: rpc error: code = Canceled desc = context canceled
18:02:35.774326 info    Closing connection to 10.103.246.186:11971: rpc error: code = Canceled desc = context canceled
18:02:35.774368 info    Closing connection to 10.103.250.151:20038: ...
18:02:35.774370 info    Closing connection to 10.103.246.137:50351: ...
18:02:35.774374 info    Closing connection to 10.103.250.192:39974: ...
   ... ~30 more, all within 4ms, covering every sidecar in the namespace ...
18:02:35.777257 info    Received status report connection from new namespace=default id=xt-battle-py host=10.103.250.192:50002
18:02:35.777284 info    Received status report connection from new namespace=default id=xt-match-py  host=10.103.250.151:50002
   ... every sidecar reconnecting ...
```

Client side, an uninvolved sidecar that was merely draining sees its stream
dropped, and — this is the key part — the dissemination version **restarts from
scratch**, which only happens if the disseminator object was destroyed and
rebuilt:

```
18:02:34.963 info Dissemination complete for version 107 (changed types [entry user])
18:02:35.774 warning Error receiving from stream: rpc error: code = Canceled desc = context canceled
18:02:35.775 info Placement stream closed. Reconnecting...
18:02:35.778 info Connected to placement service
18:02:35.981 info Dissemination complete for version 2 (changed types [match_py])
```

`107 -> 2` is `disseminator.currentVersion` being reallocated from zero.

## Root Cause

A single stream can emit **two** `ConnCloseStream` events, while
`namespaces.handleCloseStream` decrements its per-namespace connection counter
unconditionally.

Emission point 1 — `recvLoop` unwinding
([stream.go#L86-L93](https://github.com/dapr/dapr/blob/master/pkg/placement/internal/loops/stream/stream.go#L86-L93)):

```go
stream.wg.Go(func() {
	err := stream.recvLoop()
	stream.nsLoop.Enqueue(&loops.ConnCloseStream{
		StreamIDx: stream.idx,
		Namespace: stream.ns,
		Error:     err,
	})
})
```

Emission point 2 — `Handle` failing to send
([stream.go#L115-L121](https://github.com/dapr/dapr/blob/master/pkg/placement/internal/loops/stream/stream.go#L115-L121)):

```go
if err != nil {
	log.Errorf("Error handling stream event %T on %s: %v", event, s.addr, err)
	s.nsLoop.Enqueue(&loops.ConnCloseStream{
		StreamIDx: s.idx,
		Namespace: s.ns,
	})
}
```

When a sidecar disconnects mid-round, both fire for the same connection: the
`Send` inside `handleLock` fails **and** `recvLoop` returns. The consumer has no
idempotence guard
([namespaces.go#L131-L146](https://github.com/dapr/dapr/blob/master/pkg/placement/internal/loops/namespaces/namespaces.go#L131-L146)):

```go
dissLoop.connections--
dissLoop.loop.Enqueue(closeStream)

if dissLoop.connections == 0 {
	delete(n.disseminators, closeStream.Namespace)
	dissLoop.loop.Close(&loops.Shutdown{Error: closeStream.Error})
	disseminator.LoopFactory.CacheLoop(dissLoop.loop)
}
```

Note `disseminator.handleCloseStream` *is* idempotent (it returns early for an
unknown `StreamIDx`), which is why the double delivery is invisible there and
only corrupts the namespace-level counter.

Each such event pair leaves `connections` one below the true number of live
streams. The drift accumulates, and once it reaches zero the namespace
disseminator is shut down and cached while streams are still attached —
`disseminator.handleShutdown` then closes all of them. The next `ConnAdd`
constructs a fresh disseminator, restarting `currentVersion` at zero.

A corroborating artifact: because emission point 2 never populates the `Error`
field, the resulting log line prints a nil cause, so a doubly-closed connection
is visible in the logs as two different messages for the same peer:

```
18:02:23.055380 warning Error receiving from stream 10.103.251.128:26572: rpc error: code = Canceled desc = context canceled
18:02:23.055408 info    Closing connection to 10.103.251.128:26572: %!s(<nil>)
```

## Steps to Reproduce

1. Run placement with a namespace containing many actor-hosting sidecars (~30
   in our case; more sidecars means faster drift accumulation).
2. Roll deployments frequently so sidecars disconnect while a dissemination
   round is in the LOCK phase. Each disconnect that races an in-flight
   `DisseminateLock` produces one `Error handling stream event *loops.DisseminateLock`
   line and drifts the counter by one.
3. After enough such events, observe every stream in the namespace closing in
   the same millisecond, all sidecars reconnecting, and the dissemination
   version restarting from a low number.

## Impact

Every sidecar in the namespace drops its placement stream simultaneously. On
reconnect each one halts its local actors, so any actor mid-call is interrupted
even though nothing was wrong with its host. For us this repeatedly killed live
game sessions on pods that were not being rolled.

## Proposed Fix

Make `recvLoop` the single emission point. On a send failure, cancel the stream
context instead of enqueueing; `recvLoop` then unwinds and reports the close
exactly once.

```diff
 	if err != nil {
 		log.Errorf("Error handling stream event %T on %s: %v", event, s.addr, err)
-		s.nsLoop.Enqueue(&loops.ConnCloseStream{
-			StreamIDx: s.idx,
-			Namespace: s.ns,
-		})
+		s.cancel(err)
 	}
```

This also preserves the real error as the close cause, removing the
`%!s(<nil>)` log lines.

The other two paths that close a stream from the server side already funnel
through the same place and are unaffected: `handleTimeout` closes a stream whose
`recvLoop` then reports one close for a connection that genuinely went away, and
on namespace shutdown the namespace is already removed from the map so
`handleCloseStream` returns early.

I have this change running with unit coverage and am happy to open a PR.
