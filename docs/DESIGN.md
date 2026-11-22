# Design

## Problem

A product page needs a query that ranks the product in its category by aggregating recent orders. It's correct but slow. Caching it is easy. Keeping the cache correct while data changes, processes multiply and Redis fails is the hard part.

## Goals, in order

1. **Never leave a wrong value behind.** A write may be invisible for a moment, but it must not stay invisible.
2. **Protect the database** when a popular key goes cold.
3. **Stay up without Redis.** Redis makes reads faster and isn't needed to serve them.
4. Be fast.

## Read path

1. The in-process LRU. A fresh entry returns at once.
2. A coalesced lookup, so concurrent readers of one key in one process share it.
3. Redis. A fresh entry is copied into the LRU.
4. A short Redis lock, then Postgres. Other processes wait for the result or for the lock.

Each entry has a soft and a hard TTL. Past the soft one, readers get the old value and a single refresh runs. Past the hard one, nothing stale is served.

## Key decisions

### A version guard on every store

Deleting a key after a write isn't enough:

1. Reader A loads the old price from Postgres.
2. A writer commits a new price and deletes the key.
3. Reader A stores the old price, which stays until its TTL.

So each invalidation replaces a per-key version in Redis. A reader notes the version before loading, and a Lua script stores the value only if the version is unchanged. Versions are unique tokens and not counters, because Redis can evict or expire one mid-load, and a counter would then restart at a value that looks unchanged. The in-process layer has the same problem, so an invalidation also marks fills in progress, and they don't store.

There's a third form: a reader arriving after the write could join a coalesced load that began before it. Invalidation therefore also forgets the load in progress.

The benchmark's write storm runs with and without the guard. [bench-results.md](bench-results.md) counts the wrong values each leaves behind.

| Rejected | Why |
| --- | --- |
| Delete after write, no version | The race above. |
| Delete, wait, delete again | Narrows the window without closing it, and the delay is a guess. |
| Write the new value into the cache | Two writers can finish in a different order in the cache than in Postgres. |
| TTL only | Simple, and correct eventually. Reads stay stale for a whole TTL after each write. |

### Pub/sub for the in-process layer

Each process keeps its own copy, so each must hear about writes. Redis pub/sub is fast but doesn't guarantee delivery, so a process that reconnects clears its memory, and the L1 TTL bounds anything else.

### A lock across processes, which readers may ignore

Coalescing limits a stampede to one load per process. The lock limits it to one in total. A reader that waits too long loads anyway, because a dead lock holder must not take down a product page. **Cost:** waiting readers poll every 15 ms, which the stampede table shows as higher latency than coalescing alone. With few processes and a cheap query, the lock isn't worth it.

### TinyLFU admission

Plain LRU lets a one-off scan evict popular entries. TinyLFU admits a key only if a count-min sketch says it's been read more often than the entry it would replace. **Cost:** it's slower to accept newly popular keys. [policy-results.md](policy-results.md) shows both sides. W-TinyLFU adds a small LRU window to reduce that cost, and I haven't implemented it.

| Operation | Time | Memory |
| --- | --- | --- |
| LRU get, set, evict | O(1) | O(capacity) |
| Sketch update, estimate | O(4) | 4 counters of 1 byte per slot, at least 8 slots per entry |
| Sketch aging | O(width), amortized O(1) per access | |

### A timeout and circuit breaker around Redis

A dead Redis fails fast, but a hung one makes every request wait. Commands time out after 100 ms, and five failures in a row skip Redis for a second. Without Redis, in-process coalescing and the LRU still protect Postgres.

## Failure modes

| Failure | Behaviour |
| --- | --- |
| Redis down or hung | Reads continue from memory and Postgres. Staleness is bounded by the L1 TTL in each process. |
| Write while Redis is unreachable | Committed. Its invalidation is queued and sent on reconnect. Other processes may be stale until then. |
| Process crashes between commit and invalidation | Stale until the TTL expires. An outbox would close this gap, as in Booking Integrity Lab. |
| Missed pub/sub message | Memory is cleared on reconnect. Otherwise bounded by the L1 TTL. |
| Lock holder crashes | Waiters continue after the wait limit, and the lock expires. |
| Origin error | Passed to every waiter and not cached. |

## Limits

- **No authentication.** Anyone who can reach the service can change a price.
- **Expiry uses each process's clock**, so clock skew shifts TTLs. It can't affect the version guard.
- **Queued invalidations are in memory** and are lost if that process also crashes.
- **No limit on concurrent origin loads** for many different cold keys, beyond the connection pool's queue.
- **One Redis and one Postgres.** Redis failover could bring back an older version token, and a replica read could reintroduce stale loads.
- **The invalidation key and its version** must share a hash slot on Redis Cluster, which needs key hash tags that aren't here.
- **Benchmarks ran on one machine.** They compare designs and aren't capacity figures.

## A measurement lesson

My first benchmark said a fully cached service topped out near 2,600 requests/s. That was wrong. The load generator was the bottleneck, then Windows socket writes were. CPU profiling showed the cache lookup at 0.7% of the time. I moved load generation to separate threads, ran it in Linux, and added an empty-endpoint baseline, so the results show where HTTP itself is the limit.
