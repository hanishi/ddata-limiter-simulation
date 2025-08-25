# DData limiter simulation

A small experimental project to stress-test Apache Pekko Distributed Data (DData) using a rate limiter built on top of PNCounterMap.
The goal is to explore:
- Shard+rotate pattern for high-cardinality keys
- Behavior of per-key capacity limits and per-bucket distinct caps
- Gossip overhead and tombstone cleanup when windows expire
- Throughput vs. consistency trade-offs (WriteLocal vs. WriteMajority)

## How it works
- Buckets + Windows
Each key (like ip:1.2.3.4) is mapped into a (windowId, bucket) shard.
- windowMs: how long counters live (e.g. 60s)
- capacity: max count per key per window (e.g. 1 → one hit per key per minute)
- maxKeysPerBucket: limit distinct keys per bucket to avoid >100k entries/CRDT

## Rotation
Only the current and recent windows are kept. 
Older buckets are cleaned up by askDelete, throttled by deleteBatchPerTick.
- Consistency
- WriteLocal (fast, eventual) for best-effort flood control
- WriteMajority (slower) if stricter consistency is desired

# Consistency
- WriteLocal (fast, eventual) for best-effort flood control 
- WriteMajority (slower) if stricter consistency is desired

# Running the simulation

## Single node
```
sbt "limiterSim/run"
```
Logs show throughput and heap usage every ~0.5s:

```
throughput: grants=0 denials=33 (5.000803958s window of 85.010446875 total seconds)
Heap used=27MB total=40MB max=4096MB
```

## Multi node
```
sbt "limiterSim/run 17356"
sbt "limiterSim/run 17357"
sbt "limiterSim/run 17358"
```