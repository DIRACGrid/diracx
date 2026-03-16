# Operate the task system

This page covers day-to-day operational tasks for the DiracX task system. Many of these operations will be available through a monitoring dashboard in the future.

## Monitor streams

The broker uses nine Redis Streams (one per priority/size combination) named `diracx:tasks:{priority}:{size}`.

TODO: Document how to check stream lengths, consumer group lag, and pending message counts. In the interim, the Redis CLI can be used:

```bash
redis-cli XINFO STREAM diracx:tasks:normal:medium
redis-cli XINFO GROUPS diracx:tasks:normal:medium
redis-cli XLEN diracx:tasks:normal:medium
```

## Check scheduled tasks

The scheduler maintains a delayed task sorted set at `diracx:tasks:delayed`, where each member's score is the Unix timestamp when it should be promoted to a stream.

TODO: Document how to view upcoming scheduled tasks and their next run times.

```bash
# View the next 10 delayed tasks
redis-cli ZRANGEBYSCORE diracx:tasks:delayed -inf +inf WITHSCORES LIMIT 0 10
```

## Check locks

Locks are stored as Redis keys with prefixes `lock:mutex:`, `lock:rw:`, `limiter:rate:`, and `limiter:conc:`.

TODO: Document how to inspect lock state, identify stuck locks, and manually release locks if needed.

```bash
# List all active mutex locks
redis-cli KEYS "lock:mutex:*"

# Check a specific lock's TTL
redis-cli PTTL "lock:mutex:task:SyncOwnersTask:alice"
```

## Handle the dead-letter queue

Tasks marked with `dlq_eligible = True` that exhaust their retries are persisted to the `TaskDB` SQL database in the `dlq_tasks` table. Dead letter queue tasks have a status of `PENDING`, `DISPATCHED`, or `FAILED`.

TODO: Document how to query dead letter queue tasks, resubmit them, and remove successfully processed entries. This will be part of the monitoring dashboard effort.
