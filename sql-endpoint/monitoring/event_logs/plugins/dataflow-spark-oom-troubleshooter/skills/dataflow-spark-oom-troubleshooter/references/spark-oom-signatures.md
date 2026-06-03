# Spark OOM Signatures

## Failure Signatures

| Signature | Meaning | Confirm With | Usual Fixes |
|---|---|---|---|
| `exit code 137`, `SIGKILL`, `OOMKilled` | Kubernetes/container memory limit exceeded | Pod/container status, executor loss reason | Increase `memoryOverhead`, reduce executor cores, reduce per-task shuffle, check off-heap/native memory |
| `FetchFailedException` caused by `ExecutorDeadException` | Reducer could not fetch shuffle blocks because source executor died | Driver log executor-lost event before fetch failure | Fix executor death cause; stage with fetch failure may be victim |
| `Java heap space` | JVM heap exhausted | Executor stderr, heap metrics, GC logs | Increase executor memory, reduce task concurrency, reduce aggregation/join state |
| `GC overhead limit exceeded` | Heap is mostly spent in GC | GC time close to task time, executor logs | Reduce live set, partitions, cache pressure, skew; increase heap cautiously |
| `Direct buffer memory`, Netty fetch errors | Off-heap/direct/network buffers exhausted | Executor logs, direct memory errors | Increase overhead/off-heap, reduce concurrent fetches, reduce cores/parallelism per executor |
| Driver OOM during broadcast | Driver collected a large broadcast relation | Driver logs, broadcast exchange metrics | Lower broadcast threshold, increase driver memory, pre-reduce build side |

## Plan Patterns

### Large Broadcast

Look for:

```text
BroadcastExchange
BroadcastQueryStage, Statistics(sizeInBytes=2.1 GiB, rowCount=...)
BroadcastHashJoin ... BuildRight
```

Risk: the plan statistic is not the full in-memory cost. Hashed relation overhead, serialization, deserialization, executor storage, and concurrent tasks can multiply memory pressure.

`ReusedExchange` means Spark reused a broadcast/exchange logically. It does not mean the broadcast has no runtime memory footprint; the relation can still be resident while later shuffle-heavy tasks run.

### Shuffle-Heavy Stage With Small Output

Look for high shuffle read/write and low final output. The stage may still need large buffers and sort/spill state:

```text
UnsafeShuffleWriter.write
shuffle read: tens of GB
shuffle write: tens of GB
```

Small output does not prove low peak memory.

### AQE Coalescing

Look for:

```text
AQEShuffleRead ... coalesced
```

Risk: AQE may reduce task count and enlarge per-task input. If raising `spark.sql.shuffle.partitions` has no effect, inspect AQE coalescing and `spark.sql.adaptive.advisoryPartitionSizeInBytes`.

### Explode Of Zipped Arrays

Look for:

```text
Generate explode(arrays_zip(...))
```

Risk: Spark may materialize wide zipped array structs before exploding and filtering. This is worse when rows contain large arrays or many fields. Prefer filtering/exploding the original array of structs when possible.

### Few Large Executors

Symptoms:

- Very high `spark.executor.cores`, for example 64+ cores.
- Container OOM with low GC.
- Shuffle-heavy stages with many concurrent tasks per executor.

Risk: high task concurrency in one JVM/container creates aggregate direct memory, shuffle buffer, thread stack, broadcast, and execution memory pressure. One executor death also loses many shuffle blocks.

## Config Checks

Use these as starting points, not universal defaults:

```text
spark.executor.memoryOverhead = 10-15% of executor memory for large shuffle jobs
spark.sql.shuffle.partitions = 2x-8x total task cores for large shuffles, then tune from task sizes
spark.sql.adaptive.advisoryPartitionSizeInBytes = 64m or 128m for memory-sensitive shuffles
spark.sql.adaptive.coalescePartitions.enabled = false for a diagnostic run if AQE coalesces too aggressively
```

If broadcast is required for speed, keep it enabled but size memory overhead for broadcast plus shuffle concurrency.
