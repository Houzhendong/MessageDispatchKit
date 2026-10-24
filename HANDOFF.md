# Message Dispatcher 交接文档

## 背景

该仓库提供两个同步处理型消息调度组件：

- `KeyedOrderedDispatcher<TKey, TMessage>`：同一个 key 严格顺序执行，不同 key 可并行。
- `MessageDispatcher<TInput, TOutput>`：无 key 的并行转换与订阅发布，不提供消息间顺序保证。

典型 workload 是解压、protobuf / MessagePack 反序列化和少量 CPU 处理。两个 dispatcher 共用目标利用率、队列等待和资源预算驱动的负载匹配控制器。

## 代码结构

```text
src/MessageDispatching/
  DispatcherOptions.cs
  DynamicScalingOptions.cs
  DispatcherStats.cs
  KeyedDispatcherStats.cs
  DispatcherScalingStats.cs
  DispatcherScaleChange.cs
  LoadMatchingScalingPolicy.cs
  DispatcherLoadMonitor.cs
  WorkerRegistration.cs
  KeyedOrderedDispatcher.cs
  MessageDispatcher.cs

samples/DispatcherSample/
  Program.cs
  NoKeySample.cs

tests/MessageDispatching.Tests/
  DispatcherOptionsTests.cs
  LoadMatchingScalingPolicyTests.cs
  KeyedOrderedDispatcherTests.cs
  MessageDispatcherTests.cs

benchmarks/MessageDispatching.ScalingBenchmarks/
  MessageDispatching.ScalingBenchmarks.csproj
  Program.cs
```

## KeyedOrderedDispatcher

### 调度模型

不要使用固定 hash 分区：

```text
hash(key) % partitionCount -> 固定 worker
```

固定分区会让碰撞到同一分区的热点 key 被迫串行。当前实现使用：

```text
每个 key 一个 SingleReader=true / SingleWriter=false channel
每个 active key 至多一个全局 ready token
全局 worker pool
同一时刻一个 key 至多一个 consumer
```

`KeyState.Active` 保证同 key 不会并发消费。`UnreservedMessages` 在 CAS gate 内维护，worker 每次预留最多 `KeyBatchSize` 条消息，再在 gate 外调用 handler。batch 完成后，有剩余消息的 key 会重新进入 ready-key channel。

`_states` 是 copy-on-write `FrozenDictionary`：已知 key 热路径无锁读取，首次出现新 key 时才加锁重建并发布快照。当前不会移除 key 状态。

### 顺序语义

保证：

```text
同一 key 的 channel 接受顺序 == handler 开始处理顺序
同一 key 永远不会同时由两个 worker 执行
```

不保证不同 key 的全局顺序，也不保证 handler 自己启动的 fire-and-forget 副作用顺序。需要顺序语义的操作必须在同步 handler 返回前完成。

### 基础用法

```csharp
public sealed class UserEventHandler : IKeyedMessageHandler<long, UserEvent>
{
    public void Handle(long userId, UserEvent message, CancellationToken ct)
        => userService.Handle(message, ct);

    public void HandleError(long userId, UserEvent message, Exception ex, CancellationToken ct)
        => deadLetterQueue.Write(userId, message, ex, ct);
}

await using var dispatcher = new KeyedOrderedDispatcher<long, UserEvent>(
    new DispatcherOptions
    {
        Parallelism = 4,
        MaxParallelism = 16,
        KeyBatchSize = 32
    });

dispatcher.Start(new UserEventHandler());
dispatcher.Enqueue(userId, userEvent, ct);
await dispatcher.CompleteAsync(ct);
```

## MessageDispatcher

所有输入进入一个全局 unbounded channel。worker 同步调用 `IMessageTransformer<TInput,TOutput>.Transform`，然后依次同步发布给当前订阅者快照。

```csharp
await using var dispatcher = new MessageDispatcher<ReadOnlyMemory<byte>, BroadcastEvent>(
    new DispatcherOptions
    {
        Parallelism = 4,
        MaxParallelism = 16
    });

using var subscription = dispatcher.Subscribe(new BroadcastEventSubscriber());
dispatcher.Start(new BroadcastEventParser());
dispatcher.Enqueue(payload, ct);
await dispatcher.CompleteAsync(ct);
```

当 `EffectiveMaxParallelism == 1` 时，使用专用 MPSC 单 consumer 路径，不启动 scaling controller。

普通 dispatcher 的吞吐量包含 transformer 和同步 subscriber fan-out 的总耗时，因为它们共同占用 worker。

## 动态扩缩容

两个 dispatcher 都使用 `LoadMatchingScalingPolicy`。算法、配置、API 迁移和指标见
[负载匹配扩缩容](docs/load-matching-scaling.md)。
历史新旧版本实测及复现脚本见 [对比程序](benchmarks/MessageDispatching.LoadComparison/README.md)。
旧吞吐 probe 状态机已删除，对照版本直接从 git 提交构建。

- `MaxParallelism == 0` 或等于 `Parallelism`：固定并行度。
- `MaxParallelism > Parallelism`：启用动态调整；`Parallelism` 是启动数量及缩容下限。
- 基础需求为 `ceil(平均 busy workers / TargetUtilization)`。
- 就绪工作等待超过 `TargetQueueWait` 时增加需求，目标受独立工作量及资源预算限制。
- keyed 的独立工作量是去重活跃 key 数；普通 dispatcher 的每条消息都能独立执行。
- 扩容按 `ScaleUpInterval` 分步进行，CPU 压力达到阈值时暂停增员。
- 部分负载持续达到 `ScaleDownIdleDuration` 后也可以缩容，不要求所有 worker 空闲。
- 吞吐只作观测和辅助容量校准，不再执行逐次 probe。

`DispatcherLoadMonitor` 汇总 worker busy 时间、就绪等待和进程 CPU。
普通 dispatcher 的 busy 时间包含转换和同步 subscriber 投递。
正在执行的用户代码不会因缩容取消；keyed 在完整 batch 处理并重排后退休，
普通 worker 在完整消息处理之后退休。阻塞在 channel wait 的空闲 worker 通过独立 token 唤醒退休。

## Stats

### KeyedDispatcherStats

`KeyedOrderedDispatcher.GetStats()` 返回：

- `PendingMessages` / `CompletedMessages`
- `KeyCount` / `ReadyKeyCount`
- `WorkerCount` / `DesiredWorkerCount` / `BusyWorkers`
- `Throughput` / `SmoothedThroughput` / `IsSaturated`
- `ScaleUpCount` / `ScaleDownCount`
- `Accepting`
- `ActiveKeyCount` / `AverageBusyWorkers` / `ReadyKeyWaitMilliseconds`
- `CapacityLimit` / `ProcessCpuUtilization`

### DispatcherStats

`MessageDispatcher.GetStats()` 使用 `QueuedMessageCount` 替代 keyed 专属字段，
提供 `AverageBusyWorkers`、`QueueWaitMilliseconds`、`CapacityLimit` 和 `ProcessCpuUtilization`。

stats 是线程安全计数器的 best-effort 快照，不保证所有字段来自同一个原子瞬间。

## ScaleObserver

`ScaleObserver` 只报告实际 worker count 变化：

- 动态 worker 已创建：报告 scale up。
- 缩容建议先降低 desired，实际 worker 在安全边界退出后才报告 scale down。
- 初始 worker 和关闭时的 worker 退出保持静默。

```csharp
ScaleObserver = change =>
    logger.LogInformation(
        "Dispatcher scaled {Previous} -> {Current}, desired={Desired}, ready={Ready}",
        change.PreviousWorkerCount,
        change.CurrentWorkerCount,
        change.Stats.DesiredWorkerCount,
        change.Stats.ReadyWorkItemCount)
```

`DispatcherScaleChange.Stats` 是公共 `DispatcherScalingStats`，其中 `ReadyWorkItemCount` 对两个 dispatcher 使用各自正确的 ready 单位。observer 异常会被吞掉，不影响处理和 worker 生命周期。

## 入队、背压与生命周期

两个 dispatcher 都使用 unbounded channel，`Enqueue` 同步且无背压。高峰积压由内存承接；需要限流时应在 dispatcher 外实现。

正常关闭：

```csharp
dispatcher.Complete();
await dispatcher.CompleteAsync(ct);
```

`Complete()` 停止接收新消息，但 accepted backlog 排空前 controller 仍可扩容。pending 归零后 desired 设为零、停止 controller、完成 channel，worker 正常退出。

强制释放：

```csharp
await dispatcher.DisposeAsync();
```

会停止接收并取消 worker，适用于服务停止或异常退出。

## 错误处理

handler/transformer 异常会调用各自的 `HandleError`。subscriber 异常调用 subscriber 的 `HandleError`，且不会阻止继续投递给其他 subscriber。错误处理自身的非取消异常会被 dispatcher 吞掉。

当前不自动重试。业务层应自行决定重试、死信和幂等策略。

## Benchmark

`benchmarks/MessageDispatching.ScalingBenchmarks` 是无第三方依赖的 Release 控制台观测工具，覆盖：

- CPU-bound homogeneous
- 1KB / 10KB / 100KB / 1MB heterogeneous
- 1000 keys
- single hot key
- 4 hot keys / max 32 workers
- low-to-high burst
- ordinary dispatcher homogeneous

处理逻辑使用实际 buffer CPU work，不用 `Thread.Sleep` 模拟吞吐。输出 CSV-friendly time series，包括 actual/desired/busy workers、pending/completed、ready count、raw/smoothed throughput、saturation、平均占用、队列等待、容量上限和进程 CPU。

最优 worker 数依赖 CPU 拓扑、运行时、功耗状态和宿主负载，因此 benchmark 只断言顺序和上限等正确性，不硬编码吞吐峰值对应的 worker 数。

## 验证

```powershell
dotnet build .\src\MessageDispatching\MessageDispatching.csproj
dotnet test .\tests\MessageDispatching.Tests\MessageDispatching.Tests.csproj
dotnet run --project .\samples\DispatcherSample\DispatcherSample.csproj
dotnet run --project .\benchmarks\MessageDispatching.ScalingBenchmarks\MessageDispatching.ScalingBenchmarks.csproj
```

并发退休、controller cancellation、Complete/Enqueue 竞态和 observer 时序需要通过重复运行完整测试套件验证。
