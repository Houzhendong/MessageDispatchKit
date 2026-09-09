# Message Dispatcher 交接文档

## 背景

该仓库提供两个同步处理型消息调度组件：

- `KeyedOrderedDispatcher<TKey, TMessage>`：同一个 key 严格顺序执行，不同 key 可并行。
- `MessageDispatcher<TInput, TOutput>`：无 key 的并行转换与订阅发布，不提供消息间顺序保证。

典型 workload 是解压、protobuf / MessagePack 反序列化和少量 CPU 处理。动态并行度使用吞吐量探测式 hill climbing，而不是根据消息积压直接计算 worker 数。

## 代码结构

```text
src/MessageDispatching/
  DispatcherOptions.cs
  DynamicScalingOptions.cs
  DispatcherStats.cs
  KeyedDispatcherStats.cs
  DispatcherScalingStats.cs
  DispatcherScaleChange.cs
  DynamicScalingPolicy.cs
  KeyedOrderedDispatcher.cs
  MessageDispatcher.cs

samples/DispatcherSample/
  Program.cs
  NoKeySample.cs

tests/MessageDispatching.Tests/
  DispatcherOptionsTests.cs
  DynamicScalingPolicyTests.cs
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

### 启用方式

`MaxParallelism == 0` 表示固定并行度。只有 `MaxParallelism > Parallelism` 时启用动态扩缩容。

- `Parallelism`：启动 worker 数，同时也是缩容下限。
- `MaxParallelism`：动态扩容上限。
- `DynamicScaling`：采样、收益阈值、warmup、cooldown 和 idle 缩容配置。

```csharp
new DispatcherOptions
{
    Parallelism = 4,
    MaxParallelism = 32,
    DynamicScaling = new DynamicScalingOptions
    {
        SampleInterval = TimeSpan.FromMilliseconds(500),
        MinimumUsefulThroughputGain = 0.02,
        ThroughputSmoothingFactor = 0.25,
        ProbeWarmupSamples = 1,
        ScaleUpCooldown = TimeSpan.FromSeconds(2),
        ScaleDownIdleDuration = TimeSpan.FromSeconds(5)
    }
}
```

### 核心信号

```text
Saturation
  -> 证明额外 worker 当前有独立工作可做

Throughput
  -> 证明新增 worker 是否值得保留
```

饱和条件：

```text
WorkerCount > 0
&& BusyWorkers >= WorkerCount
&& ReadyWorkItemCount > 0
```

- keyed：`ReadyWorkItemCount` 是 `ReadyKeyCount`。
- no-key：`ReadyWorkItemCount` 是 `QueuedMessageCount`。

可利用并行度近似：

```text
BusyWorkers + ReadyWorkItemCount
```

keyed dispatcher 不使用 `PendingMessages` 推断 worker 上限。一个 key 即使积压一百万条消息，有效并行度仍然只有一。

### Throughput 与 EWMA

每个 controller sample 使用累计完成数的 delta：

```text
Throughput = Delta CompletedMessages / Delta TimeSeconds
```

不能使用 `CompletedMessages / dispatcher lifetime`。实现使用实际 `Stopwatch` 时间，不在每条消息上记录 timestamp。

EWMA：

```text
Smoothed = alpha * Current + (1 - alpha) * Previous
```

第一个有效样本直接初始化 EWMA，不与人为的零值混合。

`CompletedMessages` 表示处理尝试已经结束：handler/transformer 成功或抛异常都计数；入队失败回滚 pending 时不计数。

### Scale-up probe

稳定状态且真实饱和时记录 baseline，然后使用持久化的自适应 probe step 试探。`NextProbeStep` 初始为 `1`；baseline throughput 为正时使用该值，为零时无论历史 step 多大都强制请求 `+1`：

```text
requestedStep = BaselineThroughput > 0 ? NextProbeStep : 1
safetyCap = WorkerCount <= 2
    ? WorkerCount
    : ceil(WorkerCount / 2)
actualStep = min(
    requestedStep,
    MaxParallelism - WorkerCount,
    RunnableParallelism - WorkerCount,
    safetyCap)
target = WorkerCount + actualStep
```

`ActiveProbeStep` 记录 cap 后的实际 target delta。后续收益计算和 step 调整都使用该实际 delta，而不是 cap 前的请求值。worker 实际创建完成后，忽略 `ProbeWarmupSamples` 个完整样本，再比较平滑吞吐量：

```text
gain = (ProbeThroughput - BaselineThroughput) / BaselineThroughput
relativeWorkerIncrease = ActiveProbeStep / BaselineWorkerCount
elasticity = gain / relativeWorkerIncrease
```

- `gain >= MinimumUsefulThroughputGain`：接受整个实际 probe step。
- 接受正 baseline probe 后：`elasticity >= 0.75` 将下一 step 设为实际 step 的两倍（饱和到 `int.MaxValue`）；`0.25 <= elasticity < 0.75` 保留实际 step；更弱的已接受收益将实际 step 减半，下限为 `1`。
- 非负有限收益不足而拒绝时，下一 step 设为实际 step 的一半，下限为 `1`；负收益、无 gain、无效 sample 或 probe 期间可利用并行度低于 target 时重置为 `1`。
- baseline 为零：probe throughput 转为正值才接受，否则回退；不计算相对 gain，接受后下一 step 也重置为 `1`。

拒绝会一次回退整个 `ActiveProbeStep`。失败 probe 的 cooldown 从实际 worker 数完成回退后开始。这样长时间运行的 handler 不会在 worker 尚未退出时消耗完整 cooldown，也不会在物理回退完成前开始下一次 probe。

### DesiredWorkerCount 与 worker 生命周期

Policy 只输出 `DesiredWorkerCount`，不保存 `Task`、worker 或 `CancellationTokenSource`。

```text
Actual < Desired -> dispatcher 创建 worker
Actual > Desired -> worker 在安全边界自然退出
```

retirement token 只用于唤醒阻塞在 channel wait 的空闲 worker，不会传入 handler、transformer 或 subscriber。

安全退休边界：

- keyed：处理并重新调度完整 key batch 之后，或获取下一个 ready key 之前。
- no-key：完整处理一条消息之后，或等待下一条消息之前。

正在执行的用户代码不会因为 rollback/缩容被 cancel。若用户代码永久阻塞，实际回退也会被相应延迟。

### Idle scale-down

缩容不根据 throughput 下降，因为 throughput 下降可能只是输入减少。

只有以下状态持续达到 `ScaleDownIdleDuration` 才将 desired workers 减一：

```text
ReadyWorkItemCount == 0
&& BusyWorkers < DesiredWorkerCount
&& WorkerCount == DesiredWorkerCount
&& DesiredWorkerCount > Parallelism
```

每次实际退休后重新等待完整 idle duration，始终 `-1` 保守缩容。触发 idle scale-down 时会把 `NextProbeStep` 重置为 `1`；workload 返回会重置 idle timer。

## Stats

### KeyedDispatcherStats

`KeyedOrderedDispatcher.GetStats()` 返回：

- `PendingMessages` / `CompletedMessages`
- `KeyCount` / `ReadyKeyCount`
- `WorkerCount` / `DesiredWorkerCount` / `BusyWorkers`
- `Throughput` / `SmoothedThroughput` / `IsSaturated`
- `ScaleUpCount` / `ScaleDownCount`
- `ProbeAcceptCount` / `ProbeRejectCount` / `LastProbeGain`
- `Accepting`

### DispatcherStats

`MessageDispatcher.GetStats()` 使用 `QueuedMessageCount` 替代 keyed 专属字段，其余 scaling 字段一致。

stats 是线程安全计数器的 best-effort 快照，不保证所有字段来自同一个原子瞬间。

## ScaleObserver

`ScaleObserver` 只报告实际 worker count 变化：

- 动态 worker 已创建：报告 scale up。
- probe accepted：不额外报告。
- probe rejected：desired 先回退，实际 worker 在安全边界退出后才报告 scale down。
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

处理逻辑使用实际 buffer CPU work，不用 `Thread.Sleep` 模拟吞吐。输出 CSV-friendly time series，包括 actual/desired/busy workers、pending/completed、ready count、raw/smoothed throughput、saturation、probe counters 和 gain。

最优 worker 数依赖 CPU 拓扑、运行时、功耗状态和宿主负载，因此 benchmark 只断言顺序和上限等正确性，不硬编码吞吐峰值对应的 worker 数。

## 验证

```powershell
dotnet build .\src\MessageDispatching\MessageDispatching.csproj
dotnet test .\tests\MessageDispatching.Tests\MessageDispatching.Tests.csproj
dotnet run --project .\samples\DispatcherSample\DispatcherSample.csproj
dotnet run --project .\benchmarks\MessageDispatching.ScalingBenchmarks\MessageDispatching.ScalingBenchmarks.csproj
```

并发退休、controller cancellation、Complete/Enqueue 竞态和 observer 时序需要通过重复运行完整测试套件验证。
