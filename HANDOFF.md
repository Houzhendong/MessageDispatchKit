# Keyed Ordered Dispatcher 交接文档

## 背景

远程服务会实时推送内部协议消息。每个消息包由 `MessageType` 和 `ReadOnlyMemory<byte>` 组成，每个 `MessageType` 对应一个具体 protobuf 类型。

目标是在 C# 中实现统一消息总线：

- 总线接入所有推送消息。
- 按 `MessageType` 分 worker。
- worker 负责 protobuf 反序列化。
- 反序列化后的消息交给业务 handler。
- 每个 `MessageType` 支持并行处理。
- 同一个业务 key 内部保持顺序，不同 key 尽可能并行。

本次落地的是其中的核心调度组件：`KeyedOrderedDispatcher<TKey, TMessage>`。

## 当前代码结构

```text
src/
  MessageDispatching/
    MessageDispatching.csproj
    KeyedOrderedDispatcher.cs
    DispatcherOptions.cs
    DispatcherStats.cs
    IKeyedMessageHandler.cs
    IMessageTransformer.cs
    IMessageSubscriber.cs
    MessageDispatcher.cs

samples/
  DispatcherSample/
    DispatcherSample.csproj
    NoKeySample.cs
    Program.cs

tests/
  MessageDispatching.Tests/
    MessageDispatching.Tests.csproj
    KeyedOrderedDispatcherTests.cs
    MessageDispatcherTests.cs
    TestWait.cs
```

核心文件说明：

- `KeyedOrderedDispatcher.cs`
  - 实现按 key 保序、跨 key 并行。
  - 不绑定 protobuf，不绑定 `MessageType`。
  - 上游反序列化完成后调用同步方法 `Enqueue(key, message)`。
  - 每个 key 内部用一个单读者、多写者 unbounded channel 存队列，同一 key 并发 `Enqueue` 是线程安全的。写入在 CAS 自旋锁临界区外（先写队列、后在锁内递增计数，计数只滞后不超前，详见 `Enqueue` 内注释）；单读者语义由 Active 标志保证。

- `IKeyedMessageHandler.cs`
  - 业务 handler 和错误处理合并在同一个接口中，方法均为**同步** `void`。
  - 由调用方实现，并通过 `dispatcher.Start(handler)` 注入；handler 注入后才启动 worker。

- `DispatcherOptions.cs`
  - 配置并行度、keyed dispatcher 的单 key 批处理大小、动态扩缩容阈值。
  - 不再有积压上限：入队无背压、不限制最大入队数。

- `DispatcherStats.cs`
  - 暴露当前积压消息数、已知分区数、worker 数、忙碌 worker 数、已排队 work item 数、是否继续接收消息。

- `samples/DispatcherSample/Program.cs`
  - 可运行示例。
  - 演示多个热点 key 不会因为固定 hash 分区而互相阻塞。
  - 验证同一个 key 内部序号递增。

## 核心设计

不要使用固定 hash 分区模型：

```text
hash(key) % partitionCount -> 固定 partition -> 固定 worker
```

这个模型的问题是：多个热点 key 如果碰巧落在同一个 partition，会被同一个 worker 串行处理，无法最大化并行。

当前实现使用动态调度模型：

```text
每个 key 一个 SPSC unbounded channel 队列
全局 bounded ready key 队列，容量为 `EffectiveMaxParallelism`
全局 worker pool，可选动态扩容
同一时刻一个 key 最多只被一个 worker 处理
不同 key 可以被不同 worker 并行处理
```

每个 `KeyState` 的并发细节：

- 队列是 `SingleReader=true, SingleWriter=true` 的 unbounded channel。
- 用 CAS（`Interlocked.CompareExchange`）实现自旋锁，`Acquire()` 返回 `IDisposable`，配合 `using` 进出临界区。
- 上游保证同一 key 单线程写入（满足 channel 的 single-writer 契约），`Active` 标志保证同一 key 至多一个消费者（满足 single-reader 契约）。
- handler 调用和消费者 `TryRead` 排空都在锁外，不阻塞其他 key。
- ready-key channel 使用 bounded channel，容量等于启用动态扩容后的最大 worker 数。热路径先 `TryWrite`，如果队列满则挂起异步 `WriteAsync` 等待槽位，避免 worker 在重调度时同步阻塞造成死锁。
- 调度判定不依赖 channel 自带的 `Reader.Count`（`SingleConsumerUnboundedChannel` 不支持），而是用 `KeyState.UnreservedMessages` 计数；worker 会先在临界区内预留最多 `KeyBatchSize` 条消息，再到锁外读取处理。
  - key 状态不再移除；`_states` 使用 copy-on-write `FrozenDictionary` 快照 cache，已知 key 的热路径只做无锁 `TryGetValue`，首次出现新 key 时才加锁重建并发布新快照。

- `MessageDispatcher.cs`
  - 实现无 key 的普通消息 dispatcher。
  - 输入类型和输出类型可以不同：`Enqueue(TInput)` 后，worker 并行执行 `IMessageTransformer<TInput, TOutput>`，再发布 `TOutput`。
  - 不提供 key 内顺序语义；所有输入消息进入一个全局 unbounded channel。
  - 如果 `EffectiveMaxParallelism == 1`，输入队列使用 `SingleReader=true, SingleWriter=false` 的 MPSC 模式；否则使用多读多写模式。
  - 固定单 worker 模式会直接短路动态扩缩容路径：不启动 scale controller，worker 使用专用单 consumer 循环，不进入 retire/scale 判断。
  - 支持多个 `IMessageSubscriber<TOutput>` 订阅转换后的输出消息。
  - 同样支持 `Start(transformer)` 延迟启动、同步 `Enqueue(input)`、`CompleteAsync()` 排空、动态扩缩容。
  - 适用于不需要按业务 key 保序、只需要并行转换消息（例如 protobuf 反序列化）并发布给下游订阅者的场景。

效果：

- 同 key：严格 FIFO。
- 不同 key：动态分配给 worker，尽量并行。
- 热点 key：不会阻塞其他 key 的调度。
- 多个热点 key：可以被多个 worker 同时处理。
- 如果设置 `MaxParallelism > Parallelism`，负载上升时 worker 数可以从初始值扩到上限。

需要注意：如果单个 key 自身极热，并且业务要求该 key 严格顺序，那么这个 key 本身无法真正并行。这是顺序语义的硬约束。

## 使用方式

### keyed dispatcher 基础用法

先实现 handler 接口（同步 `void` 方法）：

```csharp
public sealed class UserEventHandler : IKeyedMessageHandler<long, UserEvent>
{
    public void Handle(long userId, UserEvent message, CancellationToken ct)
        => userService.Handle(message, ct);

    public void HandleError(long userId, UserEvent message, Exception ex, CancellationToken ct)
        => deadLetterQueue.Write(userId, message, ex, ct);
}
```

再构造 dispatcher：

```csharp
await using var dispatcher = new KeyedOrderedDispatcher<long, UserEvent>(
    new DispatcherOptions
    {
        Parallelism = 16,
        KeyBatchSize = 32
    });

dispatcher.Start(new UserEventHandler());

dispatcher.Enqueue(userId, userEvent, ct);
await dispatcher.CompleteAsync(ct);
```

与 protobuf worker 集成时，推荐流程：

```csharp
var message = UserEvent.Parser.ParseFrom(payload.Span);
var key = message.UserId;

dispatcher.Enqueue(key, message, ct);
```

如果 key 在协议头中已经存在，优先从协议头取 key，避免为了路由提前解析完整 protobuf。

### no-key dispatcher 基础用法

如果消息不需要按业务 key 保序，并且需要并行执行转换后发布给订阅者，可以使用 `MessageDispatcher<TInput, TOutput>`：

```csharp
public sealed class BroadcastEventParser : IMessageTransformer<ReadOnlyMemory<byte>, BroadcastEvent>
{
    public BroadcastEvent Transform(ReadOnlyMemory<byte> payload, CancellationToken ct)
        => BroadcastEvent.Parser.ParseFrom(payload.Span);

    public void HandleError(ReadOnlyMemory<byte> payload, Exception ex, CancellationToken ct)
        => logger.LogError(ex, "Parse broadcast failed.");
}

public sealed class BroadcastEventSubscriber : IMessageSubscriber<BroadcastEvent>
{
    public void Handle(BroadcastEvent message, CancellationToken ct)
        => broadcastService.Handle(message, ct);
}

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

no-key dispatcher 使用全局输入队列，不保证消息之间的业务顺序；吞吐上限由当前 worker 数、转换耗时和下游订阅者处理耗时决定。转换失败不会发布输出；订阅者抛异常不会阻止继续投递给其他订阅者。

## 推荐接入形态

每个 `MessageType` 可以拥有自己的 protobuf worker 和 dispatcher：

```text
MessageBus
  -> MessageType.UserEvent
      -> Worker<UserEvent>
          -> ParseFrom(...)
          -> KeyedOrderedDispatcher<long, UserEvent>

  -> MessageType.OrderEvent
      -> Worker<OrderEvent>
          -> ParseFrom(...)
          -> KeyedOrderedDispatcher<long, OrderEvent>
```

这样可以做到：

- 不同 `MessageType` 之间隔离。
- 每个 `MessageType` 独立配置并行度和积压上限。
- 每个 `MessageType` 独立定义 key 选择逻辑。
- 每个 `MessageType` 独立处理错误、重试和死信。

## 关键配置

### Parallelism

初始 worker 数量。

```csharp
Parallelism = 16
```

在未启用动态扩容时，它限制同一个 dispatcher 内最多同时处理多少个 key。

如果某个 `MessageType` handler 是 IO 密集型，可以适当调高。如果是 CPU 密集型，通常接近 CPU 核数或略高即可。

### MaxParallelism

动态扩容上限。默认值 `0` 表示不启用动态扩容，固定使用 `Parallelism`。

```csharp
Parallelism = 1,
MaxParallelism = 4
```

当前验证性实现支持扩容和空闲缩容。扩容条件是：

- 当前 worker 数小于 `MaxParallelism`。
- `ScaleInterval` 周期采样中连续 `ScaleUpConsecutiveSamples` 次命中扩容条件。
- 当前 worker 全部处于忙碌状态。
- 已排队 work item 数达到 `ScaleUpQueuedWorkItemsThreshold`。
- 全局 pending 消息数达到 `workerCount * ScaleUpMessagesPerWorkerThreshold`。
- 距离上次扩容至少经过 `ScaleUpCooldown`。

扩容不会破坏同 key 顺序，因为 `KeyState.Active` 仍保证同一时刻一个 key 至多被一个 worker 处理。

`Complete()` 后如果仍有已入队消息待排空，controller 仍允许扩容；当 pending 归零或 `DisposeAsync()` 取消时 controller 退出。

缩容由空闲 worker 自愿退出完成，不会取消正在处理的 worker。worker 等待 work item 超过 `ScaleDownIdleDuration` 后，如果当前 worker 数大于 `Parallelism`，且 `PendingMessages == 0`、`QueuedWorkItems == 0`，则该 worker 退出。缩容下限是 `Parallelism`。

### ScaleInterval

```csharp
ScaleInterval = TimeSpan.FromMilliseconds(200)
```

动态扩容 controller 的采样间隔。扩容判断不在 `Enqueue` 或 ready-key 调度热路径上执行。

### ScaleUpCooldown

```csharp
ScaleUpCooldown = TimeSpan.FromSeconds(1)
```

两次扩容之间的最小间隔，用于避免多路长连接持续推送时快速冲到 `MaxParallelism`。

### ScaleDownIdleDuration

```csharp
ScaleDownIdleDuration = TimeSpan.FromSeconds(30)
```

超过该空闲时长后，额外 worker 会尝试缩容退出。只有空闲 worker 会退出，正在处理消息的 worker 不会被中断。

### ScaleUpQueuedWorkItemsThreshold

```csharp
ScaleUpQueuedWorkItemsThreshold = 2
```

触发扩容所需的已排队 work item 数。keyed dispatcher 中 work item 是 ready key；no-key dispatcher 中 work item 是全局输入队列里的待转换消息。

### ScaleUpMessagesPerWorkerThreshold

```csharp
ScaleUpMessagesPerWorkerThreshold = 8
```

触发扩容所需的每 worker pending 消息数。实际判断为 `PendingMessages >= WorkerCount * ScaleUpMessagesPerWorkerThreshold`。

### ScaleUpConsecutiveSamples

```csharp
ScaleUpConsecutiveSamples = 2
```

扩容条件必须连续命中的采样次数，用于过滤瞬时尖峰。当前策略仍是验证性策略，后续可根据真实负载继续引入平均处理耗时或 pending 增长速率。

### 入队与背压

当前实现**不限制最大入队数，也没有背压**。`Enqueue` 是同步方法，不会因积压等待空位。

`Enqueue` 热路径不再持有生命周期锁。全局 `_pendingMessages` 使用 `Interlocked` 做乐观计数，`_accepting` / `_disposed` 使用 `Volatile` 读写；与 `Complete()` / `DisposeAsync()` 并发时允许状态短暂变脏，失败路径会回滚全局 pending 计数。

代价：高峰期积压完全靠内存兜底。上游如果推送速度可能长时间超过处理速度，需要在 dispatcher 之外自行做限流或背压（例如上游 channel、令牌桶、或按 `GetStats().PendingMessages` 主动降速）。

### KeyBatchSize

单个 key 每次被 worker 取到后最多连续处理多少条。

```csharp
KeyBatchSize = 32
```

作用：

- 减少频繁调度开销。
- 防止超热 key 长时间霸占 worker。
- 在吞吐和公平性之间做平衡。

如果普通 key 延迟敏感，可以调小。如果热点 key 很多且吞吐优先，可以调大。

### key 状态 cache

当前实现不考虑移除 key 的场景。`_states` 是一个读多写少 cache：所有 key 至少进入一次之后，字典结构基本稳定，后续入队通过已发布的 `FrozenDictionary` 快照无锁命中对应 `KeyState`。

首次遇到新 key 时会在写锁内基于当前快照重建字典、加入新 `KeyState`，转换为 `FrozenDictionary` 后再用 `Volatile.Write` 发布。已有快照发布后不再原地修改。

## 生命周期

正常关闭：

```csharp
dispatcher.Complete();
await dispatcher.CompleteAsync(ct);
```

语义：

- `Complete()` 停止接收新消息。
- 已经入队的消息会继续处理。
- 全部处理完成后 worker 退出。

强制释放：

```csharp
await dispatcher.DisposeAsync();
```

语义：

- 停止接收新消息。
- 取消 worker。
- 用于服务停止、异常退出或容器释放。

生产环境中建议在应用停止钩子里优先调用 `CompleteAsync`，给已有消息排空机会。

## 错误处理

handler 抛异常时，dispatcher 会捕获异常，并调用同一个 `IKeyedMessageHandler` 上的 `HandleError`（同步 `void`，默认空实现）。

```csharp
public sealed class UserEventHandler : IKeyedMessageHandler<long, UserEvent>
{
    public void Handle(long key, UserEvent message, CancellationToken ct)
    {
        userService.Handle(message, ct);
    }

    public void HandleError(long key, UserEvent message, Exception ex, CancellationToken ct)
    {
        logger.LogError(ex, "Handle message failed. Key={Key}", key);
        deadLetterQueue.Write(key, message, ex, ct);
    }
}
```

`HandleError` 自身再抛异常（非取消）会被 dispatcher 吞掉，不影响后续处理；日志/重试失败要在 handler 内部消化。

当前实现不会自动重试。建议根据业务场景选择：

- 可重试错误：外层接 Polly 或自定义重试。
- 不可重试错误：写入死信队列。
- 顺序强依赖场景：谨慎重试，避免后续消息越过失败消息造成业务状态不一致。

## 顺序语义说明

该 dispatcher 保证：

```text
同一个 key 入队顺序 == handler 开始处理顺序
```

并且同一个 key 不会同时被两个 worker 处理。

但它不保证：

- 不同 key 之间的全局顺序。
- handler 内部异步副作用的外部可见顺序。
- handler 自己启动后台任务后的顺序。

因此 handler 内部不要 fire-and-forget。需要顺序语义的操作必须在 handler 返回前完成。

## 热点 key 的处理建议

如果是多个热点 key 集中在同一个固定 hash 分区，当前动态调度模型已经解决。

如果是单个 key 自己极热，并且该 key 必须严格顺序，则无法通过 dispatcher 并行化这个 key。可选优化方向：

- 重新定义更细粒度 key，例如从 `UserId` 改成 `(UserId, OrderId)`。
- 拆分处理阶段，例如 parse、validate、enrich 并行，最终状态提交按 key 串行。
- 对同 key 消息做批处理或合并。
- 将操作设计成幂等、可交换或基于版本号覆盖。
- 给超热 key 单独资源池，避免影响普通 key。

## 运行验证

构建库项目：

```powershell
dotnet build .\src\MessageDispatching\MessageDispatching.csproj
```

运行示例：

```powershell
dotnet run --project .\samples\DispatcherSample\DispatcherSample.csproj
```

运行单元测试：

```powershell
dotnet test .\tests\MessageDispatching.Tests\MessageDispatching.Tests.csproj
```

示例期望行为：

- `hot-a` 内部序号递增。
- `hot-b` 内部序号递增。
- `cold-c` 内部序号递增。
- 输出的 key 之间会交错，说明跨 key 并发处理。
- 最后会输出类似 `max concurrency observed: 2`，具体峰值取决于采样时序和当前负载。
- 启用动态扩缩容的示例会输出类似 `peak workers observed: 2` 和 `workers after scale down: 1`，说明 worker 从初始 `Parallelism = 1` 扩容到了多个，并在空闲后回落到下限。
- no-key 示例会输出类似 `no-key peak workers observed: 2` 和 `no-key workers after scale down: 1`，说明无 key 转换 dispatcher 也支持动态扩缩容。
- no-key 固定单 worker 示例会输出类似 `no-key mpsc published count: 4` 和 `no-key mpsc max concurrency observed: 1`，验证 MPSC 模式。

## 后续待补

建议后续补充以下内容：

- 正式 `MessageBus` 注册表。
- `MessageType -> protobuf parser -> keySelector -> dispatcher` 的注册 API。
- 单元测试扩展：
  - `KeyBatchSize` 让步。
  - 新 key 并发首次入队时只创建并发布一个有效 `KeyState`。
  - 更高并发下的竞态压力测试。
- 指标：
  - 每个 `MessageType` 积压数。
  - 每个 key 的积压数采样。
  - handler 耗时。
  - parse 失败数。
  - handler 失败数。
  - 死信数。
- 生产日志接入。
- 死信队列或失败消息存储。
- 取消和停机策略接入宿主服务生命周期。

## 当前验证结果

已执行：

```powershell
dotnet build .\src\MessageDispatching\MessageDispatching.csproj
dotnet build .\samples\DispatcherSample\DispatcherSample.csproj
dotnet run --no-build --project .\samples\DispatcherSample\DispatcherSample.csproj
dotnet test .\tests\MessageDispatching.Tests\MessageDispatching.Tests.csproj
```

结果：

- 编译通过。
- 示例运行通过。
- 每个 key 内部顺序保持递增。
- 不同 key 实际发生并行处理。
- 动态扩缩容验证通过：示例从 `Parallelism = 1` 开始，峰值观察到 `peak workers observed: 2`、`max concurrency observed: 2`，空闲后观察到 `workers after scale down: 1`。
- no-key dispatcher 验证通过：24 条 raw packet 全部转换并发布，峰值观察到 `no-key peak workers observed: 2`、`no-key transform concurrency observed: 2`，空闲后观察到 `no-key workers after scale down: 1`。
- no-key MPSC 验证通过：`Parallelism = 1` 且未启用动态扩容时，观察到 `no-key mpsc published count: 4`、`no-key mpsc max concurrency observed: 1`。
- xUnit 单元测试通过：12 个用例，覆盖 keyed dispatcher 的顺序、跨 key 并发、错误处理、排空和动态扩缩容，以及 no-key dispatcher 的转换发布、订阅者隔离、动态扩缩容和单 worker 模式。
