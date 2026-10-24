# 负载匹配扩缩容

`KeyedOrderedDispatcher` 和 `MessageDispatcher` 共用负载匹配控制器。
前者观测去重的活跃 key 和 ready-key 等待，后者观测独立消息和队列等待。
两者共用的 worker 生命周期语义不变：
正在执行的 handler 不会因为缩容被取消，同 key 始终串行，完成排空后 worker 数归零。

## 配置

```csharp
var dispatcher = new KeyedOrderedDispatcher<long, MyMessage>(new DispatcherOptions
{
    Parallelism = 1,       // 启动数量，也是缩容下限
    MaxParallelism = 32,   // 大于 Parallelism 才开启动态调整
    KeyBatchSize = 32,
    DynamicScaling = new DynamicScalingOptions
    {
        SampleInterval = TimeSpan.FromMilliseconds(200),
        TargetUtilization = 0.8,
        UtilizationWindow = TimeSpan.FromSeconds(1),
        TargetQueueWait = TimeSpan.FromMilliseconds(50),
        ScaleUpInterval = TimeSpan.FromMilliseconds(200),
        ScaleDownIdleDuration = TimeSpan.FromSeconds(5),
        ResourceParallelismLimit = 0, // 0 = Environment.ProcessorCount
        CpuUtilizationLimit = 0.9,
        CapacityMeasurementWindow = TimeSpan.FromSeconds(1),
        CapacityRetryInterval = TimeSpan.FromSeconds(10)
    }
});
```

希望低负载时只保留一两个 worker，需要显式设置 `Parallelism`。其默认值仍是
`Environment.ProcessorCount`，控制器不会突破这个配置下限。

`ResourceParallelismLimit` 是每个 dispatcher 的资源预算；默认按可见逻辑 CPU 数计算，
显式值适合进程 CPU 预算或包含同步阻塞的 handler。它与 `MaxParallelism` 取较小值，
但始终保证至少 `Parallelism` 个 worker。可见 CPU 数不能完整反映宿主竞争和所有部署配额，
生产环境应按实际预算配置。`CpuUtilizationLimit` 观测的是本进程全部线程的 CPU 时间，
除以经过时间和可见 CPU 数，并非单个 dispatcher 或整台机器的 CPU 使用率。

## 需求计算

keyed worker 在 key batch 的开始和结束记录 busy 时间，普通 worker 在每条消息的转换和
同步 subscriber fan-out 前后记录时间，采样还计入尚未返回的用户代码。
因此即使所有长任务都没有产生完成消息，也能估算需求。固定并行度路径不采集这些时间。
累计 busy 时间在 worker 退休后仍被保留，避免下一次采样出现负增量。

```text
intervalBusy = 窗口内处理占用的 worker 秒数 / 窗口秒数
alpha = 1 - exp(-窗口秒数 / UtilizationWindow 秒数)
averageBusy = averageBusy + alpha * (intervalBusy - averageBusy)
need = ceil(averageBusy / TargetUtilization)

if 最老的就绪 key 等待时间 >= TargetQueueWait:
    need = max(need, workerCount + max(1, floor(workerCount / 4)))

desired = clamp(min(need, activeKeyCount, capacityLimit), Parallelism, resourceMaximum)
```

`activeKeyCount` 在 key 从空闲变为 active 时加一，处理完且没有新消息时减一。
key 在处理和重新就绪之间交接不会重复计数，也不使用历史 key 总数推断并行需求。
因此单个热点 key 再多积压也不能触发超过一个并行需求，配置下限除外。

每个 ready token 携带重新就绪时的时间，控制器读取当前队首的等待时长。
同 key 的消息等待前序 handler 返回的时间不属于这个指标，也不会被误当成可通过增员解决的等待。

普通 dispatcher 的每条消息都能独立执行，使用尚未完成的消息数作为可利用并行度上限，
使用当前队首消息入队时间测量等待。计数采用 `long`，不会将消息积压数量窄化成 `int`。
`EffectiveMaxParallelism == 1` 仍使用单 consumer 路径，不启动控制器或采集处理时间。

扩容最多每 `ScaleUpInterval` 执行一次，每次增加最多约 25%，至少一个。
CPU 压力达到上限时暂停继续增加。worker 尚未完成创建或退休时不会叠加新调整。

缩容允许发生在部分忙碌时，不要求 ready 队列持续为空。低需求必须持续达到
`ScaleDownIdleDuration`，并采用该稳定窗口内最高的需求建议；每次最多减少约 25%，
至少一个。每次实际调整后重新建立稳定窗口，避免短暂低谷造成反复增减。

## 吞吐作为辅助容量反馈

控制器不要求每次扩容都提升吞吐。输入固定、已有 worker 能跟上时，
总吞吐受输入约束，新增余量可能只改善延迟。

只有实际/期望 worker 数收敛、存在额外独立就绪 key、占用达到目标利用率时，
才积累容量测量窗口。增员后跳过一个完整窗口预热，再将可比较窗口的吞吐与旧配置比较。
期间失去独立积压会丢弃参考；这不是逐次扩容前必须完成的探测状态机。

若吞吐收益不超过 `CapacityGainTolerance`，暂停继续增员：

- 收益为正：保留当前 worker 数作为容量上限，小幅收益也可用于匹配负载。
- 收益为零或负：将参考配置作为上限，按正常稳定窗口安全缩容。
- 每 `CapacityRetryInterval` 仅重新开放一个 worker 的空间，适应资源和负载变化。

容量反馈是观测估计，消息工作量构成、宿主竞争变化会影响比较，不能保证找到全局吞吐最优点。
短暂增长来不及形成参考窗口时，首先由显式资源上限和 CPU 压力限制增长。

## 兼容性与观测

`SampleInterval` 至少为 1ms，在启动 worker 前校验。
`ScaleDownIdleDuration` 对两种 dispatcher 都表示低需求稳定时间。

旧吞吐扩容状态机及其测试已删除。以下为有意的公开 API 变更：

| 原 API | 迁移方式 |
| --- | --- |
| `ProbeTimeout` / `ProbeWarmupSamples` / `ThroughputMeasurementSamples` | 删除配置；负载匹配不再运行逐次 probe |
| `ScaleUpCooldown` | 使用 `ScaleUpInterval` 控制扩容间隔 |
| `MinimumUsefulThroughputGain` | 改为 `CapacityGainTolerance`，仅用于辅助容量校准 |
| stats 中的 `ProbeAcceptCount` / `ProbeRejectCount` / `LastProbeGain` | 已删除，观察占用、等待和容量上限 |

三个 stats record 的位置构造函数和 `Deconstruct` 同步去掉三个 probe 参数，
直接构造或解构这些类型的调用方需要更新。吞吐速率及其平滑统计仍保留。

`KeyedDispatcherStats` 提供以下负载属性：

| 属性 | 含义 |
| --- | --- |
| `ActiveKeyCount` | 当前有处理需求的去重 key 数 |
| `AverageBusyWorkers` | 按经过时间平滑后的平均 busy worker 数 |
| `ReadyKeyWaitMilliseconds` | 最近采样时就绪队首 key 的等待时间 |
| `CapacityLimit` | 当前资源预算及容量反馈允许的上限 |
| `ProcessCpuUtilization` | 最近采样区间的进程 CPU 使用率，范围 0–1 |

`DispatcherStats` 和 observer 的 `DispatcherScalingStats` 提供相同的占用、容量、CPU 属性，
并用 `QueueWaitMilliseconds` 表示队首消息或 ready-key 等待。
吞吐和缩扩容计数继续更新，吞吐只作观测及辅助容量反馈，不再负责驱动 probe 扩容。
stats 仍是线程安全计数器的尽力快照，而不是所有字段同一瞬间的事务快照。

新旧版本的实测方法和数据见 [对比程序](../benchmarks/MessageDispatching.LoadComparison/README.md)。
