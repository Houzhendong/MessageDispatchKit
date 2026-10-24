# KeyedOrderedDispatcher 负载匹配扩缩容

`KeyedOrderedDispatcher` 使用利用率和可执行 key 的等待时间计算 worker 需求。
`MessageDispatcher` 继续使用原有吞吐探测策略。两者共用的 worker 生命周期语义不变：
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

`ResourceParallelismLimit` 是 keyed dispatcher 的资源预算；默认按可见逻辑 CPU 数计算，
显式值适合进程 CPU 预算或包含同步阻塞的 handler。它与 `MaxParallelism` 取较小值，
但始终保证至少 `Parallelism` 个 worker。可见 CPU 数不能完整反映宿主竞争和所有部署配额，
生产环境应按实际预算配置。`CpuUtilizationLimit` 观测的是本进程全部线程的 CPU 时间，
除以经过时间和可见 CPU 数，并非单个 dispatcher 或整台机器的 CPU 使用率。

## 需求计算

每个 worker 在 key batch 的开始和结束记录 busy 时间，采样还计入尚未返回的 handler。
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

若吞吐收益不超过 `MinimumUsefulThroughputGain`，暂停继续增员：

- 收益为正：保留当前 worker 数作为容量上限，小幅收益也可用于匹配负载。
- 收益为零或负：将参考配置作为上限，按正常稳定窗口安全缩容。
- 每 `CapacityRetryInterval` 仅重新开放一个 worker 的空间，适应资源和负载变化。

容量反馈是观测估计，消息工作量构成、宿主竞争变化会影响比较，不能保证找到全局吞吐最优点。
短暂增长来不及形成参考窗口时，首先由显式资源上限和 CPU 压力限制增长。

## 兼容性与观测

`SampleInterval` 至少为 1ms，在启动 worker 前校验。
`ScaleDownIdleDuration` 对 keyed 表示低需求稳定时间，对普通 dispatcher 仍表示空闲缩容时间。
`ProbeTimeout`、`ProbeWarmupSamples`、`ThroughputMeasurementSamples`、`ScaleUpCooldown`
继续配置普通 dispatcher；keyed 使用新的时间窗口和扩容间隔。

原 stats 的位置参数保持兼容，`KeyedDispatcherStats` 增加属性：

| 属性 | 含义 |
| --- | --- |
| `ActiveKeyCount` | 当前有处理需求的去重 key 数 |
| `AverageBusyWorkers` | 按经过时间平滑后的平均 busy worker 数 |
| `ReadyKeyWaitMilliseconds` | 最近采样时就绪队首 key 的等待时间 |
| `CapacityLimit` | 当前资源预算及容量反馈允许的上限 |
| `ProcessCpuUtilization` | 最近采样区间的进程 CPU 使用率，范围 0–1 |

原吞吐和缩扩容计数继续更新。keyed 不再运行旧 probe，旧 `ProbeAcceptCount` /
`ProbeRejectCount` 为零、`LastProbeGain` 为 null；普通 dispatcher 的 probe 统计不变。
stats 仍是线程安全计数器的尽力快照，而不是所有字段同一瞬间的事务快照。

新旧版本的实测方法和数据见 [对比程序](../benchmarks/MessageDispatching.LoadComparison/README.md)。
