# 新旧 keyed 扩容策略对比

旧版固定到提交 `9d1663c`。脚本从 git 读取该提交的 library 源码，在 `obj/` 中构建，
不切换分支、不修改工作树或索引。相同的 harness 分别链接旧版和当前 Release DLL，
运行在独立进程中，不通过改写当前算法来模拟旧行为。

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File `
  benchmarks/MessageDispatching.LoadComparison/Compare.ps1 `
  -Repeats 3 -DurationSeconds 12

python benchmarks/MessageDispatching.LoadComparison/Report.py
```

`Compare.ps1` 可传 `-Baseline`、`-OutputDirectory`、`-Scenarios`。
初次生成无第三方包的 console 项目需要已安装 .NET 8 target packs；使用当前机器上的 .NET 8 runtime。
报告生成只使用 Python 标准库，HTML 图表离线工作。

## 工作负载与公平性

- handler 对相同 8KB buffer 做固定次数的哈希 CPU 运算，不使用 sleep 模拟处理。
- 一次校准确定每条消息的固定运算次数，并测量持续处理能力。所有版本和重复共享该校准。
  校准先运行 3 秒，再测 2 秒，避免按逻辑 CPU 数假设线性性能。
- 所有到达轨迹预先由场景、校准容量和时长确定。生产者按轨迹追赶，不会因 worker 数变化而改变消息总数。
- 新旧每对运行的消息数和最终工作校验和必须一致；校验顺序、同 key 并发、异常和 worker 上限。
- 共用 `Parallelism=1`、`MaxParallelism=min(8, max(2, CPU数))`、`KeyBatchSize=8`、
  `SampleInterval=100ms`、缩容稳定时间 1 秒。旧版 probe cooldown 为 200ms，其余新旧配置采用各自默认值。
- 每个版本独立预热，串行运行。重复之间交替先运行新旧版本，减少顺序和温度偏差。
  比较期间不应同时运行其他 CPU 基准或测试。

| 场景 | 到达轨迹（C 为校准处理能力） |
| --- | --- |
| `burst` | 前 20% 时间 0.02C，中间 45% 时间 0.8C，最后 35% 时间 0.02C |
| `partial-load` | 前 40% 时间 0.8C，后 60% 时间 0.25C |
| `one-hot-key` | 单 key，持续 0.8C |
| `heterogeneous` | 32 keys，工作量 1/2/4/9 倍轮换，到达率 0.2C |
| `cpu-saturation` | 32 keys，持续 1.5C，停止输入后继续排空 |

## 输出解释

`results/` 保存每轮 JSON、50ms 目标周期采样的 CSV、聚合 CSV、机器元数据和报告。
元数据还保存实际参与测量的新旧 DLL 及 harness 源码的 SHA256。
Windows timer 和调度会使实际采样周期不同，吞吐和占用均按真实经过时间计算。
`Report.py` 生成中位数表和可选择场景/重复轮次的 worker、积压和吞吐时间曲线。

- `Throughput`：消息总数 / 从开始投递到排空结束的总时间，**包含排空时间**。
  能跟上固定输入的场景，该值主要等于输入速率，应该同时看延迟和资源占用。
- `P95WaitMs`：从实际入队到 handler 开始的消息等待时间，**包含同 key 内的串行等待**。
  它不同于新控制器内部仅测量 ready token 的可执行等待时间。
- `WorkerSeconds`：对观察到的 worker 数变化事件做近似时间积分，表示保留的逻辑 worker 容量，
  **不是 CPU 秒，也不表示相同数量的专用 OS 线程**。
- `ProcessCpuSeconds`：进程 CPU 时间，包含生产者、harness 和 dispatcher。
- `LateAverageWorkers`：投递期最后 25% 时间中采样的平均 worker 数，观察负载回落后的匹配情况。
- `DrainSeconds`：停止投递到排空完成的耗时。

一次机器上的中位数只能描述该次环境。异构工作量、宿主竞争和可见 CPU 数与实际预算的差异
都可能影响结果；报告保留全部轮次及原始曲线，不能把某个场景的改善当作通用性能保证。
