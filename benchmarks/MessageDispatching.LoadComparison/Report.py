"""Build the comparison report and an offline, interactive time-series viewer."""
import argparse
import csv
import html
import json
from pathlib import Path


def read_csv(path):
    with path.open(encoding="utf-8-sig", newline="") as source:
        return list(csv.DictReader(source))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("directory", nargs="?", type=Path, default=Path(__file__).parent / "results")
    directory = parser.parse_args().directory
    metadata = json.loads((directory / "metadata.json").read_text(encoding="utf-8-sig"))
    summary = read_csv(directory / "summary.csv")
    runs = read_csv(directory / "runs.csv")
    scenarios = list(dict.fromkeys(row["Scenario"] for row in summary))
    cells = []
    lines = [
        "# 新旧扩容算法实测对比", "",
        f"旧版：`{metadata['Baseline']}`；新版：`{metadata['Branch']}` 的工作树。",
        f"UTC 时间：{metadata['DateUtc']}。每类场景新旧各 {metadata['Repeats']} 轮，"
        f"投递期 {metadata['DurationSeconds']} 秒。", "",
        f"环境：{metadata['LogicalProcessors']} 个可见逻辑 CPU，.NET 8 Release。"
        f"固定 CPU 工作量：{metadata['WorkPasses']} passes/消息；共享校准容量："
        f"{metadata['CalibratedCapacityPerSecond']:.0f} 条/秒。", "",
        "以下均为各指标在重复运行中的中位数，箭头表示旧版 → 新版。", "",
        "| 场景 | 吞吐（条/秒） | P95 入队等待（ms） | worker 秒 | 投递末段平均 worker |",
        "| --- | ---: | ---: | ---: | ---: |",
    ]
    for scenario in scenarios:
        versions = {row["Version"]: row for row in summary if row["Scenario"] == scenario}
        old, new = versions["old"], versions["new"]
        values = [scenario]
        for metric in ("Throughput", "P95WaitMs", "WorkerSeconds", "LateAverageWorkers"):
            values.append(f"{float(old[metric]):.2f} → {float(new[metric]):.2f}")
        lines.append("| " + " | ".join(values) + " |")
        cells.append("<tr>" + "".join(f"<td>{html.escape(value)}</td>" for value in values) + "</tr>")

    valid = all(row["Valid"].lower() == "true" and int(row["OrderingViolations"]) == 0 and
                int(row["ConcurrencyViolations"]) == 0 and int(row["Errors"]) == 0 for row in runs)
    if not valid:
        raise SystemExit("Cannot publish a report with failed correctness checks")
    for scenario in scenarios:
        for repeat in range(1, metadata["Repeats"] + 1):
            pair = [row for row in runs if row["Scenario"] == scenario and int(row["Repeat"]) == repeat]
            if len(pair) != 2 or len({(row["Enqueued"], row["Checksum"]) for row in pair}) != 1:
                raise SystemExit("Old/new replay mismatch")
    lines.extend([
        "", f"全部 {len(runs)} 次运行通过消息数、顺序、同 key 并发、异常和 worker 上限校验；"
        "每对新旧版本处理相同消息数且最终工作校验和一致。", "",
        "[交互时间曲线](comparison.html) · [全部轮次](runs.csv) · [聚合数据](summary.csv) · "
        "[环境与参数](metadata.json) · [方法及指标定义](../README.md)", "",
        "吞吐包含停止投递后的排空时间。跟得上固定到达率时，吞吐主要由输入决定，"
        "应结合等待延迟和 worker 秒判断。worker 秒是逻辑容量积分，不是 CPU 时间；"
        "消息等待包含同 key 串行等待，与控制器内部的 ready-key 等待不同。", "",
        "结果仅描述这次机器和负载；宿主竞争、实际 CPU 预算及消息工作量变化均可能改变结果。"
        "完整轮次保留于 JSON/CSV，不能由中位数推断统计显著性或通用性能保证。", "",
    ])
    (directory / "REPORT.md").write_text("\n".join(lines), encoding="utf-8")

    series = {}
    for scenario in scenarios:
        series[scenario] = {}
        for repeat in range(1, metadata["Repeats"] + 1):
            series[scenario][str(repeat)] = {
                version: [{key: float(value) for key, value in row.items()}
                          for row in read_csv(directory / f"{scenario}-{version}-{repeat}.csv")]
                for version in ("old", "new")
            }
    page = PAGE.replace("__ROWS__", "\n".join(cells))
    page = page.replace("__METADATA__", html.escape(json.dumps(metadata, ensure_ascii=False, indent=2)))
    page = page.replace("__DURATION__", json.dumps(metadata["DurationSeconds"]))
    page = page.replace("__DATA__", json.dumps(series, separators=(",", ":")).replace("</", "<\\/"))
    (directory / "comparison.html").write_text(page, encoding="utf-8")
    print(directory / "REPORT.md")
    print(directory / "comparison.html")


PAGE = """<!doctype html>
<html lang="zh-CN"><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Keyed dispatcher · 新旧扩容对比</title>
<style>
body{font:15px system-ui,sans-serif;margin:32px auto;max-width:1200px;padding:0 20px;color:#172b40;background:#f4f7fa}
h1{font-size:26px}p{line-height:1.7;color:#40576b}table{border-collapse:collapse;width:100%;background:white}
td,th{padding:12px;text-align:left;border-bottom:1px solid #dce5ed;font-variant-numeric:tabular-nums}
.scroll{overflow:auto}.controls{display:flex;gap:24px;align-items:center;margin:24px 0;flex-wrap:wrap}
select{font:inherit;padding:6px 12px}canvas{width:100%;height:260px;display:block;background:white;border-radius:8px}
.chart{margin:18px 0}.legend{display:flex;gap:22px;flex-wrap:wrap}.old{color:#a64b19}.new{color:#056eab}
pre{overflow:auto;background:white;padding:16px;font-size:12px}label{display:flex;gap:10px;align-items:center}
</style>
<h1>Keyed dispatcher · 新旧扩容对比</h1>
<p>相同到达轨迹、固定 CPU 工作量、独立进程运行。表格显示重复运行的中位数，旧版 → 新版。
吞吐包含排空时间；worker 秒是逻辑容量积分。选择场景和轮次查看原始时间曲线。</p>
<div class="scroll"><table><thead><tr><th>场景</th><th>条/秒</th><th>P95 等待 ms</th><th>worker 秒</th><th>末段 worker</th></tr></thead>
<tbody>__ROWS__</tbody></table></div>
<div class="controls"><label>场景 <select id="scenario"></select></label><label>重复轮次 <select id="repeat"></select></label></div>
<div class="legend"><span class="old">━ 旧版</span><span class="new">━ 新版</span><span>虚线：忙碌 worker；实线：全部 worker</span></div>
<div class="chart"><h3>Worker 随负载变化</h3><canvas id="workers"></canvas></div>
<div class="chart"><h3>积压消息数</h3><canvas id="pending"></canvas></div>
<div class="chart"><h3>完成速率（约 0.5 秒窗口，条/秒）</h3><canvas id="throughput"></canvas></div>
<p>横轴为从开始投递起的秒数，包含排空。消息等待包含同 key 串行等待。
结果受宿主竞争、实际 CPU 预算、工作量构成和调度影响；保留全部轮次，不作通用性能保证。</p>
<details><summary>环境与参数</summary><pre>__METADATA__</pre></details>
<script>
const data=__DATA__;
const inputEnd=__DURATION__;
const scenario=document.getElementById('scenario'), repeat=document.getElementById('repeat');
for(const name of Object.keys(data)) scenario.add(new Option(name,name));
function populate(){repeat.replaceChildren();for(const id of Object.keys(data[scenario.value]))repeat.add(new Option(id,id));render()}
const colors={old:'#a64b19',new:'#056eab'};
function rate(rows){let j=0;return rows.map((row,i)=>{while(j<i-1&&row.seconds-rows[j+1].seconds>=.5)j++;
 return {...row,rate:i===j?0:(row.completed-rows[j].completed)/Math.max(.001,row.seconds-rows[j].seconds)}})}
function plot(id,series){const canvas=document.getElementById(id),ctx=canvas.getContext('2d');
 const w=canvas.clientWidth,h=260,dpr=window.devicePixelRatio||1;canvas.width=w*dpr;canvas.height=h*dpr;ctx.scale(dpr,dpr);
 const left=64,right=20,top=16,bottom=34,xmax=Math.max(1,...series.flatMap(s=>s.rows.map(r=>r.seconds)));
 const ymax=Math.max(1,...series.flatMap(s=>s.rows.map(r=>r[s.key])))*1.08;
 const x=t=>left+t/xmax*(w-left-right),y=v=>h-bottom-v/ymax*(h-top-bottom);
 ctx.font='12px system-ui';ctx.strokeStyle='#e1e8ee';ctx.fillStyle='#526879';
 for(let i=0;i<=4;i++){const v=ymax*i/4;ctx.beginPath();ctx.moveTo(left,y(v));ctx.lineTo(w-right,y(v));ctx.stroke();ctx.fillText(v.toFixed(ymax<15?1:0),5,y(v)+4)}
 for(let i=0;i<=6;i++){const t=xmax*i/6;ctx.fillText(t.toFixed(1)+'s',x(t)-10,h-10)}
 if(inputEnd<=xmax){ctx.beginPath();ctx.setLineDash([3,3]);ctx.moveTo(x(inputEnd),top);ctx.lineTo(x(inputEnd),h-bottom);
 ctx.stroke();ctx.setLineDash([]);ctx.fillText('停止投递',Math.min(w-78,x(inputEnd)+5),top+12)}
 for(const s of series){ctx.beginPath();ctx.strokeStyle=s.color;ctx.lineWidth=1.8;ctx.setLineDash(s.dashed?[5,4]:[]);
 s.rows.forEach((r,i)=>i?ctx.lineTo(x(r.seconds),y(r[s.key])):ctx.moveTo(x(r.seconds),y(r[s.key])));ctx.stroke()}
 ctx.setLineDash([]);
}
function render(){const chosen=data[scenario.value][repeat.value],versions=['old','new'];
 plot('workers',versions.flatMap(v=>[{rows:chosen[v],key:'workers',color:colors[v]},
   {rows:chosen[v],key:'busy',color:colors[v],dashed:true}]));
 plot('pending',versions.map(v=>({rows:chosen[v],key:'pending',color:colors[v]})));
 plot('throughput',versions.map(v=>({rows:rate(chosen[v]),key:'rate',color:colors[v]})));
}
scenario.onchange=populate;repeat.onchange=render;window.addEventListener('resize',render);populate();
</script></html>"""


if __name__ == "__main__":
    main()
