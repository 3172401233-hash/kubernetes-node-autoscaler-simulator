# kubernetes-node-autoscaler-simulator

纯 Python 的集群节点扩缩容**模拟器**,用于容量规划与扩缩容策略验证。

> **重要说明:本工具只做容量规划。**
> 它不连接 Kubernetes、不调用任何集群 API;节点加入、Pod 装箱、
> 抢占回收与缩容全部在本地按时间片模拟,输出(成本、节点数、
> pending 诊断)仅为容量规划与策略评估的参考。

要求 Python 3.8.2+(仅标准库,无第三方依赖)。

## 功能

- 按时间片模拟节点状态机:`creating -> starting -> ready / failed`,
  支持模板的创建延迟与启动(加入集群)延迟。
- **启动延迟内不重复扩容**:在途节点容量可被 pending Pod 预留,
  只有预留后仍装不下的 Pod 才触发新节点创建。
- 装箱约束:CPU、内存、节点选择器(nodeSelector)、Pod 反亲和
  (双向检查);无法装箱时明确诊断缺少的是
  `cpu` / `memory` / `node_selector` / `anti_affinity`。
- 扩容按模板成本升序选择,支持按量与抢占(spot)实例、模板节点数上限。
- 缩容安全窗口:利用率需连续低于阈值若干时间片才回收,并排除
  - 含短期任务(即将完成)的节点;
  - 含最近变动(刚调度/迁移)Pod 的节点;
  - 驱逐后 Pod 无法全部重新调度的节点(先模拟迁移再执行)。
- 处理抢占实例回收(计划事件 + 每时间片概率)、节点加入失败
  (事件注入 + 概率)与需求突降(Pod 到期/移除后自动整合回收)。
- 成本记账:节点自创建起按时间片计费,输出总成本与扩缩容统计。

## 快速开始

```python
from autoscaler_sim import NodeTemplate, PodSpec, SimConfig, Simulator

spot = NodeTemplate("spot-4c8g", cpu=4, memory=8192, cost=0.3,
                    spot=True, create_delay=1, start_delay=2)
od = NodeTemplate("od-4c8g", cpu=4, memory=8192, cost=1.0,
                  create_delay=1, start_delay=2)

pods = [PodSpec("web-0", cpu=1, memory=1024,
                labels={"app": "web"}, anti_affinity={"app": "web"}),
        PodSpec("batch", cpu=2, memory=4096, duration=12)]

events = [
    {"tick": 15, "type": "reclaim_spot"},      # 计划内抢占回收
    {"tick": 20, "type": "add_pods", "pods": [PodSpec("web-1", cpu=1, memory=1024)]},
    {"tick": 25, "type": "remove_pod", "name": "web-1"},
    {"tick": 3, "type": "fail_next_join"},     # 注入一次节点加入失败
]

sim = Simulator([spot, od], pods=pods, events=events,
                config=SimConfig(max_ticks=60, scale_down_window=5))
result = sim.run()
print(result.total_cost, result.scale_ups, result.scale_downs)
print(result.unschedulable)   # 每个 pending Pod 的缺失约束诊断
for line in result.events:    # 完整时间线
    print(line)
```

## 输入模型

- `PodSpec`:cpu(核)、memory(MB)、labels、node_selector、
  anti_affinity(不得与携带这些标签的 Pod 同节点)、duration
  (运行时间片数,`None` 为长期)、short_task(短期任务标记)。
- `NodeTemplate`:cpu、memory、cost(每时间片成本)、spot、
  create_delay、start_delay、labels、max_count。
- `SimConfig`:max_ticks、scale_down_utilization、scale_down_window、
  recent_change_window、short_task_remaining、
  spot_reclaim_probability、join_failure_probability、seed。

## 输出(SimResult)

total_cost、scale_ups / scale_downs / preemptions / join_failures 计数、
pending_pods、unschedulable(每个 pending Pod 的缺失约束列表)、
events(带时间戳的完整事件流)、最终存活节点列表。

## 测试

```bash
python -m unittest
```

覆盖:延迟扩容去重、资源形状不匹配诊断、反亲和分散与永久 pending、
缩容防抖(安全窗口 / 最近变动 / 短期任务 / 不可重调度)、
需求突降后整合、抢占回收重调度、节点加入失败重试。
