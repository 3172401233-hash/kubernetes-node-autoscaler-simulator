"""按时间片驱动的集群扩缩容模拟器。

每个时间片依次执行:
  1. 处理外部事件(新增/移除 Pod、计划内抢占回收、注入加入失败)
  2. 完成到期的 Pod(模拟需求下降/突降)
  3. 推进节点状态机(creating -> starting -> ready/failed)
  4. 抢占实例随机回收
  5. 将 pending Pod 装箱到就绪节点(best-fit)
  6. 扩容:在途节点容量预留,装不下才创建新节点(避免启动延迟内重复扩容)
  7. 缩容:低利用率持续超过安全窗口才回收,并排除短期任务、
     最近变动的 Pod 以及驱逐后无法重新调度的节点
  8. 成本记账与不可调度诊断
"""
import random
from dataclasses import dataclass, field
from typing import Dict, List, Optional

from .models import Node, NodeTemplate, Pod, PodSpec
from .scheduler import explain_unschedulable, pod_node_violations, resource_usage


@dataclass
class SimConfig:
    max_ticks: int = 100
    scale_down_utilization: float = 0.5   # 利用率低于该值计入低利用时间片
    scale_down_window: int = 5            # 需连续低利用的时间片数(安全窗口)
    recent_change_window: int = 3         # 最近变动保护窗口(时间片)
    short_task_remaining: int = 5         # 剩余运行时间不超过该值视为短期任务
    spot_reclaim_probability: float = 0.0  # 每个时间片回收就绪抢占节点的概率
    join_failure_probability: float = 0.0  # 节点加入集群失败概率
    seed: int = 1


@dataclass
class SimResult:
    total_cost: float = 0.0
    ticks: int = 0
    scale_ups: int = 0
    scale_downs: int = 0
    preemptions: int = 0
    join_failures: int = 0
    pending_pods: List[str] = field(default_factory=list)
    unschedulable: Dict[str, List[str]] = field(default_factory=dict)
    events: List[str] = field(default_factory=list)
    nodes: List[str] = field(default_factory=list)


class Simulator:
    """事件通过字典描述,例如::

        {"tick": 5, "type": "add_pods", "pods": [PodSpec(...)]}
        {"tick": 8, "type": "remove_pod", "name": "web-1"}
        {"tick": 10, "type": "reclaim_spot"}        # 回收全部就绪抢占节点
        {"tick": 3, "type": "fail_next_join"}       # 下一次节点加入失败
    """

    def __init__(self, templates, pods=None, events=None, config=None):
        self.templates = list(templates)
        self.config = config or SimConfig()
        self.events = sorted(events or [], key=lambda e: e["tick"])
        self.rng = random.Random(self.config.seed)
        self.nodes = []
        self.pending = []
        self.finished = []
        self.result = SimResult()
        self.low_util = {}
        self.now = 0
        self._node_seq = 0
        self._fail_next_join = False
        for spec in pods or []:
            self.add_pod(spec)

    # ------------------------------------------------------------------ API
    def add_pod(self, spec):
        pod = Pod(spec, self.now)
        self.pending.append(pod)
        self._log("Pod %s 到达(cpu=%g mem=%g)", spec.name, spec.cpu, spec.memory)
        return pod

    def run(self):
        for tick in range(self.config.max_ticks):
            self.now = tick
            self._process_events()
            self._complete_pods()
            self._advance_nodes()
            self._reclaim_spot_random()
            self._schedule_pending()
            self._scale_up()
            self._scale_down()
            self._account_cost()
            self._update_report()
        self.result.ticks = self.config.max_ticks
        self.result.nodes = [n.id for n in self.nodes if n.state == Node.READY]
        return self.result

    def running_pods(self):
        return [p for n in self.nodes for p in n.pods]

    # --------------------------------------------------------------- events
    def _process_events(self):
        while self.events and self.events[0]["tick"] <= self.now:
            event = self.events.pop(0)
            kind = event["type"]
            if kind == "add_pods":
                for spec in event["pods"]:
                    self.add_pod(spec)
            elif kind == "remove_pod":
                self._remove_pod(event["name"])
            elif kind == "reclaim_spot":
                for node in list(self.nodes):
                    if node.state == Node.READY and node.template.spot:
                        self._reclaim_node(node, "计划内抢占回收")
            elif kind == "fail_next_join":
                self._fail_next_join = True
            else:
                raise ValueError("未知事件类型: %r" % kind)

    def _remove_pod(self, name):
        for pod in list(self.pending):
            if pod.name == name:
                self.pending.remove(pod)
                self._log("Pod %s 被移除(pending)", name)
                return
        for node in self.nodes:
            for pod in list(node.pods):
                if pod.name == name:
                    node.pods.remove(pod)
                    self._log("Pod %s 被移除(节点 %s)", name, node.id)
                    return

    def _complete_pods(self):
        for node in self.nodes:
            if node.state != Node.READY:
                continue
            for pod in list(node.pods):
                if pod.remaining is None:
                    continue
                pod.remaining -= 1
                if pod.remaining <= 0:
                    node.pods.remove(pod)
                    self.finished.append(pod)
                    self._log("Pod %s 运行完成,离开节点 %s", pod.name, node.id)

    # ---------------------------------------------------------------- nodes
    def _advance_nodes(self):
        for node in list(self.nodes):
            tmpl = node.template
            if node.state == Node.CREATING and self.now - node.created_tick >= tmpl.create_delay:
                node.state = Node.STARTING
                node.start_tick = self.now
                self._log("节点 %s(%s)创建完成,进入启动阶段", node.id, tmpl.name)
            if node.state == Node.STARTING and self.now - node.start_tick >= tmpl.start_delay:
                if self._should_fail_join():
                    self._fail_node(node)
                else:
                    self._ready_node(node)

    def _should_fail_join(self):
        if self._fail_next_join:
            self._fail_next_join = False
            return True
        return self.rng.random() < self.config.join_failure_probability

    def _ready_node(self, node):
        node.state = Node.READY
        node.ready_tick = self.now
        self.low_util[node.id] = 0
        for pod in node.reserved:
            node.pods.append(pod)
            pod.node = node
            pod.reserved_node = None
            pod.last_moved_tick = self.now
            self.pending.remove(pod)
            self._log("Pod %s 调度到节点 %s", pod.name, node.id)
        node.reserved = []
        self._log("节点 %s(%s)就绪并加入集群", node.id, node.template.name)

    def _fail_node(self, node):
        node.state = Node.FAILED
        for pod in node.reserved:
            pod.reserved_node = None
        node.reserved = []
        self.nodes.remove(node)
        self.result.join_failures += 1
        self._log("节点 %s(%s)加入集群失败,已放弃", node.id, node.template.name)

    def _reclaim_spot_random(self):
        prob = self.config.spot_reclaim_probability
        if prob <= 0:
            return
        for node in list(self.nodes):
            if node.state == Node.READY and node.template.spot and self.rng.random() < prob:
                self._reclaim_node(node, "抢占实例被云厂商回收")

    def _reclaim_node(self, node, reason):
        for pod in node.pods:
            pod.node = None
            pod.reserved_node = None
            self.pending.append(pod)
            self._log("Pod %s 被驱逐,重新进入 pending", pod.name)
        for pod in node.reserved:
            pod.reserved_node = None
        node.pods = []
        node.reserved = []
        self.nodes.remove(node)
        self.result.preemptions += 1
        self._log("节点 %s(%s):%s", node.id, node.template.name, reason)

    # ------------------------------------------------------------ scheduling
    def _schedule_pending(self):
        ready = [n for n in self.nodes if n.state == Node.READY]
        for pod in list(self.pending):
            if pod.reserved_node is not None:
                continue
            best = None
            best_left = None
            for node in ready:
                if pod_node_violations(pod.spec, node, node.pods):
                    continue
                used_cpu, used_mem = resource_usage(node.pods)
                left = (node.template.cpu - used_cpu - pod.spec.cpu,
                        node.template.memory - used_mem - pod.spec.memory)
                # 分散策略:优先放到剩余容量最多的节点
                if best is None or left > best_left:
                    best, best_left = node, left
            if best is not None:
                best.pods.append(pod)
                pod.node = best
                pod.last_moved_tick = self.now
                self.pending.remove(pod)
                self._log("Pod %s 调度到节点 %s", pod.name, best.id)

    # -------------------------------------------------------------- scale up
    def _scale_up(self):
        inflight = [n for n in self.nodes if n.state in (Node.CREATING, Node.STARTING)]
        for pod in list(self.pending):
            if pod.reserved_node is not None:
                continue
            target = None
            for node in inflight:
                # 在途节点容量可被预留,从而避免启动延迟内重复扩容
                if not pod_node_violations(pod.spec, node, node.pods + node.reserved):
                    target = node
                    break
            if target is None:
                target = self._create_node_for(pod.spec)
                if target is None:
                    continue  # 没有任何模板装得下,保持 pending
                inflight.append(target)
            target.reserved.append(pod)
            pod.reserved_node = target
            self._log("Pod %s 预留在途节点 %s 的容量", pod.name, target.id)

    def _create_node_for(self, spec):
        for tmpl in sorted(self.templates, key=lambda t: t.cost):
            if not self._template_available(tmpl):
                continue
            fresh = Node("candidate", tmpl, self.now)
            if pod_node_violations(spec, fresh, []):
                continue
            self._node_seq += 1
            node = Node("node-%d" % self._node_seq, tmpl, self.now)
            self.nodes.append(node)
            self.result.scale_ups += 1
            self._log("扩容:创建节点 %s(%s, spot=%s)", node.id, tmpl.name, tmpl.spot)
            return node
        return None

    def _template_available(self, tmpl):
        if tmpl.max_count is None:
            return True
        count = sum(1 for n in self.nodes if n.template is tmpl)
        return count < tmpl.max_count

    # ------------------------------------------------------------ scale down
    def _scale_down(self):
        cfg = self.config
        if self._has_schedulable_pending():
            return  # 还有能调度的 pending Pod,缩容可能加剧抖动
        ready = [n for n in self.nodes if n.state == Node.READY]
        candidates = []
        for node in ready:
            if self._utilization(node) < cfg.scale_down_utilization:
                self.low_util[node.id] = self.low_util.get(node.id, 0) + 1
            else:
                self.low_util[node.id] = 0
            if self.low_util[node.id] < cfg.scale_down_window:
                continue
            if self._has_short_task(node):
                self._log("节点 %s 含短期任务,跳过缩容", node.id)
                continue
            if self._has_recent_change(node):
                self._log("节点 %s 含最近变动的 Pod,跳过缩容", node.id)
                continue
            candidates.append(node)
        removed = set()
        for node in sorted(candidates, key=self._utilization):
            targets = [n for n in ready if n is not node and n not in removed]
            plan = self._plan_drain(node, targets)
            if plan is None:
                self._log("节点 %s 上的 Pod 无法全部重新调度,跳过缩容", node.id)
                continue
            for pod, target in plan:
                node.pods.remove(pod)
                target.pods.append(pod)
                pod.node = target
                pod.last_moved_tick = self.now
                self._log("Pod %s 迁移到节点 %s", pod.name, target.id)
            self.nodes.remove(node)
            removed.add(node)
            self.result.scale_downs += 1
            self._log("缩容:回收节点 %s(%s)", node.id, node.template.name)

    def _has_schedulable_pending(self):
        for pod in self.pending:
            if pod.reserved_node is not None:
                continue
            for tmpl in self.templates:
                if not self._template_available(tmpl):
                    continue
                fresh = Node("candidate", tmpl, self.now)
                if not pod_node_violations(pod.spec, fresh, []):
                    return True
        return False

    def _has_short_task(self, node):
        limit = self.config.short_task_remaining
        for pod in node.pods:
            if pod.spec.short_task:
                return True
            if pod.remaining is not None and pod.remaining <= limit:
                return True
        return False

    def _has_recent_change(self, node):
        window = self.config.recent_change_window
        return any(self.now - p.last_moved_tick < window for p in node.pods)

    def _plan_drain(self, node, targets):
        """模拟把 node 上所有 Pod 搬到 targets;全部放得下才返回迁移计划。"""
        extra = {t.id: [] for t in targets}
        placement = {}
        for pod in sorted(node.pods, key=lambda p: p.spec.cpu, reverse=True):
            for target in targets:
                if not pod_node_violations(pod.spec, target, target.pods + extra[target.id]):
                    extra[target.id].append(pod)
                    placement[pod.name] = (pod, target)
                    break
            else:
                return None
        return [placement[p.name] for p in node.pods]

    @staticmethod
    def _utilization(node):
        if not node.pods:
            return 0.0
        used_cpu, used_mem = resource_usage(node.pods)
        return max(used_cpu / node.template.cpu, used_mem / node.template.memory)

    # --------------------------------------------------------------- reporting
    def _account_cost(self):
        for node in self.nodes:
            if node.state in (Node.CREATING, Node.STARTING, Node.READY):
                node.cost_accrued += node.template.cost
                self.result.total_cost += node.template.cost

    def _update_report(self):
        ready = [n for n in self.nodes if n.state == Node.READY]
        pending = [p for p in self.pending if p.reserved_node is None]
        self.result.pending_pods = sorted(p.name for p in pending)
        report = {}
        for pod in pending:
            options = [(n.id, pod_node_violations(pod.spec, n, n.pods)) for n in ready]
            for tmpl in self.templates:
                if not self._template_available(tmpl):
                    continue
                fresh = Node("candidate", tmpl, self.now)
                options.append((tmpl.name, pod_node_violations(pod.spec, fresh, [])))
            common = explain_unschedulable(pod.spec, options)
            report[pod.name] = sorted(common) if common else ["fragmented_capacity"]
        self.result.unschedulable = report

    def _log(self, fmt, *args):
        self.result.events.append("[t=%d] %s" % (self.now, fmt % args))
