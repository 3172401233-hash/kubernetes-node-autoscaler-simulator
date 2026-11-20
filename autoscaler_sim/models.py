"""数据模型:Pod 规格、节点模板、运行时 Pod 与节点实例。"""
from dataclasses import dataclass, field
from typing import Dict, List, Optional


@dataclass
class PodSpec:
    """待调度 Pod 的静态描述。

    cpu: 核数;memory: MB;duration: 运行时间片数,None 表示长期运行。
    anti_affinity: 不得与携带这些标签的 Pod 同节点(podAntiAffinity)。
    short_task: 标记为短期任务,缩容时跳过所在节点。
    """

    name: str
    cpu: float
    memory: float
    labels: Dict[str, str] = field(default_factory=dict)
    node_selector: Dict[str, str] = field(default_factory=dict)
    anti_affinity: Dict[str, str] = field(default_factory=dict)
    duration: Optional[int] = None
    short_task: bool = False


@dataclass
class NodeTemplate:
    """节点模板(类似节点池/实例类型)。

    cost: 每时间片成本;spot: 是否抢占式实例;
    create_delay/start_delay: 创建与启动(加入集群)延迟,单位时间片;
    max_count: 该模板节点数上限,None 表示不限。
    """

    name: str
    cpu: float
    memory: float
    cost: float
    spot: bool = False
    create_delay: int = 0
    start_delay: int = 0
    labels: Dict[str, str] = field(default_factory=dict)
    max_count: Optional[int] = None


class Pod:
    """运行时 Pod:跟踪调度位置、最近变动时间与剩余运行时间。"""

    def __init__(self, spec, created_tick):
        self.spec = spec
        self.created_tick = created_tick
        self.last_moved_tick = created_tick
        self.remaining = spec.duration
        self.node = None
        self.reserved_node = None

    @property
    def name(self):
        return self.spec.name

    def __repr__(self):
        return "Pod(%s)" % self.spec.name


class Node:
    """节点实例。状态机:creating -> starting -> ready / failed。"""

    CREATING = "creating"
    STARTING = "starting"
    READY = "ready"
    FAILED = "failed"

    def __init__(self, node_id, template, created_tick):
        self.id = node_id
        self.template = template
        self.created_tick = created_tick
        self.start_tick = None
        self.ready_tick = None
        self.state = self.CREATING
        self.pods = []        # 已调度到本节点的 Pod
        self.reserved = []    # 在途期间被预留容量的 Pod(尚未真正调度)
        self.cost_accrued = 0.0

    @property
    def name(self):
        return self.template.name

    def __repr__(self):
        return "Node(%s/%s:%s)" % (self.id, self.template.name, self.state)
