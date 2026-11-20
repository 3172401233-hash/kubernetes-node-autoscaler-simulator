"""装箱检查与不可调度诊断。

诊断目标:当 Pod 无法装箱时,明确指出缺少的是
CPU、内存、节点选择器还是反亲和约束。
"""

CPU = "cpu"
MEMORY = "memory"
NODE_SELECTOR = "node_selector"
ANTI_AFFINITY = "anti_affinity"

_EPS = 1e-9


def labels_match(selector, labels):
    return all(labels.get(k) == v for k, v in selector.items())


def resource_usage(pods):
    cpu = sum(p.spec.cpu for p in pods)
    memory = sum(p.spec.memory for p in pods)
    return cpu, memory


def pod_node_violations(pod_spec, node, pods_on_node):
    """返回 Pod 放到 node 上违反的约束列表;空列表表示可以装箱。"""
    violations = []
    if not labels_match(pod_spec.node_selector, node.template.labels):
        violations.append(NODE_SELECTOR)
    used_cpu, used_mem = resource_usage(pods_on_node)
    if used_cpu + pod_spec.cpu > node.template.cpu + _EPS:
        violations.append(CPU)
    if used_mem + pod_spec.memory > node.template.memory + _EPS:
        violations.append(MEMORY)
    for other in pods_on_node:
        if pod_spec.anti_affinity and labels_match(pod_spec.anti_affinity, other.spec.labels):
            violations.append(ANTI_AFFINITY)
            break
    for other in pods_on_node:
        if other.spec.anti_affinity and labels_match(other.spec.anti_affinity, pod_spec.labels):
            violations.append(ANTI_AFFINITY)
            break
    return violations


def explain_unschedulable(pod_spec, options):
    """诊断 Pod 不可装箱的原因。

    options: [(描述, 该方案违反的约束列表), ...],通常包含所有就绪节点
    与所有可用节点模板(空节点)。返回所有方案共同违反的约束集合:
    若某约束(如 cpu)在每个方案上都被违反,则它就是缺失的资源。
    返回空集合表示单一节点都装得下、但组合后放不下(容量碎片化)。
    """
    if not options:
        return {"no_node_or_template"}
    common = None
    for _desc, violations in options:
        current = set(violations)
        common = current if common is None else (common & current)
    return common or set()
