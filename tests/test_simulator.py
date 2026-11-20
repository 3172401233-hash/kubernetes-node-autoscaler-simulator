import unittest

from autoscaler_sim import NodeTemplate, PodSpec, SimConfig, Simulator


def make_template(**kw):
    defaults = dict(name="standard", cpu=4, memory=8192, cost=1.0)
    defaults.update(kw)
    return NodeTemplate(**defaults)


def make_pod(name, cpu=1, memory=512, **kw):
    return PodSpec(name=name, cpu=cpu, memory=memory, **kw)


def events_at(result, keyword):
    return [e for e in result.events if keyword in e]


class DelayedScaleUpTest(unittest.TestCase):
    """启动延迟内不得重复扩容。"""

    def test_startup_delay_avoids_duplicate_scale_up(self):
        tmpl = make_template(create_delay=2, start_delay=3)
        sim = Simulator([tmpl], pods=[make_pod("d", cpu=2)],
                        config=SimConfig(max_ticks=20))
        result = sim.run()
        self.assertEqual(1, result.scale_ups)
        self.assertEqual([], result.pending_pods)
        ready_events = events_at(result, "就绪并加入集群")
        self.assertEqual(1, len(ready_events))
        # create_delay=2 + start_delay=3 -> 第 5 个时间片就绪
        self.assertIn("[t=5]", ready_events[0])
        self.assertEqual(1, len(sim.nodes))


class ResourceShapeMismatchTest(unittest.TestCase):
    """资源形状不匹配时要指出缺 CPU、内存还是节点选择器。"""

    def test_unschedulable_diagnosis(self):
        tmpl = make_template(cpu=4, memory=8192)
        pods = [
            make_pod("hungry-cpu", cpu=8),
            make_pod("hungry-mem", memory=99999),
            make_pod("needs-ssd", node_selector={"disk": "ssd"}),
        ]
        sim = Simulator([tmpl], pods=pods, config=SimConfig(max_ticks=10))
        result = sim.run()
        self.assertEqual(0, result.scale_ups)
        self.assertEqual(["cpu"], result.unschedulable["hungry-cpu"])
        self.assertEqual(["memory"], result.unschedulable["hungry-mem"])
        self.assertEqual(["node_selector"], result.unschedulable["needs-ssd"])
        self.assertEqual({"hungry-cpu", "hungry-mem", "needs-ssd"},
                         set(result.pending_pods))


class AntiAffinityTest(unittest.TestCase):
    """反亲和 Pod 必须分散到不同节点,装不下时报告反亲和约束。"""

    def test_anti_affinity_spread_and_pending(self):
        tmpl = make_template(cpu=4, memory=8192, max_count=2)
        pods = [
            make_pod(name, cpu=1, labels={"app": "web"},
                     anti_affinity={"app": "web"})
            for name in ("w1", "w2", "w3")
        ]
        sim = Simulator([tmpl], pods=pods, config=SimConfig(max_ticks=15))
        result = sim.run()
        self.assertEqual(2, result.scale_ups)
        placement = {}
        for node in sim.nodes:
            for pod in node.pods:
                placement[pod.name] = node.id
        self.assertEqual({"w1", "w2"}, set(placement))
        self.assertNotEqual(placement["w1"], placement["w2"])
        # 模板已达上限,w3 永久 pending,原因是反亲和
        self.assertEqual(["w3"], result.pending_pods)
        self.assertEqual(["anti_affinity"], result.unschedulable["w3"])


class ScaleDownTest(unittest.TestCase):
    """缩容:需求突降后整合,且需越过安全窗口。"""

    def test_scale_down_after_demand_drop(self):
        tmpl = make_template(cpu=4, memory=8192)
        pods = [make_pod("p%d" % i, cpu=1) for i in range(1, 9)]
        for pod in pods:
            if pod.name not in ("p4", "p8"):
                pod.duration = 10  # 6 个 Pod 到期 -> 需求突降
        config = SimConfig(max_ticks=30, scale_down_utilization=0.5,
                           scale_down_window=4, recent_change_window=2)
        sim = Simulator([tmpl], pods=pods, config=config)
        result = sim.run()
        self.assertEqual(2, result.scale_ups)
        self.assertEqual(6, len(sim.finished))
        self.assertEqual(1, result.scale_downs)
        down_events = events_at(result, "缩容:回收节点")
        self.assertEqual(1, len(down_events))
        # Pod 在 t=11 完成,窗口 4 -> 最早 t=14 才允许缩容(防抖)
        self.assertNotIn("[t=11]", down_events[0])
        self.assertNotIn("[t=12]", down_events[0])
        self.assertNotIn("[t=13]", down_events[0])
        self.assertIn("[t=14]", down_events[0])
        # 剩余两个长期 Pod 被整合到同一节点
        self.assertEqual(1, len(sim.nodes))
        self.assertEqual({"p4", "p8"}, {p.name for p in sim.nodes[0].pods})

    def test_scale_down_waits_recent_change_window(self):
        tmpl = make_template(cpu=4, memory=8192)
        pods = [make_pod("a", cpu=3), make_pod("b", cpu=3)]
        events = [
            {"tick": 5, "type": "remove_pod", "name": "a"},
            {"tick": 5, "type": "add_pods",
             "pods": [make_pod("a2", cpu=1)]},
            {"tick": 6, "type": "remove_pod", "name": "b"},
            {"tick": 6, "type": "add_pods",
             "pods": [make_pod("b2", cpu=1)]},
        ]
        config = SimConfig(max_ticks=30, scale_down_utilization=0.5,
                           scale_down_window=3, recent_change_window=4)
        sim = Simulator([tmpl], pods=pods, events=events, config=config)
        result = sim.run()
        self.assertEqual(2, result.scale_ups)
        self.assertEqual(1, result.scale_downs)
        down_events = events_at(result, "缩容:回收节点")
        # t=7 起 node-1 利用率已够低,但 a2 在 t=5 才变动,
        # recent_change_window=4 -> 必须等到 t=9
        self.assertIn("[t=9]", down_events[0])
        skipped = events_at(result, "最近变动的 Pod")
        self.assertTrue(any("[t=7]" in e for e in skipped))
        self.assertTrue(any("[t=8]" in e for e in skipped))
        # a2 被整合到 b2 所在节点
        self.assertEqual(1, len(sim.nodes))
        self.assertEqual({"a2", "b2"}, {p.name for p in sim.nodes[0].pods})

    def test_scale_down_skips_short_term_pods(self):
        tmpl = make_template(cpu=4, memory=8192)
        pods = [
            make_pod("m1", cpu=2),
            make_pod("m2", cpu=2),
            make_pod("s1", cpu=1, short_task=True, duration=8),
        ]
        config = SimConfig(max_ticks=20, scale_down_utilization=0.5,
                           scale_down_window=3, recent_change_window=1)
        sim = Simulator([tmpl], pods=pods, config=config)
        result = sim.run()
        # s1 是短期任务,所在节点在运行期间不得被回收
        self.assertEqual(1, result.scale_downs)
        down_events = events_at(result, "缩容:回收节点")
        # s1 在 t=9 完成,空节点同时间片被回收
        self.assertIn("[t=9]", down_events[0])
        skipped = events_at(result, "短期任务")
        self.assertTrue(skipped)
        self.assertEqual(1, len(sim.nodes))
        self.assertEqual({"m1", "m2"}, {p.name for p in sim.nodes[0].pods})

    def test_scale_down_blocked_when_pods_cannot_reschedule(self):
        tmpl = make_template(cpu=4, memory=8192)
        pods = [
            make_pod("x1", cpu=3, labels={"app": "x"}),
            make_pod("y1", cpu=1, anti_affinity={"app": "x"}),
        ]
        config = SimConfig(max_ticks=20, scale_down_utilization=0.5,
                           scale_down_window=3, recent_change_window=1)
        sim = Simulator([tmpl], pods=pods, events=None, config=config)
        result = sim.run()
        # y1 所在节点利用率 0.25,但 y1 因反亲和无法搬到 x1 所在节点
        self.assertEqual(0, result.scale_downs)
        self.assertEqual(2, len(sim.nodes))
        self.assertTrue(events_at(result, "无法全部重新调度"))


class SpotReclaimTest(unittest.TestCase):
    """抢占实例被回收后,Pod 驱逐并重新调度。"""

    def test_spot_reclaim_reschedules_pods(self):
        spot = make_template(name="spot", cost=0.2, spot=True)
        on_demand = make_template(name="od", cost=1.0)
        events = [{"tick": 3, "type": "reclaim_spot"}]
        sim = Simulator([spot, on_demand], pods=[make_pod("g", cpu=1)],
                        events=events, config=SimConfig(max_ticks=15))
        result = sim.run()
        self.assertEqual(1, result.preemptions)
        self.assertEqual(2, result.scale_ups)
        self.assertEqual([], result.pending_pods)
        self.assertTrue(events_at(result, "抢占回收"))
        self.assertTrue(events_at(result, "被驱逐"))
        running = sim.running_pods()
        self.assertEqual(["g"], [p.name for p in running])
        # 两次扩容都选了更便宜的抢占模板
        self.assertEqual(2, len(events_at(result, "spot=True")))


class JoinFailureTest(unittest.TestCase):
    """节点加入失败后重新扩容,Pod 最终可调。"""

    def test_node_join_failure_retries(self):
        tmpl = make_template(create_delay=1, start_delay=1)
        events = [{"tick": 2, "type": "fail_next_join"}]
        sim = Simulator([tmpl], pods=[make_pod("j", cpu=1)],
                        events=events, config=SimConfig(max_ticks=15))
        result = sim.run()
        self.assertEqual(1, result.join_failures)
        self.assertEqual(2, result.scale_ups)
        self.assertEqual([], result.pending_pods)
        self.assertTrue(events_at(result, "加入集群失败"))
        ready_events = events_at(result, "就绪并加入集群")
        self.assertIn("[t=4]", ready_events[0])


class PermanentPendingTest(unittest.TestCase):
    """没有任何模板装得下的 Pod 永久 pending,并持续给出诊断。"""

    def test_permanent_pending_pod(self):
        tmpl = make_template(cpu=4, memory=8192)
        sim = Simulator([tmpl], pods=[make_pod("too-big", cpu=16)],
                        config=SimConfig(max_ticks=8))
        result = sim.run()
        self.assertEqual(0, result.scale_ups)
        self.assertEqual(["too-big"], result.pending_pods)
        self.assertEqual(["cpu"], result.unschedulable["too-big"])
        self.assertFalse(events_at(result, "too-big 调度到"))
        self.assertFalse(events_at(result, "too-big 预留"))


if __name__ == "__main__":
    unittest.main()
