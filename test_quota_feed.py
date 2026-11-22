"""G3 配额适配器单测：注水接口/脏数据/快照外节点/越阈上报/取数故障（本地生长备料）。"""
import importlib.util
import os
import unittest

_s = importlib.util.spec_from_file_location(
    "quota_feed", os.path.join(os.path.dirname(os.path.abspath(__file__)), "quota_feed.py"))
_m = importlib.util.module_from_spec(_s)
_s.loader.exec_module(_m)

_x = importlib.util.spec_from_file_location(
    "xunheng_core", os.path.join(os.path.dirname(os.path.abspath(__file__)), "xunheng.py"))
_xh_mod = importlib.util.module_from_spec(_x)
_x.loader.exec_module(_xh_mod)


def make_xh():
    alerts = []
    return _xh_mod.Xunheng(alert_sink=alerts.append, low_water=0.1), alerts


class T(unittest.TestCase):
    def setUp(self):
        self.sink_events = []
        self.feed = _m.QuotaFeed(source=lambda: {}, low_water=0.1,
                                 sink=self.sink_events.append)

    def test_basic_feed(self):
        """正常注水：数值配额与详式结构都入台账。"""
        self.feed._source = lambda: {"n1": 50, "n2": {"quota": 3, "health": "ok"}, "n3": {"level": 0.9}}
        xh, _ = make_xh()
        self.feed.feed(xh)
        self.assertEqual(xh.latest("n1")["level"], 50)
        self.assertEqual(xh.latest("n2")["level"], 3)
        self.assertEqual(xh.latest("n3")["level"], 0.9)
        self.assertEqual(set(self.feed.last_nodes), {"n1", "n2", "n3"})

    def test_low_water_alerts(self):
        """越阈：耗尽（<=0）与低水位（< 阈）都出事件，正常水位不出。"""
        self.feed._source = lambda: {"n1": 0, "n2": 0.05, "n3": 9}
        xh, _ = make_xh()
        n = self.feed.feed(xh)
        kinds = [e["kind"] for e in self.sink_events]
        self.assertEqual(kinds.count("low_water"), 2)
        nodes = {e["node"] for e in self.sink_events if e["kind"] == "low_water"}
        self.assertEqual(nodes, {"n1", "n2"})
        self.assertEqual(n, len(self.sink_events))

    def test_snapshot_out_node_missing(self):
        """种子 004 路径：source 两轮间少报节点——明示 node_missing，不默默吞掉；台账原样保留。"""
        self.feed._source = lambda: {"n1": 5, "n2": 6}
        xh, _ = make_xh()
        self.feed.feed(xh)
        self.feed._source = lambda: {"n1": 4}    # n2 缺报
        self.sink_events.clear()
        self.feed.feed(xh)
        missing = [e for e in self.sink_events if e["kind"] == "node_missing"]
        self.assertEqual([e["node"] for e in missing], ["n2"])
        self.assertEqual(xh.latest("n2")["level"], 6)  # 台账不被清零不改写

    def test_source_error_tolerated(self):
        """取数故障/非法结构：记 source_error 一件，台账保持上轮，不炸轮。"""
        def boom():
            raise RuntimeError("pool down")
        self.feed._source = boom
        xh, _ = make_xh()
        self.assertEqual(self.feed.feed(xh), 1)
        self.assertEqual(self.sink_events[0]["kind"], "source_error")
        self.feed._source = lambda: ["not-a-dict"]
        self.feed.feed(xh)
        self.assertEqual(self.sink_events[-1]["kind"], "source_error")

    def test_dirty_node_isolated(self):
        """单节点脏数据：probe_error 只记该节点，其余节点照常入账。"""
        self.feed._source = lambda: {"bad": {}, "n1": 7}
        xh, alerts = make_xh()
        self.feed.feed(xh)
        kinds = [e["kind"] for e in self.sink_events]
        self.assertIn("probe_error", kinds)
        self.assertEqual(xh.latest("n1")["level"], 7)

    def test_sink_failure_not_blocking(self):
        """上报出口故障：不阻断取数轮（事件仍留本地 events）。"""
        def bad_sink(ev):
            raise IOError("sink down")
        f = _m.QuotaFeed(source=lambda: {"n1": 0}, low_water=0.1, sink=bad_sink)
        xh, _ = make_xh()
        self.assertEqual(f.feed(xh), 1)
        self.assertEqual(f.events[0]["kind"], "low_water")


if __name__ == "__main__":
    unittest.main()
