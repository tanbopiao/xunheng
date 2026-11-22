"""巡衡算子单测：探针记账/台账只增/察偏告警/亏盈清单/水位供给路由。"""
import importlib.util
import os
import unittest

_s = importlib.util.spec_from_file_location(
    "xunheng_core", os.path.join(os.path.dirname(os.path.abspath(__file__)), "xunheng.py"))
_x = importlib.util.module_from_spec(_s)
_s.loader.exec_module(_x)


class T(unittest.TestCase):
    def setUp(self):
        self.alerts = []
        self.xh = _x.Xunheng(alert_sink=self.alerts.append, low_water=0.1)

    def test_describe(self):
        d = self.xh.describe()
        self.assertEqual(d["name"], "xunheng")
        self.assertEqual(d["stage"], "m1")

    def test_probe_records(self):
        """巡检即记账：实测 fn 与直录 level 均入台账，记录不可变视图。"""
        r1 = self.xh.probe("n1", level=0.75)
        r2 = self.xh.probe("n2", fn=lambda: 0.9)
        self.assertEqual(r1["level"], 0.75)
        self.assertEqual(r2["level"], 0.9)
        self.assertEqual(len(self.xh.ledger()), 2)
        # 台账只读：外部改动不影响原账（只增不覆盖）
        self.xh.ledger()[0]["level"] = 999
        self.assertEqual(self.xh.ledger()[0]["level"], 0.75)

    def test_ledger_filter_and_latest(self):
        """按节点过滤台账；latest 取最近一次水位。"""
        self.xh.probe("n1", level=0.8)
        self.xh.probe("n2", level=0.5)
        self.xh.probe("n1", level=0.3)
        self.assertEqual(len(self.xh.ledger("n1")), 2)
        self.assertEqual(self.xh.latest("n1")["level"], 0.3)
        self.assertIsNone(self.xh.latest("nope"))

    def test_breach_alert(self):
        """察偏：低于低水位线即外送告警；告警出口故障不阻断巡检。"""
        self.xh.probe("n1", level=0.05)
        self.assertEqual(len(self.alerts), 1)
        self.assertEqual(self.alerts[0]["kind"], "LOW_WATER")
        self.xh.probe("n2", level=0.5)
        self.assertEqual(len(self.alerts), 1)  # 正常水位不告警
        boom = _x.Xunheng(alert_sink=lambda r: 1 / 0, low_water=0.1)
        rec = boom.probe("n3", level=0.01)     # 告警口坏，巡检仍记账
        self.assertEqual(rec["level"], 0.01)

    def test_breaches_list(self):
        """亏盈清单：低于阈值的全部记录如实呈报，不自行处置。"""
        self.xh.probe("n1", level=0.05)
        self.xh.probe("n2", level=0.5)
        self.xh.probe("n3", level=0.0)
        bs = self.xh.breaches()
        self.assertEqual({b["node_id"] for b in bs}, {"n1", "n3"})
        bs2 = self.xh.breaches(threshold=0.6)
        self.assertEqual({b["node_id"] for b in bs2}, {"n1", "n3", "n2"})

    def test_summary_feeds_routing(self):
        """衡定亏盈汇总：一次拉取全节点最近水位，供行辕筛选（同级协议）。"""
        self.xh.probe("n1", level=0.9)
        self.xh.probe("n1", level=0.2)
        self.xh.probe("n2", level=0.0)
        s = self.xh.summary()
        self.assertEqual(s, {"n1": 0.2, "n2": 0.0})
        # 与行辕联动：汇总值作中枢水位探针输出
        _s2 = importlib.util.spec_from_file_location(
            "xy_core", os.path.join(os.path.dirname(os.path.dirname(
                os.path.abspath(__file__))), "xingyuan", "xingyuan.py"))
        _xy = importlib.util.module_from_spec(_s2)
        _s2.loader.exec_module(_xy)
        pool = [{"id": "n1", "tags": ["t"], "health": "ok"},
                {"id": "n2", "tags": ["t"], "health": "ok"}]
        r = _xy.Xingyuan().route("t", pool, watermark=s)
        self.assertEqual(r["target_node_id"], "n1")  # n2 水位为 0 被剔除


if __name__ == "__main__":
    unittest.main()
