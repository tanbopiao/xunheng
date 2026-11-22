"""巡衡算子 —— 道统算子谱水位巡检件（xunheng）
道统出处：巡衡，循册察偏，衡定亏盈（构件定名册三十四员）。
工程映射：资源水位巡检算子（SRS-ROUTING-OPERATOR-001 同级依赖）——
节点配额水位探针与亏盈台账：巡检-记账-察偏-上报，输出供行辕路由
筛选使用；台账只增不覆盖，告警经注入口外送，越阈上报上层裁决。
生长纪律：本地生长自由，推送远端需宗源发令。
"""
import time


class Xunheng:
    """巡衡：水位巡检与亏盈台账。"""

    def __init__(self, alert_sink=None, low_water=0.1):
        # alert_sink: callable(rec) —— 察偏告警外送口（越阈上报，不自行处置）
        self._alert_sink = alert_sink
        self._low = low_water
        self._ledger = []   # 水位台账：只增不覆盖

    # ------------------------------------------------------------------
    def describe(self) -> dict:
        return {"name": "xunheng", "stage": "m1",
                "role": "资源水位巡检算子（配额水位探针/亏盈台账/越阈上报）",
                "records": len(self._ledger)}

    # ------------------------------------------------------------------
    def probe(self, node_id, level=None, fn=None, quota=None, **extra):
        """巡一个节点的水位：fn() 实测或 level 直录；记账并察偏。

        level 语义：0.0–1.0 归一余量，或配额型节点传 quota=剩余配额
        （内部归一为剩余/总量）。返回本次记录 dict。
        """
        if fn is not None:
            level = fn()
        if level is None and quota is not None:
            level = quota
        rec = {"node_id": node_id, "level": level,
               "ts": int(time.time() * 1000)}
        rec.update(extra)
        self._ledger.append(dict(rec))
        self._watch(rec)
        return dict(rec)

    # ------------------------------------------------------------------
    def _watch(self, rec):
        """察偏：水位低于低水位线即告警外送（不自行熔断——归上层裁决）。"""
        lvl = rec.get("level")
        if isinstance(lvl, (int, float)) and lvl <= self._low:
            rec2 = dict(rec, kind="LOW_WATER")
            if self._alert_sink is not None:
                try:
                    self._alert_sink(rec2)
                except Exception:
                    pass  # 告警出口故障不阻断巡检本身
        return rec

    # ------------------------------------------------------------------
    def ledger(self, node_id=None):
        """台账只读视图：可按节点过滤；调用方改动不影响原账。"""
        if node_id is None:
            return [dict(r) for r in self._ledger]
        return [dict(r) for r in self._ledger if r["node_id"] == node_id]

    # ------------------------------------------------------------------
    def latest(self, node_id):
        """某节点最近一次水位（路由筛选用）；无记录返回 None。"""
        for r in reversed(self._ledger):
            if r["node_id"] == node_id:
                return dict(r)
        return None

    # ------------------------------------------------------------------
    def breaches(self, threshold=None):
        """亏盈清单：低于阈值（默认低水位线）的全部记录。"""
        t = self._low if threshold is None else threshold
        return [dict(r, kind="LOW_WATER") for r in self._ledger
                if isinstance(r.get("level"), (int, float)) and r["level"] <= t]

    # ------------------------------------------------------------------
    def summary(self):
        """衡定亏盈：各节点最近水位一览（供路由侧批量拉取）。"""
        out = {}
        for r in self._ledger:
            out[r["node_id"]] = r.get("level")
        return out
