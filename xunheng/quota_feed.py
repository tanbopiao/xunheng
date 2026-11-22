"""巡衡配额取数适配器（G3 · 真实额度源接入，本地备料版）
道统出处：巡衡观水，不代水行。
工程映射：把 xunheng.probe 的注水模式接上真实额度源——source 可注入
（HTTP 只读 GET / 本机台账文件 / 任意 callable），取数后逐节点入巡衡台账，
越阈经 sink 上报。取数失败不阻断路由主链（行辕 watermark 缺省回退）。
边界：只读不写；黄区源只观测不改配置；越阈的「连 2 日」聚合属上报层，
本件只出事件不聚合。
"""
import time


class QuotaFeed:
    """G3 适配器：source() -> {node_id: 数值配额 或 {"quota","health"}}。

    - feed(xh)：逐节点 xh.probe；source 抛错或返回非法结构 → 记事件不炸轮；
      本轮无新数据时台账保持上轮原样（探针侧只增不覆盖不变）；
    - source 在两轮间少报/多报节点均合法（种子 004：快照外节点路径合法，
      不假设节点集合恒定）；缺报节点本轮不入台账、不视为清零；
    - 越阈：level < low_water（或 quota<=0 视为耗尽）→ sink 事件（只增）。
    """

    def __init__(self, source, low_water=0.1, sink=None, clock=time.time):
        self._source = source
        self._low = low_water
        self._sink = sink or (lambda ev: None)
        self._clock = clock
        self.events = []          # 只增不覆盖：本轮会话内事件留痕
        self.last_nodes = set()   # 上一轮成功入账的节点集（快照外路径观测用）

    # ------------------------------------------------------------------
    def _probe_one(self, xh, nid, val):
        if isinstance(val, dict):
            q = val.get("quota")
            lvl = val.get("level")
            if q is None and lvl is None:
                # 空结构＝无据造零是坏数据（种子 005 同族），不默记为 0 水位
                raise ValueError("无 quota/level 的结构化读数")
            if q is not None:
                return xh.probe(nid, quota=q, health=val.get("health", "ok"))
            return xh.probe(nid, level=lvl, health=val.get("health", "ok"))
        return xh.probe(nid, quota=val)

    def feed(self, xh):
        """取数一轮并入巡衡台账；返回本轮事件数。"""
        try:
            data = self._source()
        except Exception as exc:  # noqa: BLE001 —— 取数故障不阻断路由主链
            self._event("source_error", detail=f"{type(exc).__name__}: {exc}")
            return len(self.events)
        if not isinstance(data, dict):
            self._event("source_error", detail=f"非法结构:{type(data).__name__}")
            return len(self.events)
        seen = set()
        for nid, val in data.items():
            if nid in seen:
                self._event("duplicate_node", node=nid)
                continue
            seen.add(nid)
            try:
                r = self._probe_one(xh, nid, val)
            except Exception as exc:  # noqa: BLE001 —— 单节点脏数据不拖垮整轮
                self._event("probe_error", node=nid, detail=f"{type(exc).__name__}: {exc}")
                continue
            lvl = r.get("level", 0) if isinstance(r, dict) else 0
            if lvl <= 0 or lvl < self._low:
                self._event("low_water", node=nid, level=lvl)
        gone = self.last_nodes - seen
        for nid in sorted(gone):  # 快照外节点路径：缺报节点明示，不默默吞掉
            self._event("node_missing", node=nid)
        self.last_nodes = seen
        return len(self.events)

    # ------------------------------------------------------------------
    def _event(self, kind, **kw):
        ev = {"ts": int(self._clock()), "kind": kind}
        ev.update(kw)
        self.events.append(ev)
        try:
            self._sink(dict(ev))
        except Exception:
            pass  # 上报出口故障不阻断取数轮
        return ev
