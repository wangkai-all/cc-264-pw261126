"""bomwh 测试：展开语义、更正、替代、环路、构建决策、并发发布。"""
import json
import os
import sys
import tempfile
import time
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

from bomwh import db as dbmod                      # noqa: E402
from bomwh.derive import (                         # noqa: E402
    build_async, build_sync, request_abort,
)
from bomwh.explode import explode, totals          # noqa: E402
from bomwh.samples import load as load_sample      # noqa: E402


class Base(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.mkdtemp(prefix="bomwh-test-")
        self.db = os.path.join(self.dir, "t.db")
        dbmod.init_db(self.db)
        self.factory = lambda: dbmod.connect(self.db)  # noqa: E731
        self.conn = dbmod.connect(self.db)

    def tearDown(self):
        self.conn.close()

    # ---- 工具 ----
    def load(self, label):
        return load_sample(self.conn, label)

    def build(self, mode="auto", slow=0.0):
        return build_sync(self.factory, mode=mode, slow=slow)

    def pub(self):
        return self.conn.execute(
            "SELECT * FROM published WHERE id=1").fetchone()

    def snap_row(self, sid):
        return self.conn.execute(
            "SELECT * FROM snapshots WHERE snapshot_id=?", (sid,)).fetchone()

    def note(self, sid):
        return json.loads(self.snap_row(sid)["note"])

    def snap_totals(self, product, date):
        pub = self.pub()
        rows = self.conn.execute(
            """SELECT item, SUM(required_qty) AS q, MAX(cycle) AS c FROM req_rows
               WHERE snapshot_id=? AND product=? AND need_date=? GROUP BY item""",
            (pub["snapshot_id"], product, date),
        ).fetchall()
        return {r["item"]: (r["q"], r["c"]) for r in rows}


class TestExplode(Base):
    def setUp(self):
        super().setUp()
        self.load("b1")

    def test_shared_child_multiplied_per_path(self):
        """菱形+跨层共享：C 的三条路径各自连乘，汇总 60+40+50=150。"""
        rows = explode(self.conn, "F1", "2026-03-01", 10)
        t = {x["item"]: x["total"] for x in totals(rows)}
        self.assertEqual(t["C"], 150)
        self.assertEqual(t["A"], 20)
        self.assertEqual(t["B"], 10)
        self.assertEqual(t["S"], 10)
        self.assertEqual(t["P"], 20)
        paths_c = sorted(r["path"] for r in rows if r["item"] == "C")
        self.assertEqual(paths_c, ["F1/A/C", "F1/B/C", "F1/S/C"])

    def test_multi_parent_subassembly(self):
        """S 同时挂在 F1、F2 下，两个成品的需求互不影响。"""
        t1 = {x["item"]: x["total"] for x in totals(explode(self.conn, "F1", "2026-03-01", 10))}
        t2 = {x["item"]: x["total"] for x in totals(explode(self.conn, "F2", "2026-03-01", 5))}
        self.assertEqual(t1["S"], 10)
        self.assertEqual(t2["S"], 10)   # 5 × 2
        self.assertEqual(t2["C"], 85)   # 50(S路径) + 35(直连)

    def test_cycle_isolated_per_path(self):
        """X↔Y 环路只截断该路径，其余分支照常展开。"""
        rows = explode(self.conn, "F2", "2026-03-01", 5)
        cyc = [r for r in rows if r["cycle"]]
        self.assertEqual(len(cyc), 1)
        self.assertEqual(cyc[0]["path"], "F2/X/Y/X")
        self.assertEqual(cyc[0]["required_qty"], 5)
        t = {x["item"]: x["total"] for x in totals(rows)}
        self.assertEqual(t["C"], 85)    # 环路不影响其他分支
        self.assertEqual(t["P"], 20)

    def test_cycle_does_not_block_shared_node_elsewhere(self):
        """环路中的物料经其他路径到达时仍正常展开（无全局访问集）。"""
        rows = explode(self.conn, "F2", "2026-03-01", 5)
        x_rows = [r for r in rows if r["item"] == "X"]
        self.assertEqual(len(x_rows), 2)  # 正常到达一次 + 环路截断一次

    def test_slot_resolution_and_substitute_flag(self):
        rows = explode(self.conn, "F1", "2026-03-01", 10)
        p = [r for r in rows if r["item"] == "P"][0]
        self.assertEqual(p["via_slot"], "SLOT_MOTOR")
        self.assertEqual(p["via_substitute"], 0)  # P 是默认子件，不算替代

    def test_as_of_batch_time_travel(self):
        """as_of 限定事务时间：更正前的批次视图仍看到旧结构。"""
        self.load("b2")
        old = explode(self.conn, "F1", "2026-03-01", 10, as_of=1)
        new = explode(self.conn, "F1", "2026-03-01", 10)
        t_old = {x["item"]: x["total"] for x in totals(old)}
        t_new = {x["item"]: x["total"] for x in totals(new)}
        self.assertEqual(t_old["A"], 20)   # 批次1视角：旧用量 2
        self.assertEqual(t_new["A"], 30)   # 当前视角：追溯更正后 3


class TestCorrectionsAndSubs(Base):
    def setUp(self):
        super().setUp()
        self.load("b1")

    def test_retroactive_correction(self):
        """追溯更正：生效日倒签，历史查询日期也用新用量。"""
        self.load("b2")
        t = {x["item"]: x["total"] for x in totals(explode(self.conn, "F1", "2026-03-01", 10))}
        self.assertEqual(t["A"], 30)
        self.assertEqual(t["C"], 180)  # 90+40+50

    def test_substitute_priority_flip(self):
        self.load("b2")
        rows = explode(self.conn, "F1", "2026-03-01", 10)
        items = {r["item"] for r in rows}
        self.assertIn("P2", items)
        self.assertNotIn("P", items)
        p2 = [r for r in rows if r["item"] == "P2"][0]
        self.assertEqual(p2["via_substitute"], 1)

    def test_forward_correction_splits_validity(self):
        """前向更正自动拆分生效区间：更正日前用旧值，之后用新值。"""
        self.load("b3")
        self.load("b4")
        t_old = {x["item"]: x["total"] for x in totals(explode(self.conn, "F2", "2026-03-01", 5))}
        t_new = {x["item"]: x["total"] for x in totals(explode(self.conn, "F2", "2026-08-01", 6))}
        self.assertEqual(t_old["C"], 85)    # 直连 5×7
        self.assertEqual(t_new["C"], 108)   # 直连 6×8 + S路径 6×2×5
        # 旧边被取代，且拆出了前段剩余行
        rem = self.conn.execute(
            "SELECT * FROM edges WHERE kind='remainder'").fetchall()
        self.assertEqual(len(rem), 1)
        self.assertEqual((rem[0]["eff_from"], rem[0]["eff_to"]), ("2026-01-01", "2026-07-01"))

    def test_double_correction_rejected(self):
        self.load("b2")
        from bomwh.ingest import Ingester
        ing = Ingester(self.conn, "bad")
        with self.assertRaises(ValueError):
            ing.add_edge("F1", "A", 9, "2026-01-01", supersedes=1)


class TestBuildDecisions(Base):
    def setUp(self):
        super().setUp()
        self.load("b1")
        self.build()

    def test_first_build_is_full(self):
        s = self.conn.execute("SELECT * FROM snapshots ORDER BY snapshot_id").fetchall()
        self.assertEqual(s[0]["mode"], "full")
        self.assertEqual(s[0]["status"], "published")

    def test_mixed_batch_triggers_full_rebuild(self):
        """更正+优先级+需求同批到达：祖先锥覆盖全部需求 → 全量重建。"""
        self.load("b2")
        sid = self.build()
        n = self.note(sid)
        self.assertEqual(self.snap_row(sid)["mode"], "full")
        self.assertTrue(any("结构更正" in r for r in n["reasons"]))
        self.assertTrue(any("替代优先级" in r for r in n["reasons"]))
        t = self.snap_totals("F1", "2026-03-01")
        self.assertEqual(t["A"][0], 30)
        self.assertEqual(t["P2"][0], 20)

    def test_demand_only_batch_is_incremental(self):
        self.load("b3")
        sid = self.build()
        n = self.note(sid)
        self.assertEqual(self.snap_row(sid)["mode"], "incremental")
        self.assertEqual(sorted(n["exploded_pairs"]),
                         ["F1@2026-08-01", "F2@2026-08-01"])
        self.assertEqual(len(n["carried_pairs"]), 2)
        self.assertGreater(n["carried_rows"], 0)

    def test_local_correction_is_partial_rebuild(self):
        """只波及 F2 的更正 → 局部重建：F2 重展开，F1 原样沿用。"""
        self.load("b3")
        self.build()
        self.load("b4")
        sid = self.build()
        n = self.note(sid)
        self.assertEqual(self.snap_row(sid)["mode"], "partial")
        self.assertEqual(sorted(n["exploded_pairs"]),
                         ["F2@2026-03-01", "F2@2026-08-01"])
        self.assertEqual(sorted(n["carried_pairs"]),
                         ["F1@2026-03-01", "F1@2026-08-01"])
        # 沿用行与重展开行在同一快照中各自完整
        t_old = self.snap_totals("F2", "2026-03-01")
        t_new = self.snap_totals("F2", "2026-08-01")
        self.assertEqual(t_old["C"][0], 85)
        self.assertEqual(t_new["C"][0], 108)

    def test_snapshot_rows_immutable_after_partial(self):
        """局部重建后，上一版快照的行不被修改（沿用是复制不是挪动）。"""
        pub1 = self.pub()["snapshot_id"]
        before = self.conn.execute(
            "SELECT COUNT(*) AS n FROM req_rows WHERE snapshot_id=?", (pub1,)
        ).fetchone()["n"]
        self.load("b4")
        self.build()
        after = self.conn.execute(
            "SELECT COUNT(*) AS n FROM req_rows WHERE snapshot_id=?", (pub1,)
        ).fetchone()["n"]
        self.assertEqual(before, after)


class TestConcurrency(Base):
    def setUp(self):
        super().setUp()
        self.load("b1")
        self.build()
        self.load("stress")

    def test_overlapping_builds_no_rollback(self):
        """重叠构建：先开始的慢构建完成后不得回退已发布的更新版本。"""
        sid_a, th_a = build_async(self.factory, mode="full", slow=0.4)
        time.sleep(0.1)
        sid_b, th_b = build_async(self.factory, mode="full", slow=0.0)
        th_b.join()
        v1 = self.pub()["version"]
        th_a.join()
        v2 = self.pub()["version"]
        self.assertEqual(v1, v2)  # 未回退
        self.assertEqual(self.snap_row(sid_a)["status"], "stale")
        self.assertEqual(self.snap_row(sid_b)["status"], "published")
        self.assertGreater(self.snap_row(sid_b)["version"],
                           self.snap_row(sid_a)["version"])

    def test_abort_keeps_published_and_cleans_rows(self):
        v0 = self.pub()["version"]
        sid, th = build_async(self.factory, mode="full", slow=0.5)
        time.sleep(0.2)
        self.assertTrue(request_abort(sid))
        th.join()
        self.assertEqual(self.snap_row(sid)["status"], "aborted")
        n = self.conn.execute(
            "SELECT COUNT(*) AS n FROM req_rows WHERE snapshot_id=?", (sid,)
        ).fetchone()["n"]
        self.assertEqual(n, 0)
        self.assertEqual(self.pub()["version"], v0)
        self.assertFalse(request_abort(sid))  # 已结束不能再中止

    def test_reader_consistency_during_build(self):
        """构建进行中读者反复查询：结果恒等于同一份完整旧快照。"""
        seen = set()
        sid, th = build_async(self.factory, mode="full", slow=0.3)
        while th.is_alive():
            t = self.snap_totals("F1", "2026-03-01")
            seen.add(tuple(sorted((k, v[0]) for k, v in t.items())))
            time.sleep(0.03)
        th.join()
        self.assertEqual(len(seen), 1)
        self.assertEqual(dict((k, v) for k, v in seen.pop())["C"], 150)

    def test_version_monotonic(self):
        sids = [build_sync(self.factory) for _ in range(3)]
        vs = [self.snap_row(s)["version"] for s in sids]
        self.assertEqual(vs, sorted(vs))
        self.assertEqual(len(set(vs)), 3)


if __name__ == "__main__":
    unittest.main(verbosity=2)
