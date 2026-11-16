#!/usr/bin/env python3
"""无浏览器全流程演示：接入 → 构建发布 → 查询 → 混合批次决策 → 重叠构建 → 中止。

运行：python demo.py
"""
import json
import os
import sys
import tempfile
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from bomwh import db as dbmod                      # noqa: E402
from bomwh.derive import build_async, build_sync, request_abort  # noqa: E402
from bomwh.explode import explode, totals          # noqa: E402
from bomwh.samples import load as load_sample      # noqa: E402


def banner(t):
    print("\n" + "=" * 68 + f"\n{t}\n" + "=" * 68)


def show_totals(rows, title):
    print(f"-- {title}")
    for t in totals(rows):
        mark = "  (含环路截断,数量为下限)" if t["cycle"] else ""
        print(f"   {t['item']:<4} 合计 {t['total']:>8g}  路径数 {t['paths']}{mark}")


def show_note(conn, sid):
    row = conn.execute("SELECT * FROM snapshots WHERE snapshot_id=?", (sid,)).fetchone()
    note = json.loads(row["note"])
    print(f"   快照#{sid} v{row['version']} status={row['status']} mode={row['mode']}")
    for r in note.get("reasons", []):
        print(f"     · {r}")
    print(f"     · 展开 {note.get('exploded_pairs')} 沿用 {note.get('carried_pairs')}")
    return row


def pub_version(conn):
    return conn.execute("SELECT version FROM published WHERE id=1").fetchone()["version"]


def snap_totals(conn, product, date):
    """读已发布快照中某需求对的汇总（模拟页面查询）。"""
    pub = conn.execute("SELECT snapshot_id FROM published WHERE id=1").fetchone()
    rows = conn.execute(
        """SELECT item, SUM(required_qty) AS q, MAX(cycle) AS c FROM req_rows
           WHERE snapshot_id=? AND product=? AND need_date=? GROUP BY item""",
        (pub["snapshot_id"], product, date),
    ).fetchall()
    return {r["item"]: (r["q"], r["c"]) for r in rows}


def main():
    db = os.path.join(tempfile.mkdtemp(prefix="bomwh-demo-"), "demo.db")
    factory = lambda: dbmod.connect(db)  # noqa: E731
    dbmod.init_db(db)
    conn = dbmod.connect(db)

    banner("1. 原始接入 b1：多父节点、菱形/跨层共享、替代槽位、X↔Y 环路")
    bid, stats = load_sample(conn, "b1")
    print(f"   批次#{bid}: {stats}")

    banner("2. 首次构建 → 全量展开并发布")
    sid = build_sync(factory)
    show_note(conn, sid)
    print(f"   已发布版本: v{pub_version(conn)}")

    banner("3. 查询 F1@2026-03-01 ×10：共享子件 C 三条路径各自连乘、汇总不漏乘")
    rows = explode(conn, "F1", "2026-03-01", 10)
    for r in rows:
        chain = "×".join(f"{x:g}" for x in r["qty_chain"]) or "-"
        print(f"   {r['path']:<14} 链[{chain:<7}] = {r['required_qty']:>6g}"
              + ("  [替代]" if r["via_substitute"] else ""))
    show_totals(rows, "汇总")
    t = {x["item"]: x["total"] for x in totals(rows)}
    assert t["C"] == 150 and t["A"] == 20 and t["P"] == 20, t
    print("   ✓ C = 60+40+50 = 150（三条路径分别连乘后相加）")

    banner("4. 查询 F2@2026-03-01 ×5：X↔Y 环路只截断该路径，其余分支正常")
    rows = explode(conn, "F2", "2026-03-01", 5)
    for r in rows:
        print(f"   {r['path']:<16} = {r['required_qty']:>6g}"
              + ("  [环路截断]" if r["cycle"] else ""))
    t = {x["item"]: x["total"] for x in totals(rows)}
    assert t["C"] == 85 and any(r["cycle"] for r in rows), t
    print("   ✓ 环路路径被隔离，C/S/P 等其余分支照常可查")

    banner("5. 混合批次 b2：追溯更正 + 替代优先级翻转 + 新需求 同时到达")
    bid, stats = load_sample(conn, "b2")
    print(f"   批次#{bid}: {stats}")
    sid = build_sync(factory)
    show_note(conn, sid)
    t = snap_totals(conn, "F1", "2026-03-01")
    print(f"   F1@2026-03-01(快照): A={t['A'][0]:g} C={t['C'][0]:g} P2={t.get('P2', (0,))[0]:g}")
    assert t["A"][0] == 30 and t["C"][0] == 180 and t["P2"][0] == 20, t
    print("   ✓ 追溯更正生效（历史日期 A=30）、替代品翻转为 P2")

    banner("6. 纯需求批次 b3 → 安全增量（只展开新需求对，其余沿用）")
    bid, _ = load_sample(conn, "b3")
    sid = build_sync(factory)
    show_note(conn, sid)

    banner("7. 前向更正 b4：F2→C 用量 7→8 自 2026-07-01 生效 → 局部重建")
    bid, _ = load_sample(conn, "b4")
    sid = build_sync(factory)
    show_note(conn, sid)
    t_old = snap_totals(conn, "F2", "2026-03-01")
    t_new = snap_totals(conn, "F2", "2026-08-01")
    print(f"   F2@2026-03-01 直连C={t_old['C'][0]:g}（更正前用量7）")
    print(f"   F2@2026-08-01 直连C部分= {t_new['C'][0]:g}（含S路径与更正后用量8）")
    assert t_old["C"][0] == 85 and t_new["C"][0] == 108, (t_old, t_new)
    print("   ✓ 同一成品不同日期解析到不同用量（生效区间拆分）")

    banner("8. 重叠构建：慢构建先开始、快构建先发布 → 慢构建不得回退")
    load_sample(conn, "stress")
    sid_a, th_a = build_async(factory, mode="full", slow=0.4)   # 先开始，版本号小
    time.sleep(0.1)
    sid_b, th_b = build_async(factory, mode="full", slow=0.0)   # 后开始，版本号大
    th_b.join()
    v_after_b = pub_version(conn)
    th_a.join()
    v_after_a = pub_version(conn)
    st_a = conn.execute("SELECT status FROM snapshots WHERE snapshot_id=?",
                        (sid_a,)).fetchone()["status"]
    print(f"   快构建#{sid_b}发布后: v{v_after_b}；慢构建#{sid_a}完成后: v{v_after_a}")
    print(f"   慢构建状态: {st_a}")
    assert v_after_a == v_after_b and st_a == "stale"
    print("   ✓ 旧构建被拒绝（stale），发布版本未回退")

    banner("9. 构建中止：慢构建中途 abort，已发布版本保持不变")
    v0 = pub_version(conn)
    sid_c, th_c = build_async(factory, mode="full", slow=0.5)
    time.sleep(0.2)
    ok = request_abort(sid_c)
    th_c.join()
    st_c = conn.execute("SELECT status FROM snapshots WHERE snapshot_id=?",
                        (sid_c,)).fetchone()["status"]
    n_left = conn.execute("SELECT COUNT(*) AS n FROM req_rows WHERE snapshot_id=?",
                          (sid_c,)).fetchone()["n"]
    print(f"   abort={ok} 状态={st_c} 残留行={n_left} 发布版本 v{pub_version(conn)}（中止前 v{v0}）")
    assert st_c == "aborted" and n_left == 0 and pub_version(conn) == v0
    print("   ✓ 中止干净：未发布行清理，读者无感知")

    banner("10. 读者一致性：构建期间反复查询，结果恒等于旧快照")
    sid_d, th_d = build_async(factory, mode="full", slow=0.3)
    seen = set()
    while th_d.is_alive():
        t = snap_totals(conn, "F1", "2026-03-01")
        seen.add(tuple(sorted((k, v[0]) for k, v in t.items())))
        time.sleep(0.05)
    th_d.join()
    print(f"   构建期间观察到 {len(seen)} 种不同结果（应为 1：始终是同一份完整快照）")
    assert len(seen) == 1
    print("   ✓ 读者从未看到半新半旧")

    print("\n全部演示断言通过。数据库保留在: " + db)


if __name__ == "__main__":
    main()
