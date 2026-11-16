#!/usr/bin/env python3
"""命令行入口：初始化、接入样例、构建、查询、起服务。

用法示例：
  python run.py init
  python run.py ingest b1 && python run.py build
  python run.py explode F1 --date 2026-03-01 --qty 10
  python run.py serve --port 8000
"""
import argparse
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from bomwh import db as dbmod          # noqa: E402
from bomwh.derive import build_sync    # noqa: E402
from bomwh.explode import explode, totals  # noqa: E402
from bomwh.samples import LABELS, load as load_sample  # noqa: E402

ROOT = os.path.dirname(os.path.abspath(__file__))
DEFAULT_DB = os.path.join(ROOT, "data", "bom.db")


def cmd_init(db):
    dbmod.init_db(db)
    print(f"已初始化仓库: {db}")


def cmd_ingest(db, label):
    conn = dbmod.connect(db)
    bid, stats = load_sample(conn, label)
    conn.close()
    print(f"批次 #{bid} 已入仓: {stats}")


def cmd_build(db, mode, slow):
    factory = lambda: dbmod.connect(db)  # noqa: E731
    dbmod.init_db(db)
    sid = build_sync(factory, mode=mode, slow=slow)
    conn = dbmod.connect(db)
    row = conn.execute("SELECT * FROM snapshots WHERE snapshot_id=?", (sid,)).fetchone()
    note = json.loads(row["note"])
    print(f"构建 #{sid} 结束: status={row['status']} mode={row['mode']}")
    for r in note.get("reasons", []):
        print(f"  - {r}")
    conn.close()


def cmd_explode(db, product, date, qty, live):
    conn = dbmod.connect(db)
    if live:
        rows = explode(conn, product, date, qty)
        print(f"[实时] {product} @ {date} ×{qty:g}")
    else:
        pub = conn.execute("SELECT * FROM published WHERE id=1").fetchone()
        if not pub or not pub["snapshot_id"]:
            print("尚未发布快照，请先 build 或使用 --live")
            return
        sid = pub["snapshot_id"]
        head = conn.execute(
            """SELECT DISTINCT demand_qty FROM req_rows
               WHERE snapshot_id=? AND product=? AND need_date=?""",
            (sid, product, date),
        ).fetchone()
        if head is None:
            print(f"快照 v{pub['version']} 中没有 {product}@{date} 的需求")
            return
        scale = qty / head["demand_qty"] if head["demand_qty"] else 1.0
        rows = []
        for r in conn.execute(
            """SELECT * FROM req_rows WHERE snapshot_id=? AND product=? AND need_date=?
               ORDER BY path""",
            (sid, product, date),
        ).fetchall():
            d = dict(r)
            d["qty_chain"] = json.loads(d["qty_chain"])
            d["required_qty"] = d["required_qty"] * scale
            rows.append(d)
        print(f"[快照 v{pub['version']}] {product} @ {date} ×{qty:g}")
    for t in totals(rows):
        mark = "  (含环路截断)" if t["cycle"] else ""
        print(f"  {t['item']:<6} 合计 {t['total']:>10g}  路径数 {t['paths']}{mark}")
    print("逐路径:")
    for r in rows:
        chain = "×".join(f"{x:g}" for x in r["qty_chain"]) or "-"
        flags = (" [环路截断]" if r["cycle"] else "") + (" [替代品]" if r["via_substitute"] else "")
        print(f"  {'  ' * r['depth']}{r['path']:<28} {chain:<10} = {r['required_qty']:>10g}{flags}")
    conn.close()


def cmd_status(db):
    conn = dbmod.connect(db)
    pub = conn.execute("SELECT * FROM published WHERE id=1").fetchone()
    print(f"发布指针: snapshot={pub['snapshot_id']} version={pub['version']} base={pub['base_batch']}")
    for s in conn.execute("SELECT * FROM snapshots ORDER BY snapshot_id").fetchall():
        print(f"  #{s['snapshot_id']} v{s['version']} {s['status']:<10} mode={s['mode']} base={s['base_batch']}")
    conn.close()


def main():
    ap = argparse.ArgumentParser(description="物料结构仓库")
    ap.add_argument("--db", default=DEFAULT_DB, help="SQLite 路径")
    sub = ap.add_subparsers(dest="cmd", required=True)
    sub.add_parser("init")
    p = sub.add_parser("ingest")
    p.add_argument("label", choices=LABELS)
    p = sub.add_parser("build")
    p.add_argument("--mode", choices=["auto", "full"], default="auto")
    p.add_argument("--slow", type=float, default=0.0)
    p = sub.add_parser("explode")
    p.add_argument("product")
    p.add_argument("--date", required=True)
    p.add_argument("--qty", type=float, default=1.0)
    p.add_argument("--live", action="store_true")
    p = sub.add_parser("serve")
    p.add_argument("--port", type=int, default=8000)
    sub.add_parser("status")
    sub.add_parser("reset")
    args = ap.parse_args()

    if args.cmd == "init":
        cmd_init(args.db)
    elif args.cmd == "ingest":
        cmd_ingest(args.db, args.label)
    elif args.cmd == "build":
        cmd_build(args.db, args.mode, args.slow)
    elif args.cmd == "explode":
        cmd_explode(args.db, args.product, args.date, args.qty, args.live)
    elif args.cmd == "serve":
        from bomwh.server import serve
        serve(args.db, args.port)
    elif args.cmd == "status":
        cmd_status(args.db)
    elif args.cmd == "reset":
        dbmod.reset(args.db)
        print("仓库已清空")


if __name__ == "__main__":
    main()
