"""HTTP API 与网页服务（仅标准库）。

  GET  /                查询页面
  GET  /api/state       仓库状态：发布指针、快照、批次、物料、结构、需求
  GET  /api/explode     多层需求查询：?product=&date=&qty=&live=0|1
  POST /api/ingest      载入样例批次 {"label": "b1"|...}
  POST /api/build       触发构建 {"mode": "auto"|"full", "slow": 秒}
  POST /api/abort       中止构建 {"snapshot_id": n}
  POST /api/reset       清空仓库（演示用）
"""
import json
import os
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse, parse_qs

from . import db as dbmod
from .derive import build_async, request_abort
from .explode import explode
from .samples import load as load_sample, LABELS

WEB_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "web")


def _rows(conn, sql, args=()):
    return [dict(r) for r in conn.execute(sql, args).fetchall()]


def state(conn):
    pub = conn.execute(
        """SELECT p.snapshot_id, p.version, p.base_batch, s.mode, s.note,
                  s.started_at, s.finished_at
           FROM published p LEFT JOIN snapshots s ON s.snapshot_id = p.snapshot_id
           WHERE p.id=1"""
    ).fetchone()
    published = None
    if pub and pub["snapshot_id"]:
        published = dict(pub)
        published["note"] = json.loads(published.get("note") or "{}")
    snaps = _rows(
        conn, "SELECT * FROM snapshots ORDER BY snapshot_id DESC LIMIT 30"
    )
    for s in snaps:
        s["note"] = json.loads(s["note"] or "{}")
    return {
        "published": published,
        "snapshots": snaps,
        "batches": _rows(conn, "SELECT * FROM batches ORDER BY batch_id"),
        "items": _rows(conn, "SELECT * FROM items ORDER BY item_id"),
        "edges": _rows(conn, "SELECT * FROM edges ORDER BY edge_id"),
        "slot_members": _rows(conn, "SELECT * FROM slot_members ORDER BY member_id"),
        "demands": _rows(conn, "SELECT * FROM demands ORDER BY demand_id"),
        "sample_labels": LABELS,
    }


def explode_query(conn, params):
    product = params.get("product", [""])[0].strip()
    date = params.get("date", [""])[0].strip()
    qty = float(params.get("qty", ["1"])[0] or 1)
    live = params.get("live", ["0"])[0] == "1"
    if not product or not date:
        return {"error": "需要 product 与 date 参数"}, 400
    if live:  # 实时重算：直接按当前全部知识展开，不读快照
        rows = explode(conn, product, date, qty)
        meta = {"live": True, "snapshot_version": None}
    else:     # 读已发布快照；数量按需求总量线性缩放（展开对需求数量是线性的）
        pub = conn.execute("SELECT * FROM published WHERE id=1").fetchone()
        if not pub or not pub["snapshot_id"]:
            return {"error": "尚未发布任何快照，请先构建，或勾选“实时重算”"}, 409
        sid = pub["snapshot_id"]
        head = conn.execute(
            """SELECT DISTINCT demand_qty FROM req_rows
               WHERE snapshot_id=? AND product=? AND need_date=?""",
            (sid, product, date),
        ).fetchone()
        if head is None:
            return {
                "error": f"已发布快照(v{pub['version']})中没有 {product}@{date} 的需求；"
                         "可勾选“实时重算”，或接入该需求并重新构建"
            }, 404
        demand_qty = head["demand_qty"]
        scale = qty / demand_qty if demand_qty else 1.0
        rows = []
        for r in conn.execute(
            """SELECT product, need_date, demand_qty, depth, path, item, qty_chain,
                      required_qty, edge_ids, via_slot, via_substitute, cycle
               FROM req_rows WHERE snapshot_id=? AND product=? AND need_date=?
               ORDER BY path""",
            (sid, product, date),
        ).fetchall():
            d = dict(r)
            d["qty_chain"] = json.loads(d["qty_chain"])
            d["edge_ids"] = json.loads(d["edge_ids"])
            d["required_qty"] = d["required_qty"] * scale
            rows.append(d)
        meta = {
            "live": False, "snapshot_version": pub["version"],
            "base_batch": pub["base_batch"], "demand_qty": demand_qty,
            "scaled": abs(scale - 1.0) > 1e-12,
        }
    return {"meta": meta, "rows": rows}, 200


def make_handler(db_path):
    class Handler(BaseHTTPRequestHandler):
        def _conn(self):
            return dbmod.connect(db_path)

        def _send_json(self, obj, code=200):
            body = json.dumps(obj, ensure_ascii=False).encode()
            self.send_response(code)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *args):
            pass

        def do_GET(self):
            u = urlparse(self.path)
            try:
                if u.path == "/":
                    with open(os.path.join(WEB_DIR, "index.html"), "rb") as f:
                        body = f.read()
                    self.send_response(200)
                    self.send_header("Content-Type", "text/html; charset=utf-8")
                    self.send_header("Content-Length", str(len(body)))
                    self.end_headers()
                    self.wfile.write(body)
                elif u.path == "/api/state":
                    conn = self._conn()
                    try:
                        self._send_json(state(conn))
                    finally:
                        conn.close()
                elif u.path == "/api/explode":
                    conn = self._conn()
                    try:
                        obj, code = explode_query(conn, parse_qs(u.query))
                    finally:
                        conn.close()
                    self._send_json(obj, code)
                else:
                    self._send_json({"error": "not found"}, 404)
            except Exception as e:  # noqa: BLE001 - API 边界统一兜底
                self._send_json({"error": str(e)}, 500)

        def do_POST(self):
            u = urlparse(self.path)
            n = int(self.headers.get("Content-Length") or 0)
            try:
                payload = json.loads(self.rfile.read(n)) if n else {}
            except json.JSONDecodeError:
                self._send_json({"error": "请求体不是合法 JSON"}, 400)
                return
            try:
                if u.path == "/api/ingest":
                    conn = self._conn()
                    try:
                        bid, stats = load_sample(conn, payload.get("label", ""))
                    finally:
                        conn.close()
                    self._send_json({"batch_id": bid, "stats": stats})
                elif u.path == "/api/build":
                    sid, _th = build_async(
                        lambda: dbmod.connect(db_path),
                        mode=payload.get("mode", "auto"),
                        slow=min(float(payload.get("slow", 0) or 0), 5.0),
                    )
                    self._send_json({"snapshot_id": sid})
                elif u.path == "/api/abort":
                    ok = request_abort(int(payload.get("snapshot_id", 0)))
                    self._send_json({"ok": ok})
                elif u.path == "/api/reset":
                    dbmod.reset(db_path)
                    self._send_json({"ok": True})
                else:
                    self._send_json({"error": "not found"}, 404)
            except ValueError as e:
                self._send_json({"error": str(e)}, 400)
            except Exception as e:  # noqa: BLE001
                self._send_json({"error": str(e)}, 500)

    return Handler


def serve(db_path, port):
    dbmod.init_db(db_path)
    httpd = ThreadingHTTPServer(("0.0.0.0", port), make_handler(db_path))
    print(f"物料结构仓库已启动: http://localhost:{port}  (db={db_path})")
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass
