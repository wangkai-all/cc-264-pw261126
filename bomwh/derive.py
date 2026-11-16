"""派生快照：变更分类 → 构建策略决策 → 展开落库 → 原子发布。

策略（决策过程写入快照 note，页面可见）：
  - 安全增量：批次只带来成品需求（新需求对或需求数量变化）。需求是加性事实，
    不影响其他需求对的展开结果，因此只展开这些需求对，其余原样沿用。
  - 必须重建：结构更正或替代优先级变化。数量沿路径连乘、共享子件跨成品聚合，
    局部打补丁不可靠；受影响范围 = 变更父节点的祖先锥（用全部历史边取保守超集，
    保证更正删边/改边后锥只大不小）∩ 已有需求产品。
      · 锥外需求对原样沿用 → 局部重建（partial）；
      · 锥覆盖全部需求对（或手动指定）→ 全量重建（full）。
并发与发布：
  - 每次构建写独立 snapshot_id 的行，已发布快照的行不可变；
  - 版本号在构建开始时单调分配；发布是单条 UPDATE，带双重守卫：
    version 必须更大 且 base_batch 不得更小 —— 重叠构建中晚完成的旧构建被拒绝
    （status='stale'），已发布版本不会回退；
  - 读者先读发布指针、再读该快照的行，因此永远看到完整一致的一份结果，
    不会看到“一半新结构一半旧数量”；
  - 构建可被中止：状态置 aborted 并清理未发布行，已发布版本不受影响。
"""
import json
import threading
import time

from .db import now_iso
from .explode import explode

_VERSION_LOCK = threading.Lock()
_ABORT_LOCK = threading.Lock()
_ABORT_FLAGS = {}  # snapshot_id -> threading.Event


class Aborted(Exception):
    pass


# ---------- 变更分类 ----------

def demand_pairs(conn, base_batch):
    """截至某批次的 (成品, 需求日期) -> 需求总量。"""
    rows = conn.execute(
        """SELECT product, need_date, SUM(qty) AS q FROM demands
           WHERE batch_id<=? GROUP BY product, need_date""",
        (base_batch,),
    ).fetchall()
    return {(r["product"], r["need_date"]): r["q"] for r in rows}


def classify_changes(conn, since_batch, until_batch):
    """(since, until] 区间内的结构/替代变更 → (是否变化, 变更父节点集合, 说明列表)。"""
    changed_parents, reasons = set(), []
    edge_rows = conn.execute(
        """SELECT * FROM edges WHERE batch_id>? AND batch_id<=? AND kind!='remainder'
           ORDER BY edge_id""",
        (since_batch, until_batch),
    ).fetchall()
    for e in edge_rows:
        changed_parents.add(e["parent"])
        if e["supersedes"]:
            old = conn.execute(
                "SELECT parent FROM edges WHERE edge_id=?", (e["supersedes"],)
            ).fetchone()
            if old:
                changed_parents.add(old["parent"])
            reasons.append(
                f"结构更正: {e['parent']}→{e['child']} ×{e['qty_per']:g} "
                f"生效[{e['eff_from']}~{e['eff_to'] or '∞'})，取代边#{e['supersedes']}"
            )
        else:
            reasons.append(
                f"新增结构: {e['parent']}→{e['child']} ×{e['qty_per']:g} "
                f"生效[{e['eff_from']}~{e['eff_to'] or '∞'})"
            )
    sub_rows = conn.execute(
        """SELECT * FROM slot_members WHERE batch_id>? AND batch_id<=?
           AND kind!='remainder' ORDER BY member_id""",
        (since_batch, until_batch),
    ).fetchall()
    for m in sub_rows:
        users = conn.execute(
            "SELECT DISTINCT parent FROM edges WHERE slot=?", (m["slot"],)
        ).fetchall()
        for u in users:
            changed_parents.add(u["parent"])
        tail = f"，取代成员#{m['supersedes']}" if m["supersedes"] else ""
        reasons.append(
            f"替代优先级: 槽位{m['slot']} 成员{m['child']} 优先级={m['priority']}{tail}"
        )
    return bool(edge_rows or sub_rows), changed_parents, reasons


def ancestor_cone(conn, seeds):
    """变更点的祖先锥：沿“全部历史边”（含已取代版本）向上，取保守超集。"""
    rev = {}
    for e in conn.execute("SELECT parent, child FROM edges").fetchall():
        rev.setdefault(e["child"], set()).add(e["parent"])
    seen, stack = set(seeds), list(seeds)
    while stack:
        x = stack.pop()
        for p in rev.get(x, ()):
            if p not in seen:
                seen.add(p)
                stack.append(p)
    return seen


# ---------- 构建 ----------

def allocate_snapshot(conn, mode):
    """分配单调版本号并登记 building 快照（短事务）。"""
    with _VERSION_LOCK:
        v = conn.execute(
            "SELECT COALESCE(MAX(version),0)+1 AS v FROM snapshots"
        ).fetchone()["v"]
        base = conn.execute(
            "SELECT COALESCE(MAX(batch_id),0) AS b FROM batches"
        ).fetchone()["b"]
        cur = conn.execute(
            """INSERT INTO snapshots(version, status, mode, base_batch, started_at)
               VALUES (?,?,?,?,?)""",
            (v, "building", mode, base, now_iso()),
        )
        conn.commit()
        return cur.lastrowid, v, base


def _check_abort(flag):
    if flag is not None and flag.is_set():
        raise Aborted()


def run_build(conn, sid, version, base, req_mode="auto", slow=0.0, abort_flag=None):
    """决策 + 展开 + 落库 + 尝试发布。返回决策说明 dict。"""
    pub = conn.execute("SELECT * FROM published WHERE id=1").fetchone()
    prev = None
    if pub and pub["snapshot_id"]:
        prev = conn.execute(
            "SELECT * FROM snapshots WHERE snapshot_id=?", (pub["snapshot_id"],)
        ).fetchone()

    pairs_now = demand_pairs(conn, base)
    reasons = []
    if prev is None or req_mode == "full":
        mode = "full"
        reasons.append("首个快照，全量展开" if prev is None else "手动指定全量重建")
        to_explode = dict(pairs_now)
        carry = {}
    else:
        since = prev["base_batch"]
        struct_changed, parents, chg = classify_changes(conn, since, base)
        reasons.extend(chg)
        prev_pairs = demand_pairs(conn, since)
        new_or_changed = {
            k: v for k, v in pairs_now.items() if prev_pairs.get(k) != v
        }
        if not struct_changed:
            mode = "incremental"
            to_explode = dict(new_or_changed)
            carry = {k: v for k, v in pairs_now.items() if k not in to_explode}
            reasons.append(
                "仅需求变化（无结构/替代变更）→ 安全增量："
                "只展开新增或数量变化的需求对，其余原样沿用"
            )
        else:
            cone = ancestor_cone(conn, parents)
            to_explode = {
                k: v for k, v in pairs_now.items() if k[0] in cone
            }
            to_explode.update(new_or_changed)
            carry = {k: v for k, v in pairs_now.items() if k not in to_explode}
            reasons.append(
                "变更父节点集合: " + (", ".join(sorted(parents)) or "∅")
            )
            reasons.append(
                "祖先锥（含变更点，按全部历史边保守计算）: "
                + (", ".join(sorted(cone)) or "∅")
            )
            if carry:
                mode = "partial"
                reasons.append(
                    f"局部重建：重展开 {len(to_explode)} 个需求对，"
                    f"沿用 {len(carry)} 个未受影响需求对"
                )
            else:
                mode = "full"
                reasons.append("变更波及全部需求对 → 全量重建")
        if not to_explode and not carry:
            reasons.append("当前无任何需求")

    # 递归全量展开（按 base 批次的事务时间视图，构建期间新到的批次不混入）。
    # 注意：本阶段只读不写，行先缓冲在内存 —— 若在循环里逐对 INSERT，
    # 写事务会贯穿整个慢构建并持锁，把并发构建串行化，发布守卫将失去意义。
    pending = []
    for (product, date), q in sorted(to_explode.items()):
        _check_abort(abort_flag)
        pending.extend(explode(conn, product, date, q, as_of=base))
        if slow:  # 演示用：分段睡眠，便于中途观察/中止
            slept = 0.0
            while slept < slow:
                _check_abort(abort_flag)
                step = min(0.05, slow - slept)
                time.sleep(step)
                slept += step

    # ---- 单个短事务：落行 + 沿用复制 + 原子发布 ----
    conn.execute("UPDATE snapshots SET mode=? WHERE snapshot_id=?", (mode, sid))
    conn.executemany(
        """INSERT INTO req_rows(snapshot_id, product, need_date, demand_qty, depth,
               path, item, qty_chain, required_qty, edge_ids, via_slot,
               via_substitute, cycle)
           VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)""",
        [(sid, r["product"], r["need_date"], r["demand_qty"], r["depth"],
          r["path"], r["item"], json.dumps(r["qty_chain"]), r["required_qty"],
          json.dumps(r["edge_ids"]), r["via_slot"], r["via_substitute"],
          r["cycle"]) for r in pending],
    )
    n_rows = len(pending)

    # 沿用未受影响的需求对（行级复制，已发布行保持不可变）
    carried_rows = 0
    if carry:
        for (product, date) in sorted(carry):
            cur = conn.execute(
                """INSERT INTO req_rows(snapshot_id, product, need_date, demand_qty,
                       depth, path, item, qty_chain, required_qty, edge_ids, via_slot,
                       via_substitute, cycle)
                   SELECT ?, product, need_date, demand_qty, depth, path, item,
                          qty_chain, required_qty, edge_ids, via_slot, via_substitute,
                          cycle
                   FROM req_rows
                   WHERE snapshot_id=? AND product=? AND need_date=?""",
                (sid, prev["snapshot_id"], product, date),
            )
            carried_rows += cur.rowcount

    note = {
        "mode": mode,
        "reasons": reasons,
        "exploded_pairs": sorted(f"{p}@{d}" for (p, d) in to_explode),
        "carried_pairs": sorted(f"{p}@{d}" for (p, d) in carry),
        "exploded_rows": n_rows,
        "carried_rows": carried_rows,
    }
    conn.execute(
        "UPDATE snapshots SET note=?, status='ready', finished_at=? "
        "WHERE snapshot_id=?",
        (json.dumps(note, ensure_ascii=False), now_iso(), sid),
    )

    # 原子发布：版本必须更大、基线批次不得更小；否则本构建已过期，拒绝发布
    cur = conn.execute(
        """UPDATE published SET snapshot_id=?, version=?, base_batch=?
           WHERE id=1 AND version<? AND base_batch<=?""",
        (sid, version, base, version, base),
    )
    if cur.rowcount == 1:
        conn.execute("UPDATE snapshots SET status='archived' WHERE status='published'")
        conn.execute(
            "UPDATE snapshots SET status='published' WHERE snapshot_id=?", (sid,)
        )
        note["published"] = True
    else:
        conn.execute(
            "UPDATE snapshots SET status='stale' WHERE snapshot_id=?", (sid,)
        )
        note["published"] = False
        note["reasons"].append(
            "发布被拒绝：已有不更旧的版本先发布（禁止旧结果回退发布版本）"
        )
    conn.execute(
        "UPDATE snapshots SET note=? WHERE snapshot_id=?",
        (json.dumps(note, ensure_ascii=False), sid),
    )
    conn.commit()
    return note


def build_async(conn_factory, mode="auto", slow=0.0):
    """后台线程构建，返回 (snapshot_id, thread)。"""
    conn = conn_factory()
    sid, version, base = allocate_snapshot(conn, mode)
    conn.close()
    flag = threading.Event()
    with _ABORT_LOCK:
        _ABORT_FLAGS[sid] = flag

    def work():
        c = conn_factory()
        try:
            run_build(c, sid, version, base, mode, slow, flag)
        except Aborted:
            c.execute("DELETE FROM req_rows WHERE snapshot_id=?", (sid,))
            c.execute(
                "UPDATE snapshots SET status='aborted', finished_at=? "
                "WHERE snapshot_id=?",
                (now_iso(), sid),
            )
            c.commit()
        finally:
            c.close()
            with _ABORT_LOCK:
                _ABORT_FLAGS.pop(sid, None)

    th = threading.Thread(target=work, daemon=True)
    th.start()
    return sid, th


def build_sync(conn_factory, mode="auto", slow=0.0):
    sid, th = build_async(conn_factory, mode, slow)
    th.join()
    return sid


def request_abort(sid):
    """请求中止某个进行中的构建；已结束/不存在返回 False。"""
    with _ABORT_LOCK:
        flag = _ABORT_FLAGS.get(sid)
    if flag is None:
        return False
    flag.set()
    return True
