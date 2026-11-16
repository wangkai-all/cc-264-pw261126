"""递归展开：某成品在指定日期的多层需求。

语义约定：
  - 结构按“业务日期 + 事务批次”双维度解析：eff_from <= date < eff_to，
    且该版本在 as_of 批次时已知且未被取代（as_of=None 表示按当前全部知识）；
  - 数量沿路径连乘：required(child) = required(parent) × qty_per。
    共享子件被多个父件/多条路径引用时，每条路径独立展开、各自连乘，
    汇总时再相加 —— 不会因为“已访问过”而漏乘；
  - 环路按路径隔离：仅当子件已出现在当前路径（祖先栈）上时截断该路径，
    打 cycle 标记，其余路径照常展开 —— 不污染整个仓库；
  - 替代槽位：在查询日期上取优先级最高（数值最小）的生效成员；
    解析结果与边上的默认子件不同时标记 via_substitute。
"""


def _effective(row, date):
    return row["eff_from"] <= date and (row["eff_to"] is None or date < row["eff_to"])


def _known_at(row, as_of):
    """该版本在批次 as_of 时是否可见（as_of=None 表示当前最新知识）。"""
    if as_of is None:
        return row["superseded_by"] is None
    return row["batch_id"] <= as_of and (
        row["superseded_by"] is None or row["superseded_by"] > as_of
    )


def load_resolution(conn, as_of=None):
    """一次性读入结构与槽位成员，按父件/槽位分组。"""
    by_parent, mem_by_slot = {}, {}
    for e in conn.execute("SELECT * FROM edges ORDER BY edge_id").fetchall():
        if _known_at(e, as_of):
            by_parent.setdefault(e["parent"], []).append(dict(e))
    for m in conn.execute("SELECT * FROM slot_members ORDER BY member_id").fetchall():
        if _known_at(m, as_of):
            mem_by_slot.setdefault(m["slot"], []).append(dict(m))
    return by_parent, mem_by_slot


def resolve_slot(mem_by_slot, slot, date, default_child):
    """返回 (实际子件, 是否命中替代品)。无生效成员时回退到边上默认子件。"""
    cands = [m for m in mem_by_slot.get(slot, []) if _effective(m, date)]
    if not cands:
        return default_child, False
    cands.sort(key=lambda m: (m["priority"], m["child"]))
    chosen = cands[0]["child"]
    return chosen, chosen != default_child


def explode(conn, product, date, qty, as_of=None, max_rows=200000):
    """展开一个 (成品, 日期, 数量) 需求，返回逐路径行列表（按路径排序）。"""
    by_parent, mem_by_slot = load_resolution(conn, as_of)
    qty = float(qty)
    rows = []
    # 栈元素：物料、累计数量、深度、路径物料、路径边 id、路径单位用量、槽位、是否替代、是否环路截断
    stack = [(product, qty, 0, [product], [], [], None, False, False)]
    while stack:
        item, q, depth, path, eids, qchain, via_slot, via_sub, cyc = stack.pop()
        rows.append({
            "product": product, "need_date": date, "demand_qty": qty,
            "depth": depth, "path": "/".join(path), "item": item,
            "qty_chain": list(qchain), "required_qty": q,
            "edge_ids": list(eids), "via_slot": via_slot,
            "via_substitute": 1 if via_sub else 0, "cycle": 1 if cyc else 0,
        })
        if len(rows) > max_rows:
            raise RuntimeError(f"展开行数超过上限 {max_rows}，疑似异常结构")
        if cyc:
            continue  # 环路截断点：记录需求下限，但不再向下展开
        children = []
        for e in by_parent.get(item, []):
            if not _effective(e, date):
                continue
            if e["slot"]:
                child, is_sub = resolve_slot(mem_by_slot, e["slot"], date, e["child"])
            else:
                child, is_sub = e["child"], False
            # 环路判定只看当前路径（祖先栈），不用全局访问集 —— 共享子件必须能再次展开
            hit_cycle = child in path
            children.append((
                child, q * e["qty_per"], depth + 1, path + [child],
                eids + [e["edge_id"]], qchain + [e["qty_per"]],
                e["slot"], is_sub, hit_cycle,
            ))
        stack.extend(reversed(children))  # 保持边 id 顺序的深度优先
    rows.sort(key=lambda r: r["path"])
    return rows


def totals(rows):
    """按物料汇总需求数量（含环路截断点的下限数量，并标记）。"""
    agg = {}
    for r in rows:
        a = agg.setdefault(r["item"], {
            "item": r["item"], "total": 0.0, "paths": 0, "cycle": False,
        })
        a["total"] += r["required_qty"]
        a["paths"] += 1
        a["cycle"] = a["cycle"] or bool(r["cycle"])
    return sorted(agg.values(), key=lambda a: a["item"])
