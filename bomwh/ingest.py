"""原始接入：把一批同时到达的变更作为一个批次写入仓库。

要点：
  - 一个批次可混合携带结构新增、结构更正、替代优先级变化、成品需求；
  - 更正是“追溯式”的：新行自带生效区间，旧行不修改，只标记 superseded_by；
    若更正只覆盖旧行生效区间的一部分，自动拆出前段/后段剩余行（kind='remainder'），
    保证任一业务日期上解析结果唯一；
  - 批次关闭时把统计写回 batches，供构建决策分类使用。
"""
from .db import now_iso


class Ingester:
    def __init__(self, conn, label=""):
        self.conn = conn
        cur = conn.execute(
            "INSERT INTO batches(label, arrived_at) VALUES (?,?)", (label, now_iso())
        )
        self.batch_id = cur.lastrowid
        self.stats = {"edges": 0, "corrections": 0, "subs": 0, "demands": 0}

    # ---- 主数据 ----
    def add_item(self, item_id, name="", kind="component"):
        self.conn.execute(
            "INSERT OR REPLACE INTO items(item_id, name, kind) VALUES (?,?,?)",
            (item_id, name, kind),
        )

    # ---- 结构 ----
    def add_edge(self, parent, child, qty_per, eff_from, eff_to=None,
                 slot=None, supersedes=None):
        kind = "base"
        if supersedes is not None:
            self._supersede_edge(supersedes, eff_from, eff_to)
            kind = "correction"
            self.stats["corrections"] += 1
        else:
            self.stats["edges"] += 1
        cur = self.conn.execute(
            """INSERT INTO edges(parent, child, qty_per, slot, eff_from, eff_to,
                                 kind, batch_id, supersedes)
               VALUES (?,?,?,?,?,?,?,?,?)""",
            (parent, child, qty_per, slot, eff_from, eff_to,
             kind, self.batch_id, supersedes),
        )
        return cur.lastrowid

    def _supersede_edge(self, old_id, new_from, new_to):
        old = self.conn.execute(
            "SELECT * FROM edges WHERE edge_id=?", (old_id,)
        ).fetchone()
        if old is None:
            raise ValueError(f"边#{old_id} 不存在，无法更正")
        if old["superseded_by"] is not None:
            raise ValueError(f"边#{old_id} 已被批次{old['superseded_by']}取代，不能重复更正")
        self.conn.execute(
            "UPDATE edges SET superseded_by=? WHERE edge_id=?", (self.batch_id, old_id)
        )
        # 前段剩余：更正生效日之前，旧版本仍然有效
        if old["eff_from"] < new_from:
            self._insert_remainder(old, old["eff_from"], new_from, old_id)
        # 后段剩余：更正自带失效日且旧行覆盖更远的将来时，旧版本在更正失效后恢复
        if new_to is not None and (old["eff_to"] is None or old["eff_to"] > new_to):
            self._insert_remainder(old, new_to, old["eff_to"], old_id)

    def _insert_remainder(self, old, eff_from, eff_to, old_id):
        self.conn.execute(
            """INSERT INTO edges(parent, child, qty_per, slot, eff_from, eff_to,
                                 kind, batch_id, supersedes)
               VALUES (?,?,?,?,?,?, 'remainder', ?, ?)""",
            (old["parent"], old["child"], old["qty_per"], old["slot"],
             eff_from, eff_to, self.batch_id, old_id),
        )

    # ---- 替代槽位 ----
    def add_slot_member(self, slot, child, priority, eff_from, eff_to=None,
                        supersedes=None):
        kind = "base"
        if supersedes is not None:
            self._supersede_member(supersedes, eff_from, eff_to)
            kind = "correction"
        self.stats["subs"] += 1
        cur = self.conn.execute(
            """INSERT INTO slot_members(slot, child, priority, eff_from, eff_to,
                                        kind, batch_id, supersedes)
               VALUES (?,?,?,?,?,?,?,?)""",
            (slot, child, priority, eff_from, eff_to, kind, self.batch_id, supersedes),
        )
        return cur.lastrowid

    def _supersede_member(self, old_id, new_from, new_to):
        old = self.conn.execute(
            "SELECT * FROM slot_members WHERE member_id=?", (old_id,)
        ).fetchone()
        if old is None:
            raise ValueError(f"槽位成员#{old_id} 不存在，无法更正")
        if old["superseded_by"] is not None:
            raise ValueError(f"槽位成员#{old_id} 已被取代，不能重复更正")
        self.conn.execute(
            "UPDATE slot_members SET superseded_by=? WHERE member_id=?",
            (self.batch_id, old_id),
        )
        if old["eff_from"] < new_from:
            self.conn.execute(
                """INSERT INTO slot_members(slot, child, priority, eff_from, eff_to,
                                            kind, batch_id, supersedes)
                   VALUES (?,?,?,?,?, 'remainder', ?, ?)""",
                (old["slot"], old["child"], old["priority"], old["eff_from"],
                 new_from, self.batch_id, old_id),
            )
        if new_to is not None and (old["eff_to"] is None or old["eff_to"] > new_to):
            self.conn.execute(
                """INSERT INTO slot_members(slot, child, priority, eff_from, eff_to,
                                            kind, batch_id, supersedes)
                   VALUES (?,?,?,?,?, 'remainder', ?, ?)""",
                (old["slot"], old["child"], old["priority"], new_to,
                 old["eff_to"], self.batch_id, old_id),
            )

    # ---- 需求 ----
    def add_demand(self, product, qty, need_date):
        self.conn.execute(
            "INSERT INTO demands(product, qty, need_date, batch_id) VALUES (?,?,?,?)",
            (product, qty, need_date, self.batch_id),
        )
        self.stats["demands"] += 1

    def close(self):
        self.conn.execute(
            """UPDATE batches SET n_edges=?, n_corrections=?, n_subs=?, n_demands=?
               WHERE batch_id=?""",
            (self.stats["edges"], self.stats["corrections"], self.stats["subs"],
             self.stats["demands"], self.batch_id),
        )
        self.conn.commit()
        return self.batch_id
