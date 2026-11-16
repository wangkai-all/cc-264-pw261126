"""样例批次：覆盖多父节点、共享子件、追溯更正、替代优先级变化、环路、压测结构。

  b1     基础结构 + 需求（含菱形共享、跨层共享、多父节点、X↔Y 环路）
  b2     混合批次：追溯更正 + 替代优先级翻转 + 新需求 同时到达 → 决策为全量重建
  b3     纯需求批次 → 安全增量
  b4     前向更正（只波及 F2 的祖先锥）→ 局部重建，演示生效区间拆分
  stress 压测结构（60 件、10 个需求对）→ 供“慢构建 / 中止 / 重叠重建”演示
"""
from .ingest import Ingester

LABELS = ["b1", "b2", "b3", "b4", "stress"]

EFF0 = "2026-01-01"


def load(conn, label):
    ing = Ingester(conn, label)
    handler = {
        "b1": _b1, "b2": _b2, "b3": _b3, "b4": _b4, "stress": _stress,
    }.get(label)
    if handler is None:
        raise ValueError(f"未知样例批次 {label!r}，可选: {LABELS}")
    handler(ing)
    ing.close()
    return ing.batch_id, ing.stats


def _b1(ing):
    items = [
        ("F1", "总成F1（成品）", "finished"), ("F2", "总成F2（成品）", "finished"),
        ("A", "部件A", "component"), ("B", "部件B", "component"),
        ("S", "子装配件S", "component"), ("C", "标准件C（螺丝）", "purchased"),
        ("P", "电机P（主选）", "purchased"), ("P2", "电机P2（替代）", "purchased"),
        ("X", "组件X", "component"), ("Y", "组件Y", "component"),
    ]
    for it in items:
        ing.add_item(*it)
    # 菱形共享：F1→A→C、F1→B→C；跨层共享：F1→S→C；多父节点：F1、F2 都用 S
    ing.add_edge("F1", "A", 2, EFF0)
    ing.add_edge("F1", "B", 1, EFF0)
    ing.add_edge("F1", "S", 1, EFF0)
    ing.add_edge("A", "C", 3, EFF0)
    ing.add_edge("B", "C", 4, EFF0)
    ing.add_edge("S", "C", 5, EFF0)
    # 替代槽位：S 下的电机，P 主选、P2 备选
    ing.add_edge("S", "P", 2, EFF0, slot="SLOT_MOTOR")
    ing.add_slot_member("SLOT_MOTOR", "P", 1, EFF0)
    ing.add_slot_member("SLOT_MOTOR", "P2", 2, EFF0)
    # F2：共享 S，直连 C，并挂入 X↔Y 环路分支
    ing.add_edge("F2", "S", 2, EFF0)
    ing.add_edge("F2", "C", 7, EFF0)
    ing.add_edge("F2", "X", 1, EFF0)
    ing.add_edge("X", "Y", 1, EFF0)
    ing.add_edge("Y", "X", 1, EFF0)
    # 成品需求
    ing.add_demand("F1", 10, "2026-03-01")
    ing.add_demand("F2", 5, "2026-03-01")


def _b2(ing):
    """混合批次：追溯更正 + 替代优先级翻转 + 新需求 同时到达。"""
    # 追溯更正：F1→A 用量 2→3，生效日倒签回 2026-01-01（影响历史查询日期）
    old = ing.conn.execute(
        "SELECT edge_id FROM edges WHERE parent='F1' AND child='A' "
        "AND superseded_by IS NULL"
    ).fetchone()
    ing.add_edge("F1", "A", 3, EFF0, supersedes=old["edge_id"])
    # 替代优先级翻转：P2 升为最优（优先级 0）
    old_m = ing.conn.execute(
        "SELECT member_id FROM slot_members WHERE slot='SLOT_MOTOR' AND child='P2' "
        "AND superseded_by IS NULL"
    ).fetchone()
    ing.add_slot_member("SLOT_MOTOR", "P2", 0, EFF0, supersedes=old_m["member_id"])
    # 新成品需求
    ing.add_demand("F1", 8, "2026-06-01")


def _b3(ing):
    """纯需求批次：只新增需求，不碰结构 → 应决策为安全增量。"""
    ing.add_demand("F1", 12, "2026-08-01")
    ing.add_demand("F2", 6, "2026-08-01")


def _b4(ing):
    """前向更正：F2→C 用量 7→8，自 2026-07-01 生效。

    只波及 F2 的祖先锥 → 局部重建；旧版本在 2026-07-01 前继续有效
    （自动拆出前段剩余行），同一需求在不同日期解析到不同用量。
    """
    old = ing.conn.execute(
        "SELECT edge_id FROM edges WHERE parent='F2' AND child='C' "
        "AND superseded_by IS NULL"
    ).fetchone()
    ing.add_edge("F2", "C", 8, "2026-07-01", supersedes=old["edge_id"])


def _stress(ing):
    """压测结构：60 件三叉树 + 10 个需求对，让构建足够慢以便演示中止/重叠。"""
    for i in range(60):
        ing.add_item(f"G{i:02d}", f"压测件{i}", "finished" if i == 0 else "component")
    for i in range(1, 60):
        ing.add_edge(f"G{(i - 1) // 3:02d}", f"G{i:02d}", (i % 4) + 1, EFF0)
    for p in range(10):
        ing.add_demand(f"G{p:02d}", 2, "2026-03-01")
