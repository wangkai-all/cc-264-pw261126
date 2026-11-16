"""SQLite 存储层。

双时间维度：
  - 生效区间 [eff_from, eff_to)：业务时间，回答“哪一天结构是什么”；
  - batch_id / superseded_by：事务时间，回答“哪个批次知道这件事”。
历史行不可变：更正不修改旧行，只把旧行标记为被某批次取代，并按生效区间拆出剩余段。
派生结果（req_rows）按快照隔离，读者只读已发布快照。
"""
import os
import sqlite3
from datetime import datetime

SCHEMA = """
CREATE TABLE IF NOT EXISTS items(
  item_id   TEXT PRIMARY KEY,
  name      TEXT NOT NULL DEFAULT '',
  kind      TEXT NOT NULL DEFAULT 'component'   -- finished | component | purchased
);

CREATE TABLE IF NOT EXISTS batches(
  batch_id      INTEGER PRIMARY KEY AUTOINCREMENT,
  label         TEXT NOT NULL DEFAULT '',
  arrived_at    TEXT NOT NULL,
  n_edges       INTEGER NOT NULL DEFAULT 0,
  n_corrections INTEGER NOT NULL DEFAULT 0,
  n_subs        INTEGER NOT NULL DEFAULT 0,
  n_demands     INTEGER NOT NULL DEFAULT 0
);

-- 物料结构版本。kind: base=原始, correction=更正, remainder=更正拆出的剩余段
CREATE TABLE IF NOT EXISTS edges(
  edge_id       INTEGER PRIMARY KEY AUTOINCREMENT,
  parent        TEXT NOT NULL,
  child         TEXT NOT NULL,
  qty_per       REAL NOT NULL,
  slot          TEXT,                            -- 非空表示该子件经替代槽位解析
  eff_from      TEXT NOT NULL,
  eff_to        TEXT,                            -- NULL = 开放
  kind          TEXT NOT NULL DEFAULT 'base',
  batch_id      INTEGER NOT NULL,                -- 记录批次（事务时间起点）
  supersedes    INTEGER,                         -- 本行更正/拆分自哪一行
  superseded_by INTEGER                          -- 被哪个批次取代（NULL = 当前有效）
);

-- 替代槽位成员：同一槽位下按优先级挑选子件
CREATE TABLE IF NOT EXISTS slot_members(
  member_id     INTEGER PRIMARY KEY AUTOINCREMENT,
  slot          TEXT NOT NULL,
  child         TEXT NOT NULL,
  priority      INTEGER NOT NULL,                -- 数值小者优先
  eff_from      TEXT NOT NULL,
  eff_to        TEXT,
  kind          TEXT NOT NULL DEFAULT 'base',
  batch_id      INTEGER NOT NULL,
  supersedes    INTEGER,
  superseded_by INTEGER
);

CREATE TABLE IF NOT EXISTS demands(
  demand_id INTEGER PRIMARY KEY AUTOINCREMENT,
  product   TEXT NOT NULL,
  qty       REAL NOT NULL,
  need_date TEXT NOT NULL,
  batch_id  INTEGER NOT NULL
);

-- 派生快照：一次构建一份，行不可变；发布是单点切换
CREATE TABLE IF NOT EXISTS snapshots(
  snapshot_id INTEGER PRIMARY KEY AUTOINCREMENT,
  version     INTEGER NOT NULL UNIQUE,           -- 单调递增，构建开始时分配
  status      TEXT NOT NULL,                     -- building|ready|published|archived|aborted|stale
  mode        TEXT,                              -- full|partial|incremental
  base_batch  INTEGER NOT NULL,                  -- 本快照包含到哪个批次
  note        TEXT NOT NULL DEFAULT '{}',        -- 决策说明 JSON
  started_at  TEXT,
  finished_at TEXT
);

CREATE TABLE IF NOT EXISTS req_rows(
  row_id          INTEGER PRIMARY KEY AUTOINCREMENT,
  snapshot_id     INTEGER NOT NULL,
  product         TEXT NOT NULL,
  need_date       TEXT NOT NULL,
  demand_qty      REAL NOT NULL,                 -- 该需求对的需求总量
  depth           INTEGER NOT NULL,
  path            TEXT NOT NULL,                 -- 例如 F1/S/C，来源路径
  item            TEXT NOT NULL,
  qty_chain       TEXT NOT NULL,                 -- JSON：沿路径的单位用量
  required_qty    REAL NOT NULL,                 -- 需求总量 × 数量链连乘
  edge_ids        TEXT NOT NULL,                 -- JSON：沿路径用到的结构版本 id（溯源）
  via_slot        TEXT,                          -- 经哪个替代槽位解析
  via_substitute  INTEGER NOT NULL DEFAULT 0,    -- 实际命中了替代品
  cycle           INTEGER NOT NULL DEFAULT 0     -- 该行为环路截断点
);
CREATE INDEX IF NOT EXISTS idx_req ON req_rows(snapshot_id, product, need_date);

-- 单行表：当前发布指针。更新带版本与基线守卫，保证原子切换且不回退
CREATE TABLE IF NOT EXISTS published(
  id          INTEGER PRIMARY KEY CHECK(id=1),
  snapshot_id INTEGER NOT NULL DEFAULT 0,
  version     INTEGER NOT NULL DEFAULT 0,
  base_batch  INTEGER NOT NULL DEFAULT 0
);
INSERT OR IGNORE INTO published(id) VALUES (1);
"""


def now_iso():
    return datetime.now().isoformat(timespec="seconds")


def connect(db_path):
    if db_path != ":memory:":
        os.makedirs(os.path.dirname(os.path.abspath(db_path)), exist_ok=True)
    conn = sqlite3.connect(db_path, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def init_db(db_path):
    conn = connect(db_path)
    conn.executescript(SCHEMA)
    conn.commit()
    return conn


def reset(db_path):
    """清空仓库（演示用）：删除库文件与 WAL 侧文件后重建模式。"""
    for suffix in ("", "-wal", "-shm"):
        p = db_path + suffix
        if os.path.exists(p):
            os.remove(p)
    return init_db(db_path)
