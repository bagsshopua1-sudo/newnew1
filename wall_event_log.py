"""
Персистентное SQLite-хранилище для WALL_CANDIDATE/WALL_OUTCOME (и, начиная с
этапа 3/4 аудита, теневых оценок новых ABSORPTION/BREAKOUT критериев -
WOULD_ENTER/WOULD_SKIP). До этого эти события существовали только как текстовые
строки в логах Render - собрать по ним нормальную статистику можно было только
руками через subagent/grep (см. AUDIT_2026-08-18.md, "мини-аудит по реальным
данным"), а Render хранит логи ограниченное время. Здесь то же самое, но в
структурированном виде, который переживает рестарт процесса (частые на free
tier) и по которому можно прогонять SQL-запросы для калибровки порогов на
накопленных данных (см. договорённость с пользователем - "не оптимизируй
пороги, пока не накопится достаточно данных").

Текстовые log.info("WALL_CANDIDATE ...")/("WALL_OUTCOME ...") в signals.py
остаются как есть (полезны для живого дебага в реальном времени) - это
ДОПОЛНИТЕЛЬНОЕ хранилище, не замена.
"""
import os
import sqlite3
import time
from typing import Optional

DB_PATH = "logs/wall_events.db"

SCHEMA = """
CREATE TABLE IF NOT EXISTS wall_candidates (
    id INTEGER PRIMARY KEY,
    ts REAL NOT NULL,
    symbol TEXT NOT NULL,
    type TEXT NOT NULL,           -- absorption | breakout
    side TEXT NOT NULL,
    price REAL NOT NULL,
    size_usd REAL NOT NULL,
    backup_usd REAL NOT NULL,
    age REAL NOT NULL,
    stall INTEGER NOT NULL,
    updates INTEGER NOT NULL,
    refills INTEGER NOT NULL,
    wall_class TEXT NOT NULL,     -- STATIC | ACTIVE
    executed_buy REAL NOT NULL,
    executed_sell REAL NOT NULL,
    executed_buy_recent REAL NOT NULL,   -- вторая половина lookback-окна (см. executed_usd_trend)
    executed_sell_recent REAL NOT NULL,
    zone_cancels_5m INTEGER NOT NULL,
    score REAL NOT NULL,
    passed INTEGER NOT NULL,
    reason TEXT NOT NULL,
    mid REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS wall_outcomes (
    candidate_id INTEGER NOT NULL,
    delay_sec REAL NOT NULL,
    mid REAL NOT NULL,
    delta_pct REAL NOT NULL,
    favorable INTEGER NOT NULL,
    PRIMARY KEY (candidate_id, delay_sec)
);

CREATE TABLE IF NOT EXISTS shadow_evals (
    candidate_id INTEGER NOT NULL,
    stage TEXT NOT NULL,          -- absorption_v2 | breakout_v2 (этапы 3/4 аудита)
    would_enter INTEGER NOT NULL,
    criteria TEXT NOT NULL,       -- JSON-строка отдельных условий и их true/false
    ts REAL NOT NULL,
    PRIMARY KEY (candidate_id, stage)
);
"""


class WallEventLog:
    """
    Каждый метод обёрнут так, чтобы ошибка записи (диск/блокировка SQLite)
    никогда не роняла основной цикл сигналов - это калибровочные данные, а не
    критичный путь исполнения (тот же принцип, что и у текстовых
    WALL_CANDIDATE логов в signals.py).
    """

    def __init__(self, path: str = DB_PATH):
        os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
        self.conn = sqlite3.connect(path, check_same_thread=False)
        self.conn.executescript(SCHEMA)
        self.conn.commit()

    def log_candidate(self, cid: int, symbol: str, type_: str, side: str, price: float,
                       size_usd: float, backup_usd: float, age: float, stall: int, updates: int,
                       refills: int, wall_class: str, executed_buy: float, executed_sell: float,
                       executed_buy_recent: float, executed_sell_recent: float, zone_cancels: int,
                       score: float, passed: bool, reason: str, mid: float, ts: Optional[float] = None):
        try:
            self.conn.execute(
                "INSERT OR REPLACE INTO wall_candidates (id,ts,symbol,type,side,price,size_usd,backup_usd,"
                "age,stall,updates,refills,wall_class,executed_buy,executed_sell,executed_buy_recent,"
                "executed_sell_recent,zone_cancels_5m,score,passed,reason,mid) VALUES "
                "(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (cid, ts if ts is not None else time.time(), symbol, type_, side, price, size_usd,
                 backup_usd, age, stall, updates, refills, wall_class, executed_buy, executed_sell,
                 executed_buy_recent, executed_sell_recent, zone_cancels, score, int(passed), reason, mid),
            )
            self.conn.commit()
        except Exception:
            pass

    def log_outcome(self, candidate_id: int, delay_sec: float, mid: float, delta_pct: float, favorable: bool):
        try:
            self.conn.execute(
                "INSERT OR REPLACE INTO wall_outcomes (candidate_id,delay_sec,mid,delta_pct,favorable) "
                "VALUES (?,?,?,?,?)",
                (candidate_id, delay_sec, mid, delta_pct, int(favorable)),
            )
            self.conn.commit()
        except Exception:
            pass

    def log_shadow_eval(self, candidate_id: int, stage: str, would_enter: bool, criteria_json: str):
        try:
            self.conn.execute(
                "INSERT OR REPLACE INTO shadow_evals (candidate_id,stage,would_enter,criteria,ts) "
                "VALUES (?,?,?,?,?)",
                (candidate_id, stage, int(would_enter), criteria_json, time.time()),
            )
            self.conn.commit()
        except Exception:
            pass

    def close(self):
        self.conn.close()
