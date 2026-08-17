"""
SQLite-журнал сделок: каждая открытая/закрытая позиция, PnL, причина закрытия.
Источник правды для дашборда (история сделок, equity curve, win-rate).
"""
import os
import sqlite3
import time
from dataclasses import dataclass
from typing import List, Optional

DB_PATH = "logs/trades.db"

SCHEMA = """
CREATE TABLE IF NOT EXISTS trades (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol TEXT NOT NULL,
    side TEXT NOT NULL,
    entry_price REAL NOT NULL,
    exit_price REAL,
    size REAL NOT NULL,
    pnl_usd REAL,
    opened_at REAL NOT NULL,
    closed_at REAL,
    close_reason TEXT,
    signal_type TEXT
);
"""


@dataclass
class TradeRecord:
    id: int
    symbol: str
    side: str
    entry_price: float
    exit_price: Optional[float]
    size: float
    pnl_usd: Optional[float]
    opened_at: float
    closed_at: Optional[float]
    close_reason: Optional[str]
    signal_type: Optional[str]


class TradeLog:
    def __init__(self, path: str = DB_PATH):
        os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
        self.conn = sqlite3.connect(path, check_same_thread=False)
        self.conn.execute(SCHEMA)
        self.conn.commit()

    def open_trade(self, symbol: str, side: str, entry_price: float, size: float, signal_type: str) -> int:
        cur = self.conn.execute(
            "INSERT INTO trades (symbol, side, entry_price, size, opened_at, signal_type) VALUES (?,?,?,?,?,?)",
            (symbol, side, entry_price, size, time.time(), signal_type),
        )
        self.conn.commit()
        return cur.lastrowid

    def close_trade(self, trade_id: int, exit_price: float, pnl_usd: float, reason: str):
        self.conn.execute(
            "UPDATE trades SET exit_price=?, pnl_usd=?, closed_at=?, close_reason=? WHERE id=?",
            (exit_price, pnl_usd, time.time(), reason, trade_id),
        )
        self.conn.commit()

    def recent(self, limit: int = 50) -> List[TradeRecord]:
        rows = self.conn.execute(
            "SELECT id,symbol,side,entry_price,exit_price,size,pnl_usd,opened_at,closed_at,close_reason,signal_type "
            "FROM trades ORDER BY id DESC LIMIT ?", (limit,)
        ).fetchall()
        return [TradeRecord(*r) for r in rows]

    def stats(self) -> dict:
        rows = self.conn.execute("SELECT pnl_usd FROM trades WHERE closed_at IS NOT NULL").fetchall()
        pnls = [r[0] for r in rows if r[0] is not None]
        wins = [p for p in pnls if p > 0]
        losses = [p for p in pnls if p <= 0]
        return {
            "total_trades": len(pnls),
            "win_rate": round((len(wins) / len(pnls) * 100), 1) if pnls else 0.0,
            "total_pnl": round(sum(pnls), 2),
            "avg_win": round(sum(wins) / len(wins), 2) if wins else 0.0,
            "avg_loss": round(sum(losses) / len(losses), 2) if losses else 0.0,
        }

    def equity_curve(self, start_equity: float) -> List[dict]:
        rows = self.conn.execute(
            "SELECT closed_at, pnl_usd FROM trades WHERE closed_at IS NOT NULL ORDER BY closed_at ASC"
        ).fetchall()
        eq = start_equity
        curve = [{"ts": time.time(), "equity": eq}]
        for ts, pnl in rows:
            eq += pnl or 0
            curve.append({"ts": ts, "equity": eq})
        return curve

    def close(self):
        self.conn.close()
