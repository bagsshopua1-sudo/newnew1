"""
Бэктест: прогоняет CSV из MODE=collect (сборщика стакана) через тот же
SignalEngine + RiskManager, что использует живой бот, и считает статистику.

ВАЖНО - упрощения модели:
  - вход исполняется мгновенно по цене сигнала (без чейзинга лимитки и
    без задержки/непополнения, как в реальном order_manager);
  - выход только по стоп-лоссу или по первой цели (TP1) на весь объём -
    трейлинг остатка после TP1 в бэктесте не моделируется.
Это годится для грубой прикидки "рабочая ли вообще логика сигналов на
этом куске истории", а не для точного прогноза будущей прибыли.

Запуск:
    python backtest.py logs/collect_ETH-USDC_20260817_120000.csv logs/walls_ETH-USDC_20260817_120000.csv ETH-USDC
"""
import csv
import sys
from collections import defaultdict

from market_data import BookSnapshot, Wall
from risk import RiskManager
from signals import SignalEngine


def load_snapshots(snap_path: str, walls_path: str):
    walls_by_ts = defaultdict(list)
    with open(walls_path, newline="") as f:
        for row in csv.DictReader(f):
            walls_by_ts[row["ts"]].append(row)

    snapshots = []
    with open(snap_path, newline="") as f:
        for row in csv.DictReader(f):
            rows = walls_by_ts.get(row["ts"], [])
            bid_walls, ask_walls = [], []
            for w in rows:
                wall = Wall(price=float(w["price"]), size=float(w["size"]), usd=float(w["usd"]),
                            side=w["side"], distance_pct=float(w["distance_pct"]), first_seen=float(w["ts"]))
                (bid_walls if w["side"] == "bid" else ask_walls).append(wall)
            snapshots.append(BookSnapshot(
                market_id="backtest", symbol="SYMBOL",
                best_bid=float(row["best_bid"]), best_ask=float(row["best_ask"]), mid=float(row["mid"]),
                bid_walls=bid_walls, ask_walls=ask_walls,
                bid_volume_near=float(row["bid_vol_usd"]), ask_volume_near=float(row["ask_vol_usd"]),
                imbalance=float(row["imbalance"]), ts=float(row["ts"]),
            ))
    snapshots.sort(key=lambda s: s.ts)
    return snapshots


def run_backtest(symbol: str, snapshots):
    engine = SignalEngine(symbol)
    risk = RiskManager()
    open_trade = None
    trades = []

    for snap in snapshots:
        snap.symbol = symbol

        if open_trade:
            plan = open_trade
            hit_sl = (snap.mid <= plan.stop_price) if plan.side == "long" else (snap.mid >= plan.stop_price)
            hit_tp = (snap.mid >= plan.tp1_price) if plan.side == "long" else (snap.mid <= plan.tp1_price)
            if hit_sl or hit_tp:
                exit_price = plan.stop_price if hit_sl else plan.tp1_price
                direction = 1 if plan.side == "long" else -1
                pnl = (exit_price - plan.entry_price) * direction * plan.size
                risk.register_close(symbol, plan.side, pnl)
                trades.append({"side": plan.side, "entry": plan.entry_price, "exit": exit_price,
                                "pnl": pnl, "reason": "SL" if hit_sl else "TP1"})
                open_trade = None
            continue

        sig = engine.on_snapshot(snap)
        if sig is None or sig.confidence < 0.5:
            continue
        if not risk.can_trade():
            continue
        open_trade = risk.build_plan(symbol, sig.side, sig.mid)

    return trades, risk


def print_report(symbol, snapshots, trades, risk):
    wins = [t for t in trades if t["pnl"] > 0]
    losses = [t for t in trades if t["pnl"] <= 0]
    total_pnl = sum(t["pnl"] for t in trades)

    # грубая просадка по equity curve сделок
    eq = risk.day_start_equity
    peak = eq
    max_dd = 0.0
    for t in trades:
        eq += t["pnl"]
        peak = max(peak, eq)
        max_dd = max(max_dd, (peak - eq))

    print(f"\n=== БЭКТЕСТ {symbol} ===")
    print(f"Снапшотов в файле: {len(snapshots)}")
    print(f"Сделок: {len(trades)}")
    if trades:
        print(f"Win-rate: {len(wins) / len(trades) * 100:.1f}%  "
              f"({len(wins)} прибыльных / {len(losses)} убыточных)")
        print(f"Средний выигрыш: {sum(t['pnl'] for t in wins) / len(wins) if wins else 0:.2f} USD")
        print(f"Средний проигрыш: {sum(t['pnl'] for t in losses) / len(losses) if losses else 0:.2f} USD")
        print(f"Максимальная просадка (по сделкам): {max_dd:.2f} USD")
    print(f"Суммарный условный PnL: {total_pnl:.2f} USD")
    print(f"Итоговый equity: {risk.equity:.2f} USD (старт {CFG_START_EQUITY:.2f} USD)")
    print("\nПОМНИ: упрощённая модель (см. docstring файла) - не финальный вердикт по прибыльности,")
    print("а способ быстро увидеть, генерирует ли текущая логика сигналов вменяемое количество сделок")
    print("и с каким примерно соотношением побед/поражений на твоих реальных данных стакана.")


if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Использование: python backtest.py <collect_csv> <walls_csv> [symbol]")
        sys.exit(1)

    snap_path, walls_path = sys.argv[1], sys.argv[2]
    symbol = sys.argv[3] if len(sys.argv) > 3 else "SYMBOL"

    from config import CFG
    CFG_START_EQUITY = CFG.account_equity_usd

    snapshots = load_snapshots(snap_path, walls_path)
    trades, risk = run_backtest(symbol, snapshots)
    print_report(symbol, snapshots, trades, risk)
