"""
Риск-менеджмент: размер позиции от % риска на сделку, расчёт стопа/тейков,
дневной лимит убытка и пауза после серии убыточных сделок (circuit breaker).

Это "мозг", который умеет закрыть сделку в минус (жёсткий стоп) и дать
прибыли расти (частичный тейк + трейлинг), а также остановить бота,
если день идёт плохо — вместо того чтобы пытаться отыграться.
"""
import asyncio
import logging
import time
from dataclasses import dataclass, field
from typing import Awaitable, Callable, List, Optional

from config import CFG

log = logging.getLogger("risk")


@dataclass
class TradePlan:
    symbol: str
    side: str  # "long" | "short"
    entry_price: float
    size: float
    stop_price: float
    tp1_price: float
    tp1_size: float
    trailing_stop_pct: float


@dataclass
class ClosedTrade:
    symbol: str
    side: str
    pnl_usd: float
    ts: float = field(default_factory=time.time)


class RiskManager:
    def __init__(self, on_breach: Optional[Callable[[str], Awaitable[None]]] = None):
        """
        on_breach(reason) - опциональный async-колбэк, вызывается ОДИН раз при
        пробитии дневного лимита убытка или серии убытков подряд. Обычно
        привязывается к kill_switch.trigger, чтобы автоматически закрыть все
        позиции, а не просто перестать открывать новые.
        """
        self.equity = CFG.account_equity_usd
        self.day_start_equity = CFG.account_equity_usd
        self.day_key = time.strftime("%Y-%m-%d")
        self.consecutive_losses = 0
        self.cooldown_until: Optional[float] = None
        self.closed_trades: List[ClosedTrade] = []
        self.on_breach = on_breach
        self._daily_breach_notified = False
        self._streak_breach_notified = False

    # ------------------------------------------------------------------ #

    def can_trade(self) -> bool:
        self._roll_day_if_needed()
        if self.cooldown_until and time.time() < self.cooldown_until:
            return False

        daily_loss = self.day_start_equity - self.equity
        if daily_loss >= self.day_start_equity * CFG.daily_loss_limit_pct / 100:
            if not self._daily_breach_notified:
                self._daily_breach_notified = True
                log.warning("Дневной лимит убытка достигнут (%.2f USD) — торговля остановлена до конца дня.",
                            daily_loss)
                self._notify_breach("daily_loss_limit")
            return False

        if self.consecutive_losses >= CFG.max_consecutive_losses:
            self.cooldown_until = time.time() + CFG.cooldown_minutes * 60
            if not self._streak_breach_notified:
                self._streak_breach_notified = True
                log.warning("%d убытков подряд — пауза на %.0f мин.",
                            self.consecutive_losses, CFG.cooldown_minutes)
                self._notify_breach("max_consecutive_losses")
            return False

        return True

    def _notify_breach(self, reason: str):
        if self.on_breach:
            try:
                asyncio.create_task(self.on_breach(reason))
            except RuntimeError:
                pass  # нет активного event loop (например, в бэктесте) - просто пропускаем

    def _roll_day_if_needed(self):
        key = time.strftime("%Y-%m-%d")
        if key != self.day_key:
            self.day_key = key
            self.day_start_equity = self.equity
            self.consecutive_losses = 0
            self.cooldown_until = None
            self._daily_breach_notified = False
            self._streak_breach_notified = False

    # ------------------------------------------------------------------ #

    def build_plan(self, symbol: str, side: str, entry_price: float,
                    wall_price: Optional[float] = None) -> TradePlan:
        """
        wall_price - цена стенки/уровня, который породил сигнал (Signal.reference_price).
        Стоп считается от неё (+ буфер), а не фиксированным % - расстояние диктует
        реальная структура рынка на момент сигнала, а не одно и то же число для
        каждой сделки. Зажимаем в [MIN_STOP_PCT, MAX_STOP_PCT], чтобы не получить
        ни слишком узкий стоп (шум выбьет), ни неоправданно широкий.
        """
        if wall_price:
            raw_distance = abs(entry_price - wall_price) + entry_price * CFG.stop_buffer_pct / 100
        else:
            raw_distance = entry_price * CFG.stop_loss_pct / 100  # запасной вариант

        min_distance = entry_price * CFG.min_stop_pct / 100
        max_distance = entry_price * CFG.max_stop_pct / 100
        stop_distance = min(max(raw_distance, min_distance), max_distance)

        risk_usd = self.equity * CFG.risk_per_trade_pct / 100
        raw_size = risk_usd / stop_distance  # база (в монете), риск ровно = risk_usd при срабатывании стопа

        # ограничение по плечу: не больше max_leverage * equity в позиции
        max_notional = self.equity * CFG.max_leverage
        size = min(raw_size, max_notional / entry_price)

        # TP1 = стоп-дистанция * risk:reward - тоже привязан к структуре через
        # стоп, а не отдельный независимый фиксированный %.
        if side == "long":
            stop_price = entry_price - stop_distance
            tp1_price = entry_price + stop_distance * CFG.rr_target_1
        else:
            stop_price = entry_price + stop_distance
            tp1_price = entry_price - stop_distance * CFG.rr_target_1

        return TradePlan(
            symbol=symbol,
            side=side,
            entry_price=entry_price,
            size=size,
            stop_price=stop_price,
            tp1_price=tp1_price,
            tp1_size=size * CFG.take_profit_1_size,
            trailing_stop_pct=CFG.trailing_stop_pct,
        )

    def rebase_plan_to_fill(self, plan: TradePlan, actual_entry_price: float) -> TradePlan:
        """
        build_plan() считает entry_price/stop/TP1 от цены сигнала (signal.mid,
        Binance) - но это делается ДО реального входа. Настоящее исполнение
        происходит на Lighter и почти всегда отличается: basis Binance/Lighter,
        плюс между сигналом и фактической отправкой ордера проходит время
        (задержка сети, реприсинг), за которое цена успевает сдвинуться. На
        практике это давало SL/TP, привязанные к точке, где сделка на самом деле
        не открывалась (наблюдалось расхождение $24-33 при цене ~64200, то есть
        сделка стартовала уже "просевшей" относительно расчётной структуры и
        _thesis_invalidated срабатывал почти сразу).
        Пересчитываем SL/TP от РЕАЛЬНОЙ цены входа, сохраняя ту же дистанцию (в
        цене), которая уже была посчитана от структуры сигнала - size не трогаем,
        он уже определяет фактический риск в USD от исходного risk_per_trade_pct.
        """
        stop_distance = abs(plan.entry_price - plan.stop_price)
        tp1_distance = abs(plan.tp1_price - plan.entry_price)
        if plan.side == "long":
            new_stop = actual_entry_price - stop_distance
            new_tp1 = actual_entry_price + tp1_distance
        else:
            new_stop = actual_entry_price + stop_distance
            new_tp1 = actual_entry_price - tp1_distance
        if abs(actual_entry_price - plan.entry_price) > plan.entry_price * 0.02 / 100:
            log.info("[%s] SL/TP пересчитаны от реальной цены входа %.2f (план был от %.2f, "
                      "расхождение %.2f) -> SL=%.2f TP1=%.2f",
                      plan.symbol, actual_entry_price, plan.entry_price,
                      actual_entry_price - plan.entry_price, new_stop, new_tp1)
        plan.entry_price = actual_entry_price
        plan.stop_price = new_stop
        plan.tp1_price = new_tp1
        return plan

    def register_close(self, symbol: str, side: str, pnl_usd: float):
        self.equity += pnl_usd
        self.closed_trades.append(ClosedTrade(symbol=symbol, side=side, pnl_usd=pnl_usd))
        if pnl_usd < 0:
            self.consecutive_losses += 1
        else:
            self.consecutive_losses = 0
            self._streak_breach_notified = False
        log.info("Сделка закрыта %s %s PnL=%.2f USD | equity=%.2f | подряд убытков=%d",
                  symbol, side, pnl_usd, self.equity, self.consecutive_losses)
