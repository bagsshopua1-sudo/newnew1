"""
Менеджер ордеров — "руки" бота: открывает позицию лимитками, переставляет
неисполненную лимитку вслед за рынком (chase), а после входа выставляет
нативные стоп-лосс и тейк-профит на бирже (reduce-only), которые исполняет
сам движок биржи. Отдельная задача следит за частичным тейком и подтягивает
трейлинг-стоп на оставшуюся часть позиции.

Логика "закрыть в минус / дать плюсу расти" реализована так:
  - SL выставляется сразу после входа, на всю позицию -> минус всегда ограничен.
  - TP1 закрывает часть позиции на первой цели -> фиксируем часть прибыли.
  - Остаток ведём трейлинг-стопом, который подтягивается вверх (long) /
    вниз (short) вслед за ценой, но никогда не отодвигается назад.
"""
import asyncio
import itertools
import logging
import time
from dataclasses import dataclass, field
from typing import Dict, Optional

from config import CFG
from exchange_client import ExchangeClient, MarketInfo
from market_data import MarketData
from risk import RiskManager, TradePlan
from signals import Signal
from trade_log import TradeLog

log = logging.getLogger("orders")

_client_order_counter = itertools.count(int(time.time()) % 1_000_000)


def next_client_order_index() -> int:
    return next(_client_order_counter)


@dataclass
class ManagedPosition:
    symbol: str
    side: str
    market: MarketInfo
    plan: TradePlan
    filled_size: float
    avg_entry: float
    tp1_done: bool = False
    trailing_active: bool = False
    trailing_extreme: float = 0.0  # лучшая цена в пользу позиции с момента активации трейлинга
    current_sl_price: float = 0.0
    opened_at: float = field(default_factory=time.time)
    trade_id: Optional[int] = None


class OrderManager:
    def __init__(self, exchange: ExchangeClient, market_data: MarketData, risk: RiskManager,
                 trade_log: Optional[TradeLog] = None, kill_switch=None):
        self.exchange = exchange
        self.md = market_data
        self.risk = risk
        self.trade_log = trade_log
        self.kill_switch = kill_switch
        self.positions: Dict[str, ManagedPosition] = {}  # symbol -> position
        self._watchers: Dict[str, asyncio.Task] = {}
        self._latest_snap: Dict[str, "BookSnapshot"] = {}
        # Символы, для которых прямо сейчас идёт попытка входа (между сигналом и
        # регистрацией позиции есть await, поэтому has_position() одна не спасает
        # от гонки, если за это время прилетит ещё один сигнал по тому же символу).
        self._entering: set = set()

    def note_snapshot(self, snap):
        self._latest_snap[snap.symbol] = snap

    def has_position(self, symbol: str) -> bool:
        return symbol in self.positions

    # ------------------------------------------------------------------ #
    # Вход в позицию с "чейзингом" лимитки
    # ------------------------------------------------------------------ #

    async def handle_signal(self, market: MarketInfo, signal: Signal):
        if self.kill_switch and self.kill_switch.active:
            return  # аварийная остановка активна - новых входов нет
        if self.has_position(signal.symbol) or signal.symbol in self._entering:
            log.debug("[%s] уже есть открытая позиция или вход в процессе, сигнал пропущен", signal.symbol)
            return
        if not self.risk.can_trade():
            return

        plan = self.risk.build_plan(signal.symbol, signal.side, signal.mid)
        if plan.size <= 0 or plan.size < market.min_base_amount:
            log.warning("[%s] расчётный размер позиции %.6f меньше минимального лота, сигнал пропущен",
                        signal.symbol, plan.size)
            return

        # Синхронно (без await между проверкой и пометкой) резервируем символ,
        # чтобы параллельный сигнал по нему не начал второй вход, пока этот ждёт
        # исполнения лимитки.
        self._entering.add(signal.symbol)
        try:
            filled_size, avg_entry = await self._enter_with_chase(market, plan)
            if filled_size <= 0:
                log.info("[%s] вход не удался (лимитка не исполнилась за %d попыток)",
                          signal.symbol, CFG.max_reprice_attempts + 1)
                return

            pos = ManagedPosition(
                symbol=signal.symbol, side=plan.side, market=market, plan=plan,
                filled_size=filled_size, avg_entry=avg_entry, current_sl_price=plan.stop_price,
            )
            self.positions[signal.symbol] = pos
            if self.trade_log:
                pos.trade_id = self.trade_log.open_trade(
                    signal.symbol, plan.side, avg_entry, filled_size, signal.signal_type
                )
            log.info("[%s] ПОЗИЦИЯ ОТКРЫТА %s size=%.6f avg_entry=%.2f SL=%.2f TP1=%.2f",
                      signal.symbol, plan.side.upper(), filled_size, avg_entry, plan.stop_price, plan.tp1_price)

            await self._place_protective_orders(pos)
            self._watchers[signal.symbol] = asyncio.create_task(self._watch_position(pos))
        finally:
            self._entering.discard(signal.symbol)

    async def _enter_with_chase(self, market: MarketInfo, plan: TradePlan):
        is_ask = plan.side == "short"  # short = продаём, long = покупаем
        remaining = plan.size
        total_filled = 0.0
        weighted_price_sum = 0.0
        coi = next_client_order_index()

        for attempt in range(CFG.max_reprice_attempts + 1):
            snap = self._latest_snap.get(plan.symbol)
            touch_price = (snap.best_ask if is_ask else snap.best_bid) if snap else plan.entry_price
            log.info("[%s] попытка входа #%d: %s лимитка size=%.6f price=%.2f",
                      plan.symbol, attempt + 1, "SELL" if is_ask else "BUY", remaining, touch_price)

            if CFG.mode == "paper":
                result, _, _ = await self.exchange.place_limit_order(
                    market, coi, remaining, touch_price, is_ask, post_only=False,
                )
                filled = getattr(result, "filled_size", 0.0) or 0.0
                avg = getattr(result, "avg_price", touch_price) or touch_price
                if filled > 0:
                    weighted_price_sum += filled * avg
                    total_filled += filled
                    remaining -= filled
                if remaining <= 1e-9:
                    break
                await asyncio.sleep(1)  # даём стакану обновиться перед следующей попыткой
                continue

            if CFG.mode == "live":
                await self.exchange.place_limit_order(market, coi, remaining, touch_price, is_ask, post_only=True)
                await asyncio.sleep(CFG.order_fill_timeout_sec)
                order = await self.exchange.get_active_order_by_client_index(market, coi)
                if order is None:
                    # ордера больше нет в активных -> считаем исполненным полностью
                    total_filled += remaining
                    weighted_price_sum += remaining * touch_price
                    remaining = 0
                    break
                filled_now = market.size_to_float(int(order.filled_base_amount or 0))
                if filled_now > 0:
                    weighted_price_sum += filled_now * touch_price
                    total_filled += filled_now
                    remaining = market.size_to_float(int(order.remaining_base_amount or 0))
                if remaining > 1e-9:
                    await self.exchange.cancel_order(market, order.order_index)
                    coi = next_client_order_index()
                else:
                    break
                continue

            # collect: не торгуем
            return 0.0, 0.0

        avg_entry = (weighted_price_sum / total_filled) if total_filled > 0 else 0.0
        return total_filled, avg_entry

    # ------------------------------------------------------------------ #
    # Защитные ордера (SL / TP1) и трейлинг остатка
    # ------------------------------------------------------------------ #

    async def _place_protective_orders(self, pos: ManagedPosition):
        exit_is_ask = pos.side == "long"  # закрытие long = продажа, закрытие short = покупка
        await self.exchange.create_sl_order(
            pos.market, next_client_order_index(), pos.filled_size, pos.plan.stop_price, exit_is_ask,
        )
        await self.exchange.create_tp_order(
            pos.market, next_client_order_index(), pos.plan.tp1_size, pos.plan.tp1_price, exit_is_ask,
        )

    async def _watch_position(self, pos: ManagedPosition):
        """
        Периодически сверяет позицию с биржей: если размер уменьшился -> сработал
        SL или TP1 -> реагируем (пересчитываем PnL, включаем трейлинг остатка).
        Если позиция обнулилась -> сделка закрыта, регистрируем результат в risk.
        """
        try:
            while True:
                await asyncio.sleep(3)
                live_pos = await self.exchange.get_position(pos.market)
                current_size = abs(live_pos["size"]) if live_pos else 0.0

                if current_size <= 1e-9:
                    pnl, exit_price = self._estimate_closed_pnl(pos)
                    self.risk.register_close(pos.symbol, pos.side, pnl)
                    reason = "tp1+trailing_stop" if pos.tp1_done else "stop_loss"
                    if self.trade_log and pos.trade_id is not None:
                        self.trade_log.close_trade(pos.trade_id, exit_price, pnl, reason)
                    self.positions.pop(pos.symbol, None)
                    log.info("[%s] позиция полностью закрыта (%s).", pos.symbol, reason)
                    return

                if not pos.tp1_done and current_size <= pos.filled_size - pos.plan.tp1_size + 1e-9:
                    pos.tp1_done = True
                    pos.trailing_active = True
                    snap = self._latest_snap.get(pos.symbol)
                    pos.trailing_extreme = snap.mid if snap else pos.avg_entry
                    log.info("[%s] TP1 сработал, остаток %.6f переведён на трейлинг-стоп %.2f%%",
                              pos.symbol, current_size, pos.plan.trailing_stop_pct)

                if pos.trailing_active:
                    await self._update_trailing_stop(pos, current_size)

        except asyncio.CancelledError:
            return

    async def _update_trailing_stop(self, pos: ManagedPosition, current_size: float):
        snap = self._latest_snap.get(pos.symbol)
        if not snap:
            return
        price = snap.mid
        improved = False
        if pos.side == "long" and price > pos.trailing_extreme:
            pos.trailing_extreme = price
            improved = True
        elif pos.side == "short" and (pos.trailing_extreme == 0 or price < pos.trailing_extreme):
            pos.trailing_extreme = price
            improved = True

        if not improved:
            return

        trail_dist = pos.trailing_extreme * pos.plan.trailing_stop_pct / 100
        new_sl = (pos.trailing_extreme - trail_dist) if pos.side == "long" else (pos.trailing_extreme + trail_dist)

        better = (new_sl > pos.current_sl_price) if pos.side == "long" else (new_sl < pos.current_sl_price)
        if not better:
            return

        pos.current_sl_price = new_sl
        exit_is_ask = pos.side == "long"
        # проще и надёжнее всего переставить стоп: он и так стоит на бирже reduce-only,
        # выставляем новый на актуальный remaining size (cancel+create, т.к. modify требует order_index).
        await self.exchange.create_sl_order(pos.market, next_client_order_index(), current_size, new_sl, exit_is_ask)
        log.info("[%s] трейлинг-стоп подтянут до %.2f (пик %.2f)", pos.symbol, new_sl, pos.trailing_extreme)

    def _estimate_closed_pnl(self, pos: ManagedPosition):
        # Best-effort оценка: без прямого фида по исполненным trade-репортам берём
        # последнюю известную mid-цену как приближение цены выхода.
        snap = self._latest_snap.get(pos.symbol)
        exit_price = snap.mid if snap else pos.avg_entry
        direction = 1 if pos.side == "long" else -1
        pnl = (exit_price - pos.avg_entry) * direction * pos.filled_size
        return pnl, exit_price

    # ------------------------------------------------------------------ #
    # Kill switch: аварийное закрытие всех позиций и остановка входов
    # ------------------------------------------------------------------ #

    async def flatten_all(self, reason: str):
        if CFG.mode == "live" and self.exchange.signer:
            try:
                await self.exchange.signer.cancel_all_orders(
                    time_in_force=self.exchange.signer.CANCEL_ALL_TIF_IMMEDIATE,
                    timestamp_ms=int(time.time() * 1000),
                )
            except Exception as e:
                log.error("Не удалось массово отменить ордера: %s", e)

        for symbol, pos in list(self.positions.items()):
            try:
                live_pos = await self.exchange.get_position(pos.market)
            except Exception:
                live_pos = None
            size = abs(live_pos["size"]) if live_pos else pos.filled_size

            if size > 1e-9:
                exit_is_ask = pos.side == "long"  # закрытие long = продажа, short = покупка
                snap = self._latest_snap.get(symbol)
                # закрываемся агрессивно (навстречу лучшей цене), не post-only - это аварийный выход
                price = (snap.best_bid if exit_is_ask else snap.best_ask) if snap else pos.avg_entry
                try:
                    await self.exchange.place_limit_order(
                        pos.market, next_client_order_index(), size, price, exit_is_ask,
                        reduce_only=True, post_only=False,
                    )
                except Exception as e:
                    log.error("[%s] не удалось закрыть позицию при kill switch: %s", symbol, e)

            pnl, exit_price = self._estimate_closed_pnl(pos)
            self.risk.register_close(symbol, pos.side, pnl)
            if self.trade_log and pos.trade_id is not None:
                self.trade_log.close_trade(pos.trade_id, exit_price, pnl, f"kill_switch:{reason}")
            self.positions.pop(symbol, None)

        for t in self._watchers.values():
            t.cancel()
        self._watchers.clear()
        log.warning("Все позиции закрыты, новые входы заблокированы (kill switch: %s).", reason)

    async def shutdown(self):
        for t in self._watchers.values():
            t.cancel()
