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
    # Тезис сделки (для "умного" выхода по развалу структуры) - см. _thesis_invalidated
    signal_type: str = ""
    reference_price: float = 0.0
    realized_pnl: float = 0.0  # накапливается по частичным закрытиям (TP1 + финал)


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
        self._latest_snap: Dict[str, "BookSnapshot"] = {}  # стакан Lighter - цена исполнения
        self._latest_signal_snap: Dict[str, "BookSnapshot"] = {}  # стакан-источник сигнала (Binance) - структура
        # Символы, для которых прямо сейчас идёт попытка входа (между сигналом и
        # регистрацией позиции есть await, поэтому has_position() одна не спасает
        # от гонки, если за это время прилетит ещё один сигнал по тому же символу).
        self._entering: set = set()

    def note_snapshot(self, snap):
        self._latest_snap[snap.symbol] = snap

    def note_signal_snapshot(self, snap):
        self._latest_signal_snap[snap.symbol] = snap

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

        plan = self.risk.build_plan(signal.symbol, signal.side, signal.mid,
                                     wall_price=signal.reference_price)
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
                signal_type=signal.signal_type, reference_price=signal.reference_price,
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

            if CFG.mode == "paper":
                # PaperClient умеет исполнять только пересекающие спред IOC-ордера
                # (нет модели резидентной лимитки в очереди) - берём цену с
                # противоположной стороны стакана и добавляем небольшой буфер,
                # чтобы гарантированно пробить topbook и получить fill.
                cross_price = (snap.best_bid if is_ask else snap.best_ask) if snap else plan.entry_price
                buf = cross_price * CFG.paper_cross_buffer_pct / 100
                order_price = (cross_price - buf) if is_ask else (cross_price + buf)
            else:
                order_price = touch_price

            log.info("[%s] попытка входа #%d: %s лимитка size=%.6f price=%.2f",
                      plan.symbol, attempt + 1, "SELL" if is_ask else "BUY", remaining, order_price)

            if CFG.mode == "paper":
                result, _, _ = await self.exchange.place_limit_order(
                    market, coi, remaining, order_price, is_ask, post_only=False,
                )
                filled = getattr(result, "filled_size", 0.0) or 0.0
                avg = getattr(result, "avg_price", touch_price) or touch_price
                if filled > 0:
                    weighted_price_sum += filled * avg
                    total_filled += filled
                    remaining -= filled
                # Остаток может быть настолько мал, что после округления до
                # size_decimals рынка превращается в 0 - PaperClient.validate_order
                # тогда падает с ValueError("base amount must be positive, got 0.0").
                # Такой "пыльный" хвост считаем полностью исполненным, а не гоняемся
                # за ним ещё одной попыткой.
                tick = 10 ** (-market.size_decimals)
                if remaining <= 1e-9 or round(remaining, market.size_decimals) < tick:
                    remaining = 0.0
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
        Следит за открытой позицией и решает, когда её закрывать - тремя путями:
          - цена дошла до стопа (защита от убытка, дистанция посчитана от структуры
            сигнала в risk.build_plan, не фиксированный %);
          - цена дошла до TP1 (фиксируем часть, risk:reward от той же стоп-дистанции)
            -> остаток переводим на трейлинг, чтобы прибыль могла расти дальше;
          - "умный" выход: сама причина входа перестала быть верной (стенка, от
            которой фейдили, снята и цена уже прошла её; пробой не удержался;
            дисбаланс резко развернулся против позиции) - закрываем ДО того, как
            цена вообще дойдёт до стопа, вместо того чтобы ждать его вслепую.
        В live-режиме на бирже дополнительно висят нативные reduce-only SL/TP
        (см. _place_protective_orders) как резервная защита на случай обрыва
        связи с ботом - эта функция не полагается только на них.
        """
        try:
            while True:
                await asyncio.sleep(CFG.position_check_interval_sec)
                snap = self._latest_snap.get(pos.symbol)  # цена исполнения (Lighter)
                if snap is None:
                    continue
                price = snap.mid

                hit_stop = (price <= pos.current_sl_price) if pos.side == "long" else (price >= pos.current_sl_price)
                if hit_stop:
                    await self._close_position_now(pos, snap, "stop_loss")
                    if not self.has_position(pos.symbol):
                        return
                    # закрывающий ордер исполнился не полностью (paper: не пересёк
                    # спред) - позиция всё ещё числится открытой, пробуем закрыть
                    # остаток на следующем тике вместо того чтобы бросить слежение.
                    continue

                if not pos.tp1_done:
                    hit_tp1 = (price >= pos.plan.tp1_price) if pos.side == "long" else (price <= pos.plan.tp1_price)
                    if hit_tp1:
                        await self._partial_close(pos, snap, pos.plan.tp1_size, "tp1")
                        pos.tp1_done = True
                        pos.trailing_active = True
                        pos.trailing_extreme = price
                        log.info("[%s] TP1 сработал, остаток переведён на трейлинг-стоп %.2f%%",
                                  pos.symbol, pos.plan.trailing_stop_pct)

                if pos.trailing_active:
                    await self._update_trailing_stop(pos, price)

                # структура сигнала оцениваем по тому же стакану, откуда пришёл
                # сигнал (Binance, если включён - там реальные стенки/дисбаланс)
                signal_snap = self._latest_signal_snap.get(pos.symbol) or snap
                if time.time() - pos.opened_at >= CFG.thesis_grace_period_sec and \
                        self._thesis_invalidated(pos, signal_snap):
                    await self._close_position_now(pos, snap, "structure_invalidated")
                    if not self.has_position(pos.symbol):
                        return
                    continue

        except asyncio.CancelledError:
            return

    def _thesis_invalidated(self, pos: ManagedPosition, snap) -> bool:
        """
        "Умный" выход: проверяет, жива ли ещё сама причина, по которой вошли в
        сделку - а не только цена относительно стопа/тейка.
        """
        if snap is None or not pos.reference_price:
            return False

        if pos.signal_type == "absorption":
            # Тезис: стенка держится и её ещё не пробили. Инвалидация - стенка
            # исчезла с той стороны, от которой фейдили, И цена уже прошла её
            # уровень (не просто пропала из выдачи на секунду).
            wall_side = "bid" if pos.side == "long" else "ask"
            walls = snap.bid_walls if wall_side == "bid" else snap.ask_walls
            still_there = any(
                abs(w.price - pos.reference_price) / pos.reference_price <= CFG.wall_max_distance_pct / 100
                for w in walls
            )
            price_broke_through = (snap.mid < pos.reference_price) if pos.side == "long" \
                else (snap.mid > pos.reference_price)
            if not still_there and price_broke_through:
                return True
            # дисбаланс резко развернулся против позиции - давление сменилось
            imb = snap.imbalance if pos.side == "long" else (1 - snap.imbalance)
            if imb < (1 - CFG.imbalance_threshold):
                return True
            return False

        if pos.signal_type == "breakout":
            # Тезис: пробой удержался. Инвалидация - цена вернулась обратно за
            # уровень пробоя (failed breakout / ложный пробой).
            if pos.side == "long" and snap.mid < pos.reference_price:
                return True
            if pos.side == "short" and snap.mid > pos.reference_price:
                return True
            return False

        return False

    async def _update_trailing_stop(self, pos: ManagedPosition, price: float):
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
        if CFG.mode == "live":
            # Двигаем нативный резервный стоп на бирже (best-effort - order_index
            # предыдущего SL не отслеживается и явно не отменяется, известное
            # ограничение; на paper это не нужно - там протекция считается тут же).
            live_pos = await self.exchange.get_position(pos.market)
            current_size = abs(live_pos["size"]) if live_pos else pos.filled_size
            exit_is_ask = pos.side == "long"
            await self.exchange.create_sl_order(pos.market, next_client_order_index(),
                                                 current_size, new_sl, exit_is_ask)
        log.info("[%s] трейлинг-стоп подтянут до %.2f (пик %.2f)", pos.symbol, new_sl, pos.trailing_extreme)

    def _close_price(self, snap, exit_is_ask: bool, fallback_price: float) -> float:
        """
        Цена для reduce-only закрывающего ордера. В paper-режиме PaperClient
        исполняет только IOC, пересекающие спред (см. _enter_with_chase) - пассивная
        цена (голый best_bid/best_ask) часто НЕ пересекает книгу гарантированно
        (округления/рассинхрон снепшота), и ордер исполняется частично или вообще
        не исполняется. Раньше это делалось без буфера - в проде это привело к
        тому, что позиция считалась закрытой (мы её убирали из self.positions),
        а на самом paper-аккаунте оставался непогашенный остаток, который затем
        складывался со следующим входом (наблюдалось: 2 входа по 0.046780 BTC
        дали 0.093560 к моменту следующего закрытия). В live-режиме буфер не
        нужен - там реальная книга и другой механизм исполнения.
        """
        if CFG.mode != "paper" or snap is None:
            return fallback_price
        cross_price = snap.best_bid if exit_is_ask else snap.best_ask
        buf = cross_price * CFG.paper_cross_buffer_pct / 100
        return (cross_price - buf) if exit_is_ask else (cross_price + buf)

    async def _partial_close(self, pos: ManagedPosition, snap, size: float, reason: str):
        exit_is_ask = pos.side == "long"  # закрытие long = продажа, short = покупка
        fallback = (snap.best_bid if exit_is_ask else snap.best_ask) if snap else pos.avg_entry
        price = self._close_price(snap, exit_is_ask, fallback)
        exit_price = price
        filled = 0.0
        try:
            result, _, _ = await self.exchange.place_limit_order(
                pos.market, next_client_order_index(), size, price, exit_is_ask,
                reduce_only=True, post_only=False,
            )
            if result is not None:
                filled = getattr(result, "filled_size", 0.0) or 0.0
                avg = getattr(result, "avg_price", None)
                if avg:
                    exit_price = avg
        except Exception as e:
            log.error("[%s] частичное закрытие (%s) не удалось: %s", pos.symbol, reason, e)
            return

        if filled <= 1e-9:
            log.warning("[%s] частичное закрытие (%s): ордер не исполнился (0 из %.6f) - позиция НЕ уменьшена",
                        pos.symbol, reason, size)
            return
        if filled < size - 1e-9:
            log.warning("[%s] частичное закрытие (%s): исполнилось только %.6f из запрошенных %.6f",
                        pos.symbol, reason, filled, size)
        size = filled

        direction = 1 if pos.side == "long" else -1
        pnl = (exit_price - pos.avg_entry) * direction * size
        pos.realized_pnl += pnl
        pos.filled_size -= size
        self.risk.register_close(pos.symbol, pos.side, pnl)
        log.info("[%s] %s: закрыто %.6f по %.2f | PnL этой части=%.2f", pos.symbol, reason, size, exit_price, pnl)

    async def _close_position_now(self, pos: ManagedPosition, snap, reason: str):
        try:
            live_pos = await self.exchange.get_position(pos.market)
        except Exception:
            live_pos = None
        current_size = abs(live_pos["size"]) if live_pos else 0.0

        exit_is_ask = pos.side == "long"
        fallback = (snap.best_bid if exit_is_ask else snap.best_ask) if snap else pos.avg_entry
        price = self._close_price(snap, exit_is_ask, fallback)
        exit_price = price
        closed_size = 0.0
        if current_size > 1e-9:
            try:
                result, _, _ = await self.exchange.place_limit_order(
                    pos.market, next_client_order_index(), current_size, price, exit_is_ask,
                    reduce_only=True, post_only=False,
                )
                if result is not None:
                    closed_size = getattr(result, "filled_size", 0.0) or 0.0
                    avg = getattr(result, "avg_price", None)
                    if avg:
                        exit_price = avg
                elif CFG.mode != "paper":
                    # live: результат ордера не возвращается синхронно тем же способом -
                    # считаем закрытым весь запрошенный размер (как и раньше).
                    closed_size = current_size
            except Exception as e:
                log.error("[%s] закрытие позиции (%s) не удалось: %s", pos.symbol, reason, e)

        remaining = current_size - closed_size
        if remaining > 1e-9:
            # Не исполнилось полностью (частый случай для paper без кросс-буфера
            # раньше) - НЕ считаем позицию закрытой и не убираем из self.positions,
            # иначе на бирже останется реальный "хвост", который тихо сложится со
            # следующим входом по этому же символу (см. комментарий в _close_price).
            log.warning("[%s] закрытие позиции (%s): исполнилось %.6f из %.6f, остаток %.6f - "
                        "позиция остаётся под наблюдением, повторим на следующей проверке",
                        pos.symbol, reason, closed_size, current_size, remaining)
            if closed_size > 1e-9:
                direction = 1 if pos.side == "long" else -1
                pnl = (exit_price - pos.avg_entry) * direction * closed_size
                pos.realized_pnl += pnl
                self.risk.register_close(pos.symbol, pos.side, pnl)
            pos.filled_size = remaining
            return

        direction = 1 if pos.side == "long" else -1
        pnl = (exit_price - pos.avg_entry) * direction * current_size
        pos.realized_pnl += pnl
        self.risk.register_close(pos.symbol, pos.side, pnl)
        if self.trade_log and pos.trade_id is not None:
            self.trade_log.close_trade(pos.trade_id, exit_price, pos.realized_pnl, reason)
        self.positions.pop(pos.symbol, None)
        log.info("[%s] позиция закрыта (%s): последний кусок %.6f по %.2f | PnL сделки суммарно=%.2f",
                  pos.symbol, reason, current_size, exit_price, pos.realized_pnl)

    def _estimate_closed_pnl(self, pos: ManagedPosition):
        # Используется только аварийным flatten_all (kill switch) - best-effort
        # оценка по последней известной mid-цене, без ожидания фактического филла.
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

            exit_price = None
            if size > 1e-9:
                exit_is_ask = pos.side == "long"  # закрытие long = продажа, short = покупка
                snap = self._latest_snap.get(symbol)
                # закрываемся агрессивно (навстречу лучшей цене), не post-only - это аварийный выход.
                # В paper дополнительно пересекаем спред с буфером (см. _close_price) -
                # без этого reduce-only IOC мог не исполниться, и аварийное закрытие
                # оставляло бы реальный хвост на бирже, хотя мы считали его закрытым.
                fallback = (snap.best_bid if exit_is_ask else snap.best_ask) if snap else pos.avg_entry
                price = self._close_price(snap, exit_is_ask, fallback)
                try:
                    result, _, _ = await self.exchange.place_limit_order(
                        pos.market, next_client_order_index(), size, price, exit_is_ask,
                        reduce_only=True, post_only=False,
                    )
                    avg = getattr(result, "avg_price", None) if result is not None else None
                    exit_price = avg or price
                except Exception as e:
                    log.error("[%s] не удалось закрыть позицию при kill switch: %s", symbol, e)

            # Итоговый PnL сделки = уже накопленное по прошлым частичным закрытиям
            # (TP1 и т.п., см. pos.realized_pnl) + PnL последнего закрытого куска
            # здесь. Не используем _estimate_closed_pnl.pnl напрямую - он посчитан
            # от pos.filled_size (весь исходный размер), что задвоило бы PnL для
            # позиций, у которых TP1 уже сработал до kill switch.
            if exit_price is None:
                _, exit_price = self._estimate_closed_pnl(pos)
            chunk_pnl = 0.0
            if size > 1e-9:
                direction = 1 if pos.side == "long" else -1
                chunk_pnl = (exit_price - pos.avg_entry) * direction * size
                pos.realized_pnl += chunk_pnl
            # equity двигаем только на PnL ИМЕННО этого (последнего) куска - PnL
            # прошлых частичных закрытий (TP1 и т.п.) уже учтён в equity в момент
            # их собственного register_close(); pos.realized_pnl - только для лога.
            self.risk.register_close(symbol, pos.side, chunk_pnl)
            if self.trade_log and pos.trade_id is not None:
                self.trade_log.close_trade(pos.trade_id, exit_price, pos.realized_pnl, f"kill_switch:{reason}")
            self.positions.pop(symbol, None)

        for t in self._watchers.values():
            t.cancel()
        self._watchers.clear()
        log.warning("Все позиции закрыты, новые входы заблокированы (kill switch: %s).", reason)

    async def shutdown(self):
        for t in self._watchers.values():
            t.cancel()
