"""
Менеджер ордеров - "руки" бота: открывает позицию лимитками, переставляет
неисполненную лимитку вслед за рынком (chase/reprice/cancel), а после входа
следит за позицией и постоянно пересчитывает EDGE (ту же функцию
compute_edge(), что и вход - см. signals.py), решая CLOSE/REDUCE/HOLD.

ПЕРЕСТРОЕНО 21.08 по прямому запросу пользователя вместе с остальной торговой
логикой (см. config.py/signals.py). Старая система выхода (TP1 нативным
ордером + трейлинг-стоп на остаток + THESIS INVALIDATED/WEAKENING по
ABSORPTION/BREAKOUT + opposing_wall_exit + opposite_flow_exit по исполненному
тейпу + TIME_EXIT) убрана целиком - десятки независимых порогов конфликтовали
бы с новой единой механикой. EXIT теперь работает по прямой просьбе
пользователя:

    Постоянно пересчитывать LONG EDGE / SHORT EDGE.
    Если наше преимущество исчезло -> CLOSE.
    Если преимущество уменьшилось -> REDUCE.
    Если преимущество усиливается -> HOLD / дать позиции развиваться.

CLOSE закрывает НЕМЕДЛЕННО, даже в 0 или небольшой минус, если крупная
стенка, от которой вошли, реально исчезла/сторона сменилась (пример
пользователя: LONG было BID $1.2M/ASK $200K, стало BID $200K/ASK $1.2M) -
подтверждаем EDGE_EXIT_CONFIRM_TICKS тиков подряд (защита от спуф-мигания
стенки, тот же принцип, что и на входе, только короче - см. config.py).

Жёсткий стоп-лосс (risk.build_plan, посчитан от структуры сигнала на входе)
остаётся как аварийный бэкстоп на случай, если цена улетит быстрее, чем
успеет подтвердиться разворот EDGE - НЕ основной механизм выхода.
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
from signals import Signal, compute_edge
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
    current_sl_price: float = 0.0  # аварийный бэкстоп - см. модульный докстринг
    opened_at: float = field(default_factory=time.time)
    trade_id: Optional[int] = None
    signal_type: str = ""       # всегда "wall_edge" новой механикой, оставлено ради trade_log/dashboard
    reference_price: float = 0.0  # цена EDGE-стенки на момент входа (лог/дашборд)
    realized_pnl: float = 0.0     # накапливается по частичным закрытиям (REDUCE + финал)
    # Сколько раз ПОДРЯД последняя попытка закрытия (полного или REDUCE) не
    # смогла исполниться целиком - см. _close_price/paper_cross_buffer_escalation_pct.
    close_stall_count: int = 0
    # Причина закрытия, которое УЖЕ решено сделать, но последняя попытка не
    # исполнилась целиком - следующая попытка идёт на САМОМ СЛЕДУЮЩЕМ тике с
    # той же причиной, без повторного набора подтверждений EDGE.
    pending_close_reason: Optional[str] = None
    # MFE/MAE (% от avg_entry, знак - "хорошо"/"плохо" для стороны позиции) -
    # чисто для журнала/дашборда, на решение CLOSE/REDUCE/HOLD не влияют.
    mfe_pct: float = 0.0
    mae_pct: float = 0.0
    # Сколько тиков ПОДРЯД встречная сторона книги (компare EDGE) стала
    # крупнее нашей - подтверждение перед CLOSE, см. CFG.edge_exit_confirm_ticks
    # и _watch_position. Анти-спуф: одиночное мигание стенки не успевает
    # набрать нужное число тиков подряд.
    edge_reverse_streak: int = 0
    # REDUCE срабатывает не больше одного раза за эпизод ослабления - сбрасывается,
    # как только EDGE на нашей стороне снова начинает проходить
    # WALL_ADVANTAGE_RATIO_MIN (HOLD), так что в следующем отдельном эпизоде
    # ослабления REDUCE может сработать снова.
    reduced_once: bool = False


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
        self._latest_signal_snap: Dict[str, "BookSnapshot"] = {}  # стакан-источник сигнала (Binance) - структура EDGE
        # Символы, для которых прямо сейчас идёт попытка входа (между сигналом и
        # регистрацией позиции есть await, поэтому has_position() одна не спасает
        # от гонки, если за это время прилетит ещё один сигнал по тому же символу).
        self._entering: set = set()
        # Лок на закрытие ПОЗИЦИИ по символу. Закрыть позицию могут ДВА разных
        # независимых источника одновременно: фоновый _watch_position (стоп/
        # EDGE-разворот) и (потенциально) другой путь закрытия - без лока
        # возможна гонка, где оба читают позицию как ещё открытую и оба шлют
        # закрывающий ордер / считают PnL.
        self._close_locks: Dict[str, asyncio.Lock] = {}
        # Когда символ последний раз закрывался watcher'ом (stop_loss/edge_reversed) -
        # см. CFG.reentry_cooldown_sec в handle_signal.
        self._last_close_ts: Dict[str, float] = {}

    def note_snapshot(self, snap):
        self._latest_snap[snap.symbol] = snap

    def note_signal_snapshot(self, snap):
        self._latest_signal_snap[snap.symbol] = snap

    def has_position(self, symbol: str) -> bool:
        return symbol in self.positions

    def _get_close_lock(self, symbol: str) -> asyncio.Lock:
        lock = self._close_locks.get(symbol)
        if lock is None:
            lock = asyncio.Lock()
            self._close_locks[symbol] = lock
        return lock

    async def _close_position_safely(self, pos: ManagedPosition, snap, reason: str):
        """
        Обёртка над _close_position_now с локом по символу + перепроверкой
        после его получения - см. комментарий у self._close_locks в __init__.
        Пока ждали лок, позицию мог уже закрыть параллельный вызов - если
        self.positions[symbol] больше не тот же самый объект pos, закрывать
        уже нечего.
        """
        async with self._get_close_lock(pos.symbol):
            if self.positions.get(pos.symbol) is not pos:
                return
            await self._close_position_now(pos, snap, reason)

    # ------------------------------------------------------------------ #
    # Вход в позицию с "чейзингом" лимитки (REPRICE/CANCEL)
    # ------------------------------------------------------------------ #

    async def handle_signal(self, market: MarketInfo, signal: Signal):
        if self.kill_switch and self.kill_switch.active:
            return  # аварийная остановка активна - новых входов нет
        if signal.symbol in self._entering:
            log.debug("[%s] вход уже в процессе, сигнал пропущен", signal.symbol)
            return

        # Резервируем символ СИНХРОННО (без await между проверкой выше и этой
        # строкой), ДО первого await ниже - и держим резерв на всю обработку
        # сигнала. Без этого один и тот же сигнал мог породить НЕСКОЛЬКО
        # параллельных asyncio-задач handle_signal почти одновременно - гонка,
        # которая могла задвоить открытие позиции по одному символу.
        self._entering.add(signal.symbol)
        try:
            existing = self.positions.get(signal.symbol)
            if existing is not None:
                if existing.side == signal.side:
                    log.debug("[%s] уже есть открытая позиция в ту же сторону, сигнал пропущен", signal.symbol)
                else:
                    # Встречный сигнал против открытой позиции - НЕ закрываем
                    # тут же по факту нового сигнала на вход. Разворотом уже
                    # открытой позиции управляет постоянный пересчёт EDGE в
                    # _watch_position (CLOSE/REDUCE/HOLD ниже) - именно там
                    # прямо реализовано требование пользователя "если крупная
                    # стенка исчезает и преимущество меняется на
                    # противоположное - немедленно пересчитать позицию".
                    log.debug("[%s] встречный сигнал (%s) против открытой позиции %s проигнорирован - "
                              "разворотом управляет пересчёт EDGE в _watch_position, не отдельный сигнал",
                              signal.symbol, signal.side.upper(), existing.side.upper())
                return

            # Кулдаун ТОЛЬКО для входа с нуля - см. CFG.reentry_cooldown_sec.
            last_close = self._last_close_ts.get(signal.symbol)
            if last_close is not None:
                since = time.time() - last_close
                if since < CFG.reentry_cooldown_sec:
                    log.info("[%s] вход пропущен: кулдаун после закрытия (%.0fs назад из %.0fs) - "
                              "не влезаем сразу обратно в тот же шум", signal.symbol, since,
                              CFG.reentry_cooldown_sec)
                    return

            if not self.risk.can_trade():
                return

            plan = self.risk.build_plan(signal.symbol, signal.side, signal.mid,
                                         wall_price=signal.reference_price,
                                         wall_usd=signal.wall_usd, backup_usd=signal.backup_usd,
                                         exchange_basis=signal.exchange_basis)
            if plan.size <= 0 or plan.size < market.min_base_amount:
                log.warning("[%s] расчётный размер позиции %.6f меньше минимального лота, сигнал пропущен",
                            signal.symbol, plan.size)
                return

            filled_size, avg_entry = await self._enter_with_chase(market, plan)
            if filled_size <= 0:
                log.info("[%s] вход не удался (лимитка не исполнилась за %d попыток)",
                          signal.symbol, CFG.max_reprice_attempts + 1)
                return

            # SL в plan посчитан от цены СИГНАЛА (до входа) - реальная цена
            # исполнения (avg_entry) почти всегда отличается (basis Lighter/Binance
            # + задержка между сигналом и ордером). Пересчитываем от факта, иначе
            # структура сделки считается от точки, где вход на самом деле не было.
            plan = self.risk.rebase_plan_to_fill(plan, avg_entry)

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
            log.info("[%s] ПОЗИЦИЯ ОТКРЫТА %s size=%.6f avg_entry=%.2f SL(бэкстоп)=%.2f "
                      "ratio_на_входе=%.2f", signal.symbol, plan.side.upper(), filled_size, avg_entry,
                      plan.stop_price, signal.ratio)

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
                    # цена ушла / лимитка не исполнилась за отведённое время -
                    # отменяем старую заявку и переставляем по актуальной цене
                    # (REPRICE/CANCEL, прямое требование пользователя).
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
    # Защитный SL (аварийный бэкстоп) и постоянный пересчёт EDGE
    # ------------------------------------------------------------------ #

    async def _place_protective_orders(self, pos: ManagedPosition):
        """Только SL - реduce-only ордер на бирже как резервная защита на
        случай обрыва связи с ботом (аварийный бэкстоп, см. модульный
        докстринг). Отдельного нативного TP-ордера больше нет - выход решает
        постоянный пересчёт EDGE в _watch_position, а не фиксированная цель."""
        exit_is_ask = pos.side == "long"  # закрытие long = продажа, закрытие short = покупка
        await self.exchange.create_sl_order(
            pos.market, next_client_order_index(), pos.filled_size, pos.plan.stop_price, exit_is_ask,
        )

    async def _watch_position(self, pos: ManagedPosition):
        """
        ORDER BOOK -> EDGE -> HOLD/REDUCE/EXIT на уже открытой позиции - та же
        compute_edge(), что и на входе (см. signals.py), пересчитывается
        каждый тик (CFG.position_check_interval_sec):

          - HOLD:   наша сторона всё ещё крупнее встречной И проходит
                    WALL_ADVANTAGE_RATIO_MIN - преимущество живо (или
                    усиливается) - ничего не делаем, даём позиции развиваться.
          - REDUCE: наша сторона ещё крупнее, но ratio уже не проходит порог
                    (преимущество ослабло, но ещё не развернулось) -
                    закрываем EDGE_REDUCE_FRACTION остатка, один раз за
                    эпизод ослабления.
          - CLOSE:  встречная сторона стала крупнее нашей (сама причина
                    входа исчезла/развернулась) - подтверждаем
                    EDGE_EXIT_CONFIRM_TICKS тиков подряд (анти-спуф - то же
                    требование персистентности, что и на входе, только
                    короче, см. config.py) и закрываем НЕМЕДЛЕННО по рынку,
                    даже в 0 или в небольшой минус.

        Жёсткий SL (посчитан от структуры сигнала на входе, risk.build_plan)
        проверяется КАЖДЫЙ тик первым, независимо от состояния EDGE выше -
        аварийный бэкстоп на случай, если цена улетит быстрее, чем успеет
        подтвердиться разворот EDGE, а не основной механизм выхода.
        """
        try:
            while True:
                await asyncio.sleep(CFG.position_check_interval_sec)
                snap = self._latest_snap.get(pos.symbol)  # цена исполнения (Lighter)
                if snap is None:
                    continue
                price = snap.mid

                profit_pct = ((price - pos.avg_entry) / pos.avg_entry * 100) if pos.side == "long" \
                    else ((pos.avg_entry - price) / pos.avg_entry * 100)
                if profit_pct > pos.mfe_pct:
                    pos.mfe_pct = profit_pct
                if profit_pct < pos.mae_pct:
                    pos.mae_pct = profit_pct

                # Закрытие уже решено раньше, но не исполнилось целиком - см.
                # ManagedPosition.pending_close_reason. Повторяем ТУ ЖЕ причину
                # немедленно на этом тике, без повторного набора подтверждений.
                if pos.pending_close_reason is not None:
                    await self._close_position_safely(pos, snap, pos.pending_close_reason)
                    if not self.has_position(pos.symbol):
                        return
                    continue

                hit_stop = (price <= pos.current_sl_price) if pos.side == "long" else (price >= pos.current_sl_price)
                if hit_stop:
                    pos.pending_close_reason = "stop_loss"
                    await self._close_position_safely(pos, snap, "stop_loss")
                    if not self.has_position(pos.symbol):
                        return
                    continue

                # EDGE recompute - структуру берём с того же стакана, откуда
                # пришёл вход (Binance, если включён - там реальные крупные
                # заявки; иначе Lighter).
                signal_snap = self._latest_signal_snap.get(pos.symbol) or snap
                wall_min_usd = CFG.binance_wall_min_usd if CFG.use_binance_signals else CFG.wall_min_usd
                edge = compute_edge(signal_snap, wall_min_usd)

                if edge.side == pos.side and edge.qualifies:
                    # HOLD: преимущество живо (или усиливается) - сбрасываем
                    # счётчики разворота/ослабления, ничего не делаем.
                    pos.edge_reverse_streak = 0
                    pos.reduced_once = False
                    continue

                if edge.side != pos.side:
                    # Встречная сторона стала крупнее нашей (edge.side - либо
                    # противоположная сторона, либо None, если вообще нет
                    # заявок) - потенциальный разворот. Подтверждаем
                    # EDGE_EXIT_CONFIRM_TICKS тиков подряд, прежде чем закрыть -
                    # одиночное мигание стенки (спуф) не успевает набрать
                    # нужное число тиков подряд.
                    pos.edge_reverse_streak += 1
                    log.info("[%s] EDGE против позиции (наша=%s, теперь крупнее=%s favor=%.0f oppose=%.0f "
                              "ratio=%.2f) - %d/%d тиков подряд", pos.symbol, pos.side.upper(),
                              (edge.side or "none").upper(), edge.favor_usd, edge.oppose_usd, edge.ratio,
                              pos.edge_reverse_streak, CFG.edge_exit_confirm_ticks)
                    if pos.edge_reverse_streak >= CFG.edge_exit_confirm_ticks:
                        pos.pending_close_reason = "edge_reversed"
                        log.info("[%s] CLOSE: EDGE развернулся против позиции, подтверждено %d тиков "
                                  "подряд - закрываем немедленно (профит сейчас=%.3f%%)",
                                  pos.symbol, pos.edge_reverse_streak, profit_pct)
                        await self._close_position_safely(pos, snap, "edge_reversed")
                        if not self.has_position(pos.symbol):
                            return
                        pos.edge_reverse_streak = 0
                        continue
                    continue

                # edge.side == pos.side, но edge.qualifies == False - наша
                # сторона всё ещё крупнее встречной, но реальное преимущество
                # (ratio/размер) уже недостаточно - REDUCE, не полный CLOSE,
                # один раз за эпизод ослабления (см. pos.reduced_once).
                pos.edge_reverse_streak = 0
                if not pos.reduced_once and pos.filled_size > 0:
                    reduce_size = pos.filled_size * CFG.edge_reduce_fraction
                    tick = 10 ** (-pos.market.size_decimals)
                    if reduce_size > 0 and round(reduce_size, pos.market.size_decimals) >= tick:
                        log.info("[%s] REDUCE: EDGE ослаб (favor=%.0f oppose=%.0f ratio=%.2f < порог %.2f) "
                                  "- закрываем %.0f%% остатка", pos.symbol, edge.favor_usd, edge.oppose_usd,
                                  edge.ratio, CFG.wall_advantage_ratio_min, CFG.edge_reduce_fraction * 100)
                        await self._partial_close(pos, snap, reduce_size, "edge_reduce")
                        pos.reduced_once = True
                        if not self.has_position(pos.symbol):
                            return

        except asyncio.CancelledError:
            return

    def _close_price(self, snap, exit_is_ask: bool, fallback_price: float, stall_count: int = 0) -> float:
        """
        Цена для reduce-only закрывающего ордера. В paper-режиме PaperClient
        исполняет только IOC, пересекающие спред (см. _enter_with_chase) - пассивная
        цена (голый best_bid/best_ask) часто НЕ пересекает книгу гарантированно
        (округления/рассинхрон снепшота), и ордер исполняется частично или вообще
        не исполняется. Без буфера позиция могла считаться закрытой (мы её
        убирали из self.positions), а на самом paper-аккаунте оставался
        непогашенный остаток, который затем складывался со следующим входом.
        В live-режиме буфер не нужен - там реальная книга и другой механизм
        исполнения.

        stall_count - см. pos.close_stall_count. Базовый буфер гарантирует
        пересечение СПРЕДА, но не гарантирует пересечение достаточной ГЛУБИНЫ
        книги, если объём позиции больше того, что стоит на самой верхушке.
        Эскалируем буфер с каждой неудачной попыткой (шире буфер = глубже
        внутрь книги), чтобы закрытие в итоге гарантированно прошло целиком,
        вместо неопределённо долгого зависания.
        """
        if CFG.mode != "paper" or snap is None:
            return fallback_price
        cross_price = snap.best_bid if exit_is_ask else snap.best_ask
        buf_pct = min(
            CFG.paper_cross_buffer_pct + stall_count * CFG.paper_cross_buffer_escalation_pct,
            CFG.paper_cross_buffer_max_pct,
        )
        buf = cross_price * buf_pct / 100
        return (cross_price - buf) if exit_is_ask else (cross_price + buf)

    async def _partial_close(self, pos: ManagedPosition, snap, size: float, reason: str) -> float:
        """Возвращает фактически закрытый размер (0.0, если не исполнилось совсем) -
        вызывающий код (REDUCE в _watch_position) использует это, чтобы не
        считать частичный/нулевой филл полным уменьшением позиции."""
        exit_is_ask = pos.side == "long"  # закрытие long = продажа, short = покупка
        fallback = (snap.best_bid if exit_is_ask else snap.best_ask) if snap else pos.avg_entry
        price = self._close_price(snap, exit_is_ask, fallback, pos.close_stall_count)
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
            return 0.0

        if filled <= 1e-9:
            pos.close_stall_count += 1
            log.warning("[%s] частичное закрытие (%s): ордер не исполнился (0 из %.6f) - позиция НЕ уменьшена "
                        "(попытка %d подряд, буфер эскалирован)", pos.symbol, reason, size, pos.close_stall_count)
            return 0.0
        if filled < size - 1e-9:
            pos.close_stall_count += 1
            log.warning("[%s] частичное закрытие (%s): исполнилось только %.6f из запрошенных %.6f "
                        "(попытка %d подряд, буфер эскалирован)",
                        pos.symbol, reason, filled, size, pos.close_stall_count)
        else:
            pos.close_stall_count = 0
        size = filled

        direction = 1 if pos.side == "long" else -1
        pnl = (exit_price - pos.avg_entry) * direction * size
        pos.realized_pnl += pnl
        pos.filled_size -= size
        self.risk.register_close(pos.symbol, pos.side, pnl)
        log.info("[%s] %s: закрыто %.6f по %.2f | PnL этой части=%.2f", pos.symbol, reason, size, exit_price, pnl)
        return size

    async def _close_position_now(self, pos: ManagedPosition, snap, reason: str):
        try:
            live_pos = await self.exchange.get_position(pos.market)
        except Exception:
            live_pos = None
        current_size = abs(live_pos["size"]) if live_pos else 0.0

        exit_is_ask = pos.side == "long"
        fallback = (snap.best_bid if exit_is_ask else snap.best_ask) if snap else pos.avg_entry
        price = self._close_price(snap, exit_is_ask, fallback, pos.close_stall_count)
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
            pos.close_stall_count += 1
            escalated_buf_pct = min(
                CFG.paper_cross_buffer_pct + pos.close_stall_count * CFG.paper_cross_buffer_escalation_pct,
                CFG.paper_cross_buffer_max_pct,
            )
            log.warning("[%s] закрытие позиции (%s): исполнилось %.6f из %.6f, остаток %.6f - "
                        "позиция остаётся под наблюдением, повторим на следующей проверке "
                        "(попытка %d подряд, буфер эскалирован до %.3f%%)",
                        pos.symbol, reason, closed_size, current_size, remaining,
                        pos.close_stall_count, escalated_buf_pct)
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
            self.trade_log.close_trade(pos.trade_id, exit_price, pos.realized_pnl, reason,
                                        mfe_pct=pos.mfe_pct, mae_pct=pos.mae_pct)
        self.positions.pop(pos.symbol, None)
        if reason in ("stop_loss", "edge_reversed"):
            self._last_close_ts[pos.symbol] = time.time()
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

        # Останавливаем фоновых watcher'ов ДО закрытия - иначе _watch_position
        # может в этот самый момент тикать по стопу/EDGE-развороту на той же
        # позиции и гонка с закрытием ниже (см. self._close_locks в __init__ -
        # тот же принцип, здесь дополнительно глушим watcher'ов заранее).
        for t in self._watchers.values():
            t.cancel()
        self._watchers.clear()

        for symbol, pos in list(self.positions.items()):
            async with self._get_close_lock(symbol):
                # Пока ждали лок, позицию мог уже закрыть параллельный вызов -
                # перепроверяем.
                if self.positions.get(symbol) is not pos:
                    continue
                await self._flatten_one(symbol, pos, reason)

        log.warning("Все позиции закрыты, новые входы заблокированы (kill switch: %s).", reason)

    async def _flatten_one(self, symbol: str, pos: ManagedPosition, reason: str):
        try:
            live_pos = await self.exchange.get_position(pos.market)
        except Exception:
            live_pos = None
        size = abs(live_pos["size"]) if live_pos else pos.filled_size

        exit_price = None
        if size > 1e-9:
            exit_is_ask = pos.side == "long"  # закрытие long = продажа, закрытие short = покупка
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
        # (REDUCE и т.п., см. pos.realized_pnl) + PnL последнего закрытого
        # куска здесь. Не используем _estimate_closed_pnl.pnl напрямую - он
        # посчитан от pos.filled_size (весь исходный размер), что задвоило бы
        # PnL для позиций, у которых REDUCE уже сработал до kill switch.
        if exit_price is None:
            _, exit_price = self._estimate_closed_pnl(pos)
        chunk_pnl = 0.0
        if size > 1e-9:
            direction = 1 if pos.side == "long" else -1
            chunk_pnl = (exit_price - pos.avg_entry) * direction * size
            pos.realized_pnl += chunk_pnl
        # equity двигаем только на PnL ИМЕННО этого (последнего) куска - PnL
        # прошлых частичных закрытий (REDUCE и т.п.) уже учтён в equity в момент
        # их собственного register_close(); pos.realized_pnl - только для лога.
        self.risk.register_close(symbol, pos.side, chunk_pnl)
        if self.trade_log and pos.trade_id is not None:
            self.trade_log.close_trade(pos.trade_id, exit_price, pos.realized_pnl, f"kill_switch:{reason}",
                                        mfe_pct=pos.mfe_pct, mae_pct=pos.mae_pct)
        self.positions.pop(symbol, None)

    # ------------------------------------------------------------------ #
    # Сброс счёта бота с дашборда - "начать заново" без передеплоя на Render
    # ------------------------------------------------------------------ #

    async def reset_account(self, reason: str = "manual (dashboard)"):
        """
        Полный сброс "счёта" бота по кнопке с дашборда - баланс, открытые
        позиции и вся история сделок обнуляются, как при холодном старте
        процесса, но без самого рестарта (значит, без разрыва WS Lighter/
        Binance и простоя на пересборку - см. фикс переподключения в
        market_data.py). Доступно ТОЛЬКО в paper-режиме (см. проверку ниже и
        в Dashboard.handle_reset_account) - у live своя реальная биржа со
        своими реальными деньгами, "сбросить баланс" там не бывает.

        Порядок важен:
          1. Сначала принудительно закрываем все открытые позиции (тот же
             путь, что и kill switch - лок по символу + перепроверка, см.
             flatten_all/_flatten_one) - нельзя обнулять equity/историю, пока
             на "бирже" ещё висит реальный (пусть и виртуальный) риск.
          2. Пересоздаём PaperClient - у него собственный внутренний баланс,
             отдельный от RiskManager.equity, простым обнулением одних чисел
             в этом классе paper-счёт не сбросить (см. exchange_client.py).
          3. Обнуляем RiskManager (equity, дневной старт, серия убытков) и
             TradeLog (история сделок, на которой считается win-rate/график
             на дашборде).
          4. Если был активен kill switch - снимаем, "чистый лист" не должен
             начинаться с заблокированных входов.
        """
        if CFG.mode != "paper":
            log.warning("Сброс счёта запрошен (%s), но MODE=%s - доступно только в paper", reason, CFG.mode)
            return

        for t in self._watchers.values():
            t.cancel()
        self._watchers.clear()

        for symbol, pos in list(self.positions.items()):
            async with self._get_close_lock(symbol):
                if self.positions.get(symbol) is not pos:
                    continue
                await self._flatten_one(symbol, pos, reason)

        await self.exchange.reset_paper_account()

        self.risk.reset()
        if self.trade_log:
            self.trade_log.reset()

        self._last_close_ts.clear()
        self._entering.clear()

        if self.kill_switch and self.kill_switch.active:
            self.kill_switch.reset()

        log.warning("СЧЁТ СБРОШЕН (%s): баланс -> $%.2f, позиции и история сделок обнулены.",
                    reason, CFG.account_equity_usd)

    async def shutdown(self):
        for t in self._watchers.values():
            t.cancel()
