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
    # Счётчик подряд идущих проверок, где _thesis_invalidated вернул True - см.
    # _watch_position: закрываем не по первому же срабатыванию (это может быть
    # шум в стакане на долю секунды), а только после INVALIDATION_CONFIRM_TICKS
    # подтверждений подряд.
    invalidation_streak: int = 0
    # Волатильность рынка на момент входа (Signal.volatility_pct) - используется
    # для масштабирования opposing_wall_min_profit_pct под текущий режим рынка
    # (см. _opposing_wall_exit) вместо одного фиксированного % на все случаи.
    entry_volatility_pct: float = 0.0
    # Сколько раз ПОДРЯД последняя попытка закрытия (полного или TP1) не смогла
    # исполниться целиком - см. _close_price/paper_cross_buffer_escalation_pct.
    # Найдено в проде 18.08: без эскалации буфера позиция может "зависать" в
    # закрытии на много минут, если верхушка книги тоньше размера позиции.
    close_stall_count: int = 0
    # Сколько из pos.plan.tp1_size уже реально закрыто по TP1 - раньше
    # pos.tp1_done ставился в True сразу после ОДНОЙ попытки _partial_close,
    # даже если она исполнилась частично или вообще на 0 (см. коммент у
    # _watch_position) - тихо теряли профит-тейк на тонком стакане. Теперь
    # TP1 считается выполненным только когда реально закрыт весь tp1_size.
    tp1_filled: float = 0.0
    # Причина закрытия, которое УЖЕ решено сделать, но последняя попытка не
    # исполнилась целиком (см. close_stall_count) - см. _watch_position.
    # Найдено в проде 18.08 (ETH): после неудачной попытки закрытия по
    # structure_invalidated invalidation_streak сбрасывался в 0, и следующая
    # попытка закрытия ждала, пока тезис заново "развалится" 2 тика подряд -
    # на практике это растягивало интервал между попытками закрытия до
    # ~17-19 секунд вместо штатной 1 секунды (POSITION_CHECK_INTERVAL_SEC),
    # хотя решение закрыться уже было принято и отменять его никто не
    # собирался. За это время цена продолжала уходить против уже
    # "приговорённой" позиции - итоговый убыток (-6.35 USD на ETH) получился
    # заметно больше, чем должен был дать сам стоп. Теперь если закрытие уже
    # начато, но не исполнилось целиком - следующая попытка идёт на САМОМ
    # СЛЕДУЮЩЕМ тике с той же причиной, без повторного набора подтверждений.
    pending_close_reason: Optional[str] = None
    # MFE/MAE (max favorable / max adverse excursion, % от avg_entry, знак -
    # "хорошо"/"плохо" для стороны позиции, не сырой ценовой знак) за весь
    # срок жизни сделки - добавлено 18.08 (аудит стратегии, этап 1.1). Нужно,
    # чтобы отличить "стоп сработал ровно там, где сделка объективно должна
    # была закрыться" от "сделка была в плюсе X%, но выход упустил момент и
    # закрыл хуже" - раньше TradeLog хранил только итоговый PnL, без этого
    # измерить упущенный потенциал выхода было нельзя (см. AUDIT_2026-08-18.md).
    mfe_pct: float = 0.0
    mae_pct: float = 0.0
    # THESIS VALID/WEAKENING/INVALIDATED (аудит стратегии, этап 5, 18.08) -
    # WEAKENING - новое промежуточное состояние между "всё ок" и "закрываем
    # всё" (см. _watch_position): позиция уже была заметно в плюсе (MFE), но
    # заметная часть этого пути откатилась назад, хотя формальная структура
    # ещё не сломана (INVALIDATED, см. _thesis_invalidated - логика ТАМ не
    # меняется). При WEAKENING - частичный выход (фиксируем часть уже
    # заработанного), не более одного раза за сделку.
    thesis_state: str = "VALID"
    weakening_partial_done: bool = False
    # Счётчик подряд идущих тиков, где сравнение стенок в _thesis_invalidated
    # (nearest_blocking >= nearest_holding * WALL_DOMINANCE_RATIO) было верным -
    # ОТДЕЛЬНЫЙ от invalidation_streak. Найдено в проде 18.08 вечером: именно
    # эта проверка мгновенно (за 1 тик) хлопала structure_invalidated на
    # обычном дёрганье стенок в стакане, а общий invalidation_streak (всего
    # 2 тика) не успевал это отфильтровать - серия 69 сделок за 26 минут по
    # 8-40 сек, PnL -13.59. См. WALL_DOMINANCE_CONFIRM_TICKS в config.py.
    wall_dominance_streak: int = 0
    # Те же отдельные счётчики для ДВУХ других веток _thesis_invalidated
    # (ABSORPTION) - "стенку съели, цена стоит на месте" и "дисбаланс
    # развернулся". Найдено в проде 18.08 вечером: фикс одного только
    # wall_dominance_streak не изменил картину (49 из 60 сделок всё ещё
    # structure_invalidated за 13-90 сек) - значит основной шум шёл через
    # эти две ветки, которые до этого закрывали мгновенно, за 1 тик.
    wall_eaten_streak: int = 0
    # Счётчик подряд идущих тиков, где _opposite_flow_exit увидел решительно
    # доминирующий встречный исполненный поток (19.08, финальный этап
    # рестройки) - тот же debounce-принцип, что и у wall_dominance_streak/
    # wall_eaten_streak выше, но со своим порогом (opposite_flow_confirm_ticks),
    # заменяет удалённую imbalance_streak-проверку (см. _thesis_invalidated).
    opposite_flow_streak: int = 0
    # Размер стенки, от которой реально вошли в сделку (Signal.wall_usd на
    # момент входа) - используется в новой версии dominance-проверки выше
    # ("стенка просела относительно СВОЕГО же размера на входе"), а не
    # сравнивается с каким-то фиксированным числом на все сделки сразу.
    entry_wall_usd: float = 0.0
    # Последний РЕАЛЬНО увиденный размер держащей/встречной стенки и когда
    # именно (snap.ts) - см. CFG.wall_presence_grace_sec и комментарий в
    # _thesis_invalidated. Сырой снепшот Binance partial-depth (топ-20
    # уровней) может на одном тике не содержать стенку, которая на бирже
    # никуда не делась - без этого кэша такой тик читался бы как "стенки
    # нет" (0 USD) вместо её последнего известного размера.
    last_blocking_wall_usd: float = 0.0
    last_blocking_wall_ts: float = 0.0
    last_holding_wall_usd: float = 0.0
    last_holding_wall_ts: float = 0.0


class OrderManager:
    def __init__(self, exchange: ExchangeClient, market_data: MarketData, risk: RiskManager,
                 trade_log: Optional[TradeLog] = None, kill_switch=None, trade_feed=None):
        self.exchange = exchange
        self.md = market_data
        self.risk = risk
        self.trade_log = trade_log
        self.kill_switch = kill_switch
        # BinanceTradeFeed - реальный исполненный тейп (см. binance_trades.py),
        # передаётся из bot.py. Используется в _opposite_flow_exit (19.08,
        # финальный этап рестройки) - в отличие от signals.py, где тейп решает
        # ВХОДИТЬ ли, тут он решает, не развернулся ли реальный поток ПРОТИВ
        # уже открытой позиции. None, если Binance-сигналы выключены.
        self.trade_feed = trade_feed
        self.positions: Dict[str, ManagedPosition] = {}  # symbol -> position
        self._watchers: Dict[str, asyncio.Task] = {}
        self._latest_snap: Dict[str, "BookSnapshot"] = {}  # стакан Lighter - цена исполнения
        self._latest_signal_snap: Dict[str, "BookSnapshot"] = {}  # стакан-источник сигнала (Binance) - структура
        # Символы, для которых прямо сейчас идёт попытка входа (между сигналом и
        # регистрацией позиции есть await, поэтому has_position() одна не спасает
        # от гонки, если за это время прилетит ещё один сигнал по тому же символу).
        self._entering: set = set()
        # Лок на закрытие ПОЗИЦИИ по символу. Закрыть позицию могут ДВА разных
        # независимых источника одновременно: фоновый _watch_position (стоп/
        # тейк/встречная стенка/развал структуры) и handle_signal (немедленное
        # закрытие при развороте сигнала). Оба вызывают _close_position_now по
        # своему расписанию - без лока возможна гонка: оба читают позицию как
        # ещё открытую (ни один не успел её убрать из self.positions), оба
        # шлют закрывающий ордер и оба считают PnL/пишут в trade_log - на
        # практике это задвоило бы закрытие одной и той же сделки.
        self._close_locks: Dict[str, asyncio.Lock] = {}
        # Когда символ последний раз закрывался watcher'ом (stop_loss/opposing_wall/
        # structure_invalidated) - см. CFG.reentry_cooldown_sec в handle_signal.
        # НЕ трогаем при reversal_signal - это отдельный, намеренный разворот.
        self._last_close_ts: Dict[str, float] = {}
        # Когда символ последний раз РАЗВОРАЧИВАЛСЯ (сигнал против открытой
        # позиции) - см. CFG.reversal_cooldown_sec в handle_signal. Отдельно
        # от _last_close_ts выше: тот кулдаун не действует на развороты
        # вообще, а этот действует ТОЛЬКО на них - защита от серии мгновенных
        # флипов подряд в боковике (найдено в проде 18.08).
        self._last_reversal_ts: Dict[str, float] = {}

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
        Пока ждали лок, позицию мог уже закрыть параллельный вызов (из
        _watch_position или из handle_signal) - если self.positions[symbol]
        больше не тот же самый объект pos, закрывать уже нечего.
        """
        async with self._get_close_lock(pos.symbol):
            if self.positions.get(pos.symbol) is not pos:
                return
            await self._close_position_now(pos, snap, reason)

    # ------------------------------------------------------------------ #
    # Вход в позицию с "чейзингом" лимитки
    # ------------------------------------------------------------------ #

    async def handle_signal(self, market: MarketInfo, signal: Signal):
        if self.kill_switch and self.kill_switch.active:
            return  # аварийная остановка активна - новых входов нет
        if signal.symbol in self._entering:
            log.debug("[%s] вход уже в процессе, сигнал пропущен", signal.symbol)
            return

        # Резервируем символ СИНХРОННО (без await между проверкой выше и этой
        # строкой), ДО первого await ниже - и держим резерв на всю обработку
        # сигнала, включая ветку разворота, а не только вход в новую позицию.
        # Раньше резервирование стояло только перед входом в НОВУЮ позицию (см.
        # ниже) - ветка разворота (закрыть текущую и тут же открыть
        # противоположную) не была защищена вообще. В проде (см. лог-спам,
        # разобранный в signals.py) один и тот же сигнал мог порождать
        # НЕСКОЛЬКО параллельных asyncio-задач handle_signal почти одновременно
        # - каждая из них видела ОДНУ И ТУ ЖЕ открытую позицию (ещё не
        # закрытую, т.к. закрытие через await), и каждая пыталась независимо
        # её закрыть/переоткрыть - гонка, которая могла задвоить закрытие
        # (дважды посчитанный PnL в trade_log/risk) и/или открытие новой
        # позиции. Резерв на весь вызов полностью сериализует обработку
        # сигналов по одному символу - вторая параллельная задача теперь всегда
        # уходит через проверку self._entering выше, ДО того как коснётся
        # self.positions.
        self._entering.add(signal.symbol)
        try:
            existing = self.positions.get(signal.symbol)
            if existing is not None:
                if existing.side == signal.side:
                    log.debug("[%s] уже есть открытая позиция в ту же сторону, сигнал пропущен", signal.symbol)
                    return
                # ОТКЛЮЧЕНО 19.08 по прямой просьбе пользователя ("reversal_signal
                # тоже убирай") - после включения wall-flip dominance логики
                # (19.08, см. _thesis_invalidated) и живых наблюдений выяснилось,
                # что мгновенный разворот по СВЕЖЕМУ встречному сигналу тоже даёт
                # плохие закрытия - отдельный от dominance путь, не завязанный на
                # реальную структуру СВОЕЙ стенки, а просто на факт нового сигнала.
                # Раньше (до 18.08, см. история ниже) такой сигнал молча
                # отбрасывался - возвращаемся к этому: позицию теперь закрывает
                # ТОЛЬКО штатный "умный" выход (wall-flip dominance / стоп / тейк /
                # time_exit) в _watch_position, реагирующий на РЕАЛЬНЫЙ разворот
                # доминирования у своей же стенки, а не на любой новый сигнал.
                # Если понадобится вернуть - меняем `if False:` ниже на `if True:`.
                if False:
                    # Свежий сигнал ПРОТИВ открытой позиции (новая стенка/пробой с
                    # противоположной стороны) - это не шум, а реальный сдвиг структуры
                    # рынка. Раньше такой сигнал молча отбрасывался (has_position() ->
                    # return), и позиция ждала своих штатных условий выхода (стоп/тейк/
                    # thesis_invalidated с INVALIDATION_CONFIRM_TICKS подтверждениями) -
                    # жалоба пользователя: "после лонга надо сразу шортить, а оно стоит,
                    # потом цена падает и закрывает в минус". Закрываем НЕМЕДЛЕННО по
                    # рынку, без debounce (как и _opposing_wall_exit) - раз структура
                    # уже развернулась, ждать нет смысла, только отдаём движение.
                    # Кулдаун на ПОВТОРНЫЙ разворот - см. CFG.reversal_cooldown_sec и
                    # комментарий у self._last_reversal_ts в __init__. Первый разворот
                    # всегда мгновенный (как и раньше) - ограничиваем только частоту
                    # повторных, чтобы не флипаться туда-сюда в боковике.
                    last_reversal = self._last_reversal_ts.get(signal.symbol)
                    if last_reversal is not None:
                        since_reversal = time.time() - last_reversal
                        if since_reversal < CFG.reversal_cooldown_sec:
                            log.info("[%s] разворот пропущен: кулдаун после предыдущего разворота "
                                      "(%.0fs назад из %.0fs) - позиция %s остаётся как есть",
                                      signal.symbol, since_reversal, CFG.reversal_cooldown_sec,
                                      existing.side.upper())
                            return

                    log.info("[%s] РАЗВОРОТ: сигнал %s (%s) против открытой позиции %s - закрываем немедленно",
                              signal.symbol, signal.side.upper(), signal.signal_type, existing.side.upper())
                    self._last_reversal_ts[signal.symbol] = time.time()
                    snap = self._latest_snap.get(signal.symbol)
                    existing.pending_close_reason = "reversal_signal"
                    await self._close_position_safely(existing, snap, "reversal_signal")
                    if self.has_position(signal.symbol):
                        # Закрытие исполнилось не полностью (paper: не пересекло спред) -
                        # остаток всё ещё числится открытым, новую позицию поверх не
                        # открываем. pending_close_reason выставлен выше - следующий тик
                        # _watch_position реально дозакроет хвост той же причиной, а не
                        # будет заново ждать штатных условий выхода (см. комментарий у
                        # ManagedPosition.pending_close_reason).
                        return
                    watcher = self._watchers.pop(signal.symbol, None)
                    if watcher:
                        watcher.cancel()
                    # Ниже продолжаем этим же вызовом на вход в новую (развернутую)
                    # сторону - не ждём следующего независимого срабатывания сигнала,
                    # это и есть "сразу шортить", а не через один-два цикла проверки.
                else:
                    log.debug("[%s] встречный сигнал против открытой позиции %s проигнорирован "
                              "(reversal_signal отключён 19.08) - позицию закрывает только "
                              "штатный умный выход", signal.symbol, existing.side.upper())
                    return
            else:
                # Кулдаун ТОЛЬКО для входа с нуля (existing был None) - см.
                # CFG.reentry_cooldown_sec. Разворот выше (existing is not None)
                # намеренно НЕ проверяется - это уже принятое решение "прямо
                # сейчас развернуться", а не повторный вход после паузы.
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

            # SL/TP1 в plan посчитаны от цены СИГНАЛА (до входа) - реальная цена
            # исполнения (avg_entry) почти всегда отличается (basis Lighter/Binance
            # + задержка между сигналом и ордером). Пересчитываем от факта, иначе
            # структура сделки считается от точки, где вход на самом деле не было.
            plan = self.risk.rebase_plan_to_fill(plan, avg_entry)

            pos = ManagedPosition(
                symbol=signal.symbol, side=plan.side, market=market, plan=plan,
                filled_size=filled_size, avg_entry=avg_entry, current_sl_price=plan.stop_price,
                signal_type=signal.signal_type, reference_price=signal.reference_price,
                entry_volatility_pct=signal.volatility_pct, entry_wall_usd=signal.wall_usd,
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

                # profit_pct от РЕАЛЬНОЙ цены исполнения (Lighter, тот же
                # стакан, где avg_entry) - считаем один раз за тик и переиспользуем
                # ниже (раньше пересчитывался отдельно ближе к концу функции
                # только для wall_eaten_flat_pct - см. коммент там). Также отсюда
                # обновляем MFE/MAE КАЖДЫЙ тик, до любых веток закрытия/continue
                # ниже - иначе тик, на котором сработало закрытие, выпал бы из
                # истории экскурсии (этап 1.1 аудита, см. ManagedPosition.mfe_pct).
                profit_pct = ((price - pos.avg_entry) / pos.avg_entry * 100) if pos.side == "long" \
                    else ((pos.avg_entry - price) / pos.avg_entry * 100)
                if profit_pct > pos.mfe_pct:
                    pos.mfe_pct = profit_pct
                if profit_pct < pos.mae_pct:
                    pos.mae_pct = profit_pct

                # Закрытие уже решено раньше, но не исполнилось целиком - см.
                # ManagedPosition.pending_close_reason. Повторяем ТУ ЖЕ причину
                # немедленно на этом тике, а не заново проверяем стоп/тейк/
                # тезис с нуля - решение закрыться уже принято, тут только
                # добиваем исполнение (буфер пересечения спреда сам
                # эскалируется в _close_price с каждой попыткой).
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
                    # закрывающий ордер исполнился не полностью (paper: не пересёк
                    # спред) - позиция всё ещё числится открытой, пробуем закрыть
                    # остаток на следующем тике вместо того чтобы бросить слежение.
                    continue

                if not pos.tp1_done:
                    hit_tp1 = (price >= pos.plan.tp1_price) if pos.side == "long" else (price <= pos.plan.tp1_price)
                    if hit_tp1:
                        # Раньше tp1_done ставился в True сразу после ОДНОЙ попытки,
                        # даже если она исполнилась частично/на 0 (тонкий стакан) -
                        # тихо теряли часть профит-тейка навсегда. Теперь запрашиваем
                        # только незакрытый остаток tp1_size и подтверждаем TP1
                        # только когда он реально закрыт целиком (см. pos.tp1_filled).
                        # min(..., pos.filled_size) - добавлено 18.08 (этап 5 аудита):
                        # THESIS WEAKENING (см. выше) мог частично закрыть позицию ДО
                        # TP1, оставшийся фактический размер тогда меньше, чем
                        # изначально запланированный tp1_size - без каппинга запрос
                        # на закрытие мог бы попытаться закрыть больше, чем реально
                        # осталось от позиции.
                        remaining_tp1 = min(pos.plan.tp1_size - pos.tp1_filled, pos.filled_size)
                        # НАЙДЕНО В ПРОДЕ 19.08 - реальный баг, не гипотеза: ETH-сделка
                        # застряла в бесконечном цикле на ~95 секунд (11:20:00-11:21:35),
                        # каждую секунду повторяя "TP1 исполнился частично (0.785100 из
                        # 0.785137)" -> _partial_close с remaining_tp1=0.000037 ->
                        # PaperClient.validate_order бросает "base amount must be
                        # positive, got 0.0" (0.000037 округляется в 0 по
                        # market.size_decimals) -> filled_now=0 -> remaining_tp1 не
                        # меняется -> следующий тик та же ошибка, и так до бесконечности
                        # (пока позицию не закроет что-то ДРУГОЕ - стоп/dominance/
                        # time_exit). Ровно та же "пыльная хвостовая" проблема, что уже
                        # решена в _enter_with_chase (см. tick/round там) - там она
                        # обрабатывается, тут нет. Добавляем тот же tick-порог.
                        tick = 10 ** (-pos.market.size_decimals)
                        is_dust = remaining_tp1 <= 1e-9 or round(remaining_tp1, pos.market.size_decimals) < tick
                        if is_dust:
                            # WEAKENING уже закрыл не меньше, чем предполагал план TP1
                            # (или остаток - "пыль", которую биржа всё равно не примет) -
                            # по факту план TP1 считаем выполненным остатком позиции.
                            pos.tp1_done = True
                            pos.trailing_active = True
                            pos.trailing_extreme = price
                            log.info("[%s] TP1 пропущен (WEAKENING уже закрыл достаточно, либо остаток "
                                      "%.8f - пыль ниже размера лота) - остаток переведён на трейлинг-стоп %.2f%%",
                                      pos.symbol, remaining_tp1, pos.plan.trailing_stop_pct)
                        else:
                            filled_now = await self._partial_close(pos, snap, remaining_tp1, "tp1")
                            pos.tp1_filled += filled_now
                            if pos.tp1_filled >= pos.plan.tp1_size - 1e-6 or pos.filled_size <= 1e-9:
                                pos.tp1_done = True
                                pos.trailing_active = True
                                pos.trailing_extreme = price
                                log.info("[%s] TP1 сработал полностью, остаток переведён на трейлинг-стоп %.2f%%",
                                          pos.symbol, pos.plan.trailing_stop_pct)
                            else:
                                log.warning("[%s] TP1 исполнился частично (%.6f из %.6f) - повторим остаток "
                                            "на следующей проверке", pos.symbol, pos.tp1_filled, pos.plan.tp1_size)

                if pos.trailing_active:
                    await self._update_trailing_stop(pos, price)

                # структура сигнала оцениваем по тому же стакану, откуда пришёл
                # сигнал (Binance, если включён - там реальные стенки/дисбаланс)
                signal_snap = self._latest_signal_snap.get(pos.symbol) or snap

                # Встречная стенка закрывает НЕМЕДЛЕННО, без confirm-tick debounce -
                # если уже сработало (порог по прибыли внутри _opposing_wall_exit
                # отсекает шум/безубыток), ждать нельзя: за 2 тика (~2с) цена успевает
                # откатить обратно и прибыль превращается в убыток (см. коммент к
                # OPPOSING_WALL_MIN_PROFIT_PCT в config.py - именно так и было в проде).
                if self._opposing_wall_exit(pos, signal_snap, snap):
                    pos.pending_close_reason = "opposing_wall"
                    await self._close_position_safely(pos, snap, "opposing_wall")
                    if not self.has_position(pos.symbol):
                        return
                    pos.invalidation_streak = 0
                    continue

                # OPPOSITE-FLOW EXIT (19.08, финальный этап рестройки стратегии) -
                # новый тип "умного" выхода из явного списка требований
                # пользователя ("opposite-flow exit"), отдельный и от
                # THESIS INVALIDATED (структура своей стенки сломалась), и от
                # THESIS WEAKENING (цена откатила от MFE). Смотрит на РЕАЛЬНЫЙ
                # исполненный поток (BinanceTradeFeed) рядом с ТЕКУЩЕЙ ценой,
                # а не у стенки входа - если агрессивный объём в последних
                # секундах явно течёт ПРОТИВ позиции, это опережающий признак
                # того, что edge, на котором строился вход (order flow), уже
                # исчез, даже если формальная структура ещё не успела
                # развалиться. Ждём thesis_grace_period_sec (тот же грейс, что
                # и у _thesis_invalidated) - сразу после входа обычный шум
                # тейпа не должен закрывать свежую позицию.
                if time.time() - pos.opened_at >= CFG.thesis_grace_period_sec and \
                        self._opposite_flow_exit(pos, snap):
                    pos.opposite_flow_streak += 1
                else:
                    pos.opposite_flow_streak = 0
                if pos.opposite_flow_streak >= CFG.opposite_flow_confirm_ticks:
                    pos.pending_close_reason = "opposite_flow"
                    await self._close_position_safely(pos, snap, "opposite_flow")
                    if not self.has_position(pos.symbol):
                        return
                    pos.opposite_flow_streak = 0
                    pos.invalidation_streak = 0
                    continue

                # TIME_EXIT (этап 2.3, уточнено в этапе 5 аудита 18.08). Если
                # сделка висит дольше эффективного лимита и так и не сдвинулась
                # в нашу пользу дальше TIME_EXIT_MIN_PROFIT_PCT - тезис явно не
                # отрабатывает так, как рассчитано, закрываем вместо
                # неопределённо долгого ожидания стопа. Лимит теперь НЕ один и
                # тот же для всех сделок:
                #  - ABSORPTION - это ставка на быстрый micro-scalp импульс, у
                #    неё короткий базовый лимит (TIME_EXIT_SEC);
                #  - BREAKOUT - продолжение тренда, ему законно нужно больше
                #    времени на разгон (TIME_EXIT_BREAKOUT_SEC, заметно выше);
                # и масштабируется текущей волатильностью НА ВХОДЕ (см.
                # Signal.volatility_pct/pos.entry_volatility_pct) относительно
                # референса TIME_EXIT_VOL_REF_PCT - выше волатильность, чем
                # референс, короче окно (быстрее должно стать понятно, сработал
                # ли тезис), ниже волатильность - окно шире, зажато в
                # [TIME_EXIT_MIN_SEC, TIME_EXIT_MAX_SEC].
                base_time_exit_sec = CFG.time_exit_sec if pos.signal_type == "absorption" \
                    else CFG.time_exit_breakout_sec
                vol_scale = 1.0
                if pos.entry_volatility_pct > 0:
                    vol_scale = min(max(CFG.time_exit_vol_ref_pct / pos.entry_volatility_pct, 0.5), 2.0)
                effective_time_exit_sec = min(
                    max(base_time_exit_sec * vol_scale, CFG.time_exit_min_sec), CFG.time_exit_max_sec)

                elapsed = time.time() - pos.opened_at
                if elapsed >= effective_time_exit_sec and profit_pct < CFG.time_exit_min_profit_pct:
                    pos.pending_close_reason = "time_exit"
                    await self._close_position_safely(pos, snap, "time_exit")
                    if not self.has_position(pos.symbol):
                        return
                    pos.invalidation_streak = 0
                    continue

                thesis_invalid = (
                    time.time() - pos.opened_at >= CFG.thesis_grace_period_sec and
                    self._thesis_invalidated(pos, signal_snap, snap)
                )

                # БЫЛО (до 18.08 поздно вечером): не резали уже прибыльную
                # сделку по этой проверке, потому что раньше _thesis_invalidated
                # реагировала на обычное шумное дёрганье стенок (голый
                # WALL_DOMINANCE_RATIO=1.0 - любой перевес хоть на цент). Теперь
                # dominance-проверка ПЕРЕПИСАНА (см. _thesis_invalidated) под
                # прямую просьбу пользователя - решительный разворот (держащая
                # стенка просела ≤30% от размера на входе И встречная выросла
                # до калибра входного порога), а wall_eaten_flat/imbalance
                # (самые шумные) отключены вовсе. Это уже НЕ шум - это ровно тот
                # сигнал, по которому пользователь прямо просил выходить, даже
                # если позиция в моменте в плюсе ("держит лонг до того как
                # заявка лонга [не станет] выше за заявку шорта"). Раньше эта
                # экспозиция блокировала закрытие ИМЕННО в таких случаях (найдено
                # в проде 18.08 поздно вечером: "открывает шорт при 1.5кк, оно
                # пропадает, появляется 2кк в лонг - а бот всё равно держит
                # сделку", потому что позиция была в плюсе на момент разворота).
                # Поэтому эта экспозиция больше НЕ используется для гейта
                # invalidation_streak ниже - profit_pct всё ещё считается выше
                # тика ради THESIS WEAKENING ниже.
                already_profitable = False

                # THESIS WEAKENING (этап 5 аудита, 18.08) - промежуточное
                # состояние между VALID и INVALIDATED: формальная структура
                # (стенка/уровень) ещё не сломана (thesis_invalid=False - если
                # уже True, ниже сработает полноценный INVALIDATED-путь, эта
                # ветка тут не нужна), но позиция уже набирала заметный ход
                # (MFE) и заметная его часть откатилась назад - импульс явно
                # затухает. Не завязано на invalidation_streak - решение
                # принимается сразу, но частичный выход срабатывает не больше
                # одного раза за сделку (weakening_partial_done).
                # ИСПРАВЛЕНО 19.08 (финальный этап рестройки) - НАЙДЕН РЕАЛЬНЫЙ БАГ:
                # условие `profit_pct > 0` не даёт этой ветке сработать вообще,
                # если цена успела ПОЛНОСТЬЮ откатить от MFE через вход и уйти в
                # минус - то есть ровно в том случае, который эта проверка и
                # должна ловить (momentum decay), она молчала. Живая жалоба
                # пользователя: шорт по 69655, цена дошла до 69560 (в плюсе),
                # затем развернулась до 69685 (уже в минусе) - позиция
                # оставалась открытой, потому что profit_pct к этому моменту
                # был отрицательным и WEAKENING не проверялся вовсе, хотя MFE
                # уже был набран, а retrace от него - почти полный. Убрали
                # гейт `profit_pct > 0` - retrace_pct = mfe_pct - profit_pct
                # корректно считается и тогда, когда profit_pct отрицателен
                # (в этом случае retrace получается даже БОЛЬШЕ mfe_pct, и
                # условие retrace_pct >= mfe_pct * MOMENTUM_DECAY_RETRACE_PCT
                # сразу истинно) - именно так и должно быть: чем сильнее
                # откат от пика (вплоть до ухода в минус), тем очевиднее, что
                # импульс, на котором строился вход, уже не работает.
                if (not thesis_invalid and not pos.weakening_partial_done
                        and pos.mfe_pct > CFG.wall_eaten_flat_pct * CFG.weakening_mfe_min_mult):
                    retrace_pct = pos.mfe_pct - profit_pct
                    if retrace_pct >= pos.mfe_pct * CFG.momentum_decay_retrace_pct:
                        close_amount = pos.filled_size * CFG.weakening_partial_close_pct
                        if close_amount > 0:
                            filled_now = await self._partial_close(pos, snap, close_amount, "thesis_weakening")
                            if filled_now > 0:
                                pos.weakening_partial_done = True
                                pos.thesis_state = "WEAKENING"
                                log.info("[%s] THESIS WEAKENING: закрыто %.1f%% позиции "
                                          "(MFE=%.3f%% профит сейчас=%.3f%% откат=%.3f%%)",
                                          pos.symbol, CFG.weakening_partial_close_pct * 100,
                                          pos.mfe_pct, profit_pct, retrace_pct)
                            if not self.has_position(pos.symbol):
                                return

                if thesis_invalid and not already_profitable:
                    pos.invalidation_streak += 1
                else:
                    pos.invalidation_streak = 0

                if pos.invalidation_streak >= CFG.invalidation_confirm_ticks:
                    pos.thesis_state = "INVALIDATED"
                    pos.pending_close_reason = "structure_invalidated"
                    await self._close_position_safely(pos, snap, "structure_invalidated")
                    if not self.has_position(pos.symbol):
                        return
                    pos.invalidation_streak = 0
                    continue

        except asyncio.CancelledError:
            return

    def _thesis_invalidated(self, pos: ManagedPosition, snap, exec_snap=None) -> bool:
        """
        "Умный" выход: проверяет, жива ли ещё сама причина, по которой вошли в
        сделку - а не только цена относительно стопа/тейка.

        exec_snap - стакан Lighter (та же цена, от которой avg_entry) - нужен
        только для расчёта PnL в ветке "стенку съели, цена стоит на месте" ниже
        (см. wall_eaten_flat_pct); остальные проверки в этой функции работают
        по структуре signal_snap (snap), как и раньше.
        """
        if snap is None or not pos.reference_price:
            return False

        # Сравнение силы стенок - НЕ завязано на signal_type и НЕ завязано на
        # % прибыли (в отличие от _opposing_wall_exit ниже). Берём ближайшую к
        # цене стенку с каждой стороны - именно она реально "держит"/
        # "блокирует" цену прямо сейчас, а не самая крупная стенка где-то
        # далеко в стакане. Если стенки, которая держит движение, вообще нет
        # (0) - это ещё хуже, чем слабая: любая блокирующая стенка тогда
        # считается доминирующей.
        #
        # ПЕРЕПИСАНО 18.08 вечером, затем ЕЩЁ РАЗ 19.08 по прямой просьбе
        # пользователя после разбора живых сделок. Версия от 18.08 требовала
        # ДВУХ условий: держащая стенка просела ≤30% от своего размера НА
        # ВХОДЕ (WALL_FLIP_SHRINK_RATIO), И встречная стенка доросла до
        # калибра входного порога (BINANCE_WALL_MIN_USD=1.5М). На практике
        # (см. живые примеры пользователя 19.08) это оказалось СЛИШКОМ
        # ПОЗДНО: сделка на 64358.80 - держащая стенка 2М просела всего до
        # 1М (50%, не ≤30%) ровно когда встречная стенка тоже стала 1М -
        # то есть стенки УЖЕ СРАВНЯЛИСЬ, а бот всё ещё держал позицию, потому
        # что ни одно из двух условий формально не выполнилось (1М/2М=50%>30%,
        # и 1М < 1.5М порога). Второй пример - держащая 2М просела до 1М,
        # встречная была уже 2.5М (больше самого входного порога) с самого
        # начала - бот всё равно держал, по той же причине (50%>30%).
        # Пользователь прямо потребовал: сравнивать ТЕКУЩИЕ размеры стенок
        # напрямую (кто сейчас больше - та стенка и держит), а не % от
        # исторического размера на входе. pos.entry_wall_usd больше не
        # участвует в сравнении (оставлено в ManagedPosition как история/на
        # случай отката). "Встречная - настоящая" теперь проверяется по
        # низкому базовому порогу WALL_MIN_USD (150к), а не по калибру
        # входного сигнала (1.5М) - иначе ровно так же пропускаем случаи,
        # где встречная всего 1М, но этого уже достаточно, раз она сравнялась
        # с держащей. Шум на 1 тик всё ещё гасится WALL_DOMINANCE_CONFIRM_TICKS
        # (3 тика) ниже - см. pos.wall_dominance_streak.
        blocking_walls = snap.ask_walls if pos.side == "long" else snap.bid_walls
        holding_walls = snap.bid_walls if pos.side == "long" else snap.ask_walls
        now_ts = snap.ts

        # НАЙДЕНО В ПРОДЕ 19.08 - см. CFG.wall_presence_grace_sec: snap.ask_walls/
        # bid_walls - это сырой топ-20 снепшот Binance partial-depth стрима, а не
        # персистентный трекинг. Реальная стенка может на одном тике просто не
        # попасть в эти 20 уровней (плотный стакан), хотя на бирже она никуда не
        # делась - раньше это читалось как "стенки нет" (0 USD) и dominance_now
        # даже не успевал попробовать стать True (живой пример - SHORT 64463.70,
        # ноль строк "dominance:" за всю сделку). Держим последний реально
        # увиденный размер ещё wall_presence_grace_sec секунд, прежде чем
        # признать стенку по-настоящему исчезнувшей.
        if blocking_walls:
            nearest_blocking = min(blocking_walls, key=lambda w: w.distance_pct)
            blocking_usd_now = nearest_blocking.usd
            pos.last_blocking_wall_usd = blocking_usd_now
            pos.last_blocking_wall_ts = now_ts
        elif now_ts - pos.last_blocking_wall_ts <= CFG.wall_presence_grace_sec:
            blocking_usd_now = pos.last_blocking_wall_usd
        else:
            blocking_usd_now = 0.0

        if holding_walls:
            nearest_holding = min(holding_walls, key=lambda w: w.distance_pct)
            holding_usd = nearest_holding.usd
            pos.last_holding_wall_usd = holding_usd
            pos.last_holding_wall_ts = now_ts
        elif now_ts - pos.last_holding_wall_ts <= CFG.wall_presence_grace_sec:
            holding_usd = pos.last_holding_wall_usd
        else:
            holding_usd = 0.0

        opposing_is_real_wall = blocking_usd_now >= CFG.wall_min_usd
        opposing_at_least_holding = blocking_usd_now >= holding_usd
        dominance_now = opposing_is_real_wall and opposing_at_least_holding
        # Отдельный, собственный счётчик подряд идущих тиков для ЭТОЙ проверки
        # (не общий invalidation_streak) - см. комментарий у
        # pos.wall_dominance_streak. Разовое дёрганье стенки на 1 тик больше
        # не хлопает structure_invalidated само по себе.
        #
        # Логируем ТОЛЬКО переходы состояния (старт/сброс/срабатывание серии),
        # а не каждый тик - иначе спам в логах при position_check_interval_sec=1s.
        # Добавлено 19.08 - раньше не было видно ПОЧЕМУ конкретная сделка
        # держалась N секунд (пользователь спрашивал про размеры стенок в
        # моменте, а в логах их не было вообще, приходилось гадать по
        # косвенным WALL_CANDIDATE/WALL_OUTCOME). Теперь видно держащая/
        # встречная в USD на каждой смене состояния серии.
        # НАЙДЕНО В ПРОДЕ 19.08 - прямая причина жалобы пользователя ("встречная
        # 3кк появилась, а бот держит сделку"): жёсткий сброс streak->0 на ОДНОМ
        # пропущенном тике убивал серию почти всегда, ДО того как она успевала
        # дойти до 3. Живой пример из логов (LONG от 11:30:29): в 11:32:12 серия
        # началась (держащая=0 встречная=1734031), но уже в 11:32:20 сброшена на
        # 2/3 тиках с "держащая=0 встречная=0" - то есть блокирующая стенка на
        # ОДНОМ тике просто выпала из snap.ask_walls (мигнула) и весь прогресс
        # обнулился, хотя её USD реально не менялся ни до, ни после - через пару
        # секунд она вернулась, но серия уже начиналась с нуля. Дёрганье walls-
        # списка от тика к тику (а не только dominance_now) - отдельный источник
        # шума, который жёсткий reset не переживал. Меняем на "протекающее
        # ведро": пропущенный тик снимает 1 из streak вместо обнуления - серия
        # True,True,miss,True,True всё ещё доходит до 3 и закрывает сделку,
        # вместо того чтобы начинать заново с нуля на каждом одиночном сбое.
        if dominance_now:
            if pos.wall_dominance_streak == 0:
                log.info("[%s] dominance: серия началась (держащая=%.0f встречная=%.0f, нужно %d "
                          "тиков подряд)", pos.symbol, holding_usd, blocking_usd_now,
                          CFG.wall_dominance_confirm_ticks)
            pos.wall_dominance_streak += 1
        else:
            if pos.wall_dominance_streak > 0:
                pos.wall_dominance_streak -= 1
                log.info("[%s] dominance: пропущен тик, серия %d/%d -> %d/%d (держащая=%.0f встречная=%.0f)",
                          pos.symbol, pos.wall_dominance_streak + 1, CFG.wall_dominance_confirm_ticks,
                          pos.wall_dominance_streak, CFG.wall_dominance_confirm_ticks,
                          holding_usd, blocking_usd_now)
        if pos.wall_dominance_streak >= CFG.wall_dominance_confirm_ticks:
            log.info("[%s] dominance: ПОДТВЕРЖДЕНО %d тиков подряд (держащая=%.0f встречная=%.0f) - "
                      "закрываем", pos.symbol, pos.wall_dominance_streak, holding_usd, blocking_usd_now)
            return True

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
            if not still_there:
                if price_broke_through:
                    # реальный слом структуры - цена уже прошла уровень стенки.
                    # Это НЕ голое сравнение размеров стенок (как dominance
                    # выше) - тут два условия сразу (стенки нет И цена уже
                    # прошла её уровень), само по себе достаточно решительно -
                    # оставляем мгновенным (только через общий invalidation_streak).
                    return True
                # Стенка пропала (съедена реальным объёмом или снята), но цена
                # ЕЩЁ НЕ пробила её уровень против нас - смотрим, куда цена
                # успела уйти относительно входа (по exec_snap = Lighter, той
                # же цене, что и avg_entry - иначе basis Binance/Lighter
                # искажает расчёт, см. аналогичный комментарий у
                # _opposing_wall_exit). Если цена стоит на месте (в пределах
                # WALL_EATEN_FLAT_PCT) - держать больше нечего, повод для
                # сделки исчез без какого-либо результата - выходим примерно
                # в ноль, не дожидаясь ни тейка, ни стопа. Если же цена уже
                # ушла в НАШУ пользу за пределы этой полосы - это не провал
                # тезиса, а скорее его подтверждение (стенку съели и цена
                # пошла куда надо - ровно на этом строится отдельный сигнал
                # BREAKOUT в signals.py) - не режем прибыль, даём тейку/
                # трейлингу отработать как обычно.
                # Найдено в проде 18.08 вечером (после фикса dominance-streak,
                # структура закрытий не улучшилась - 49 из 60 сделок всё ещё
                # structure_invalidated за 13-90 сек): exec_snap - это ТИК
                # снепшот стакана Lighter, "стенки нет" может быть тем же
                # секундным дёрганьем, что и dominance выше - см. комментарий
                # там. Даём тот же 3-тиковый счётчик, а не мгновенный выход.
                # ОТКЛЮЧЕНО 18.08 поздно вечером по прямой просьбе пользователя -
                # "заходит на 2кк, стенка стоит пару минут, а оно всё равно
                # закрывает в минус". Дословная позиция пользователя (см. его
                # же формулировку wall-flip логики выше): держим позицию, ПОКА
                # НЕ сработает именно разворот доминирования стенок (dominance
                # выше) - никаких других "умных" причин закрыться раньше, пока
                # СВОЯ стенка вообще в книге. wall_eaten_flat закрывал даже
                # когда стенка ещё технически на месте, просто эта ветка не
                # проверяет её сохранность вообще, только застой цены - что и
                # противоречит просьбе. Если понадобится вернуть - меняем
                # False на исходное условие ниже.
                wall_eaten_flat_now = False
                if False and exec_snap is not None:
                    profit_pct = ((exec_snap.mid - pos.avg_entry) / pos.avg_entry * 100) if pos.side == "long" \
                        else ((pos.avg_entry - exec_snap.mid) / pos.avg_entry * 100)
                    wall_eaten_flat_now = abs(profit_pct) <= CFG.wall_eaten_flat_pct
                if wall_eaten_flat_now:
                    pos.wall_eaten_streak += 1
                else:
                    pos.wall_eaten_streak = 0
                if pos.wall_eaten_streak >= CFG.wall_dominance_confirm_ticks:
                    return True
            else:
                pos.wall_eaten_streak = 0
            # Старая проверка "дисбаланс по всей книге развернулся" (imbalance_flip_now)
            # УДАЛЕНА 19.08 (финальный этап рестройки) - она была отключена
            # 18.08 по прямой просьбе пользователя именно потому, что imbalance -
            # метрика ПО ВСЕЙ книге, не привязанная к конкретной стенке входа, и
            # могла закрыть сделку даже когда своя стенка ещё на месте. Заменена
            # по существу более точным механизмом - см. _opposite_flow_exit
            # ниже: вместо статичного снимка дисбаланса объёма в стакане
            # смотрим на РЕАЛЬНЫЙ исполненный поток (BinanceTradeFeed) рядом с
            # текущей ценой за последние секунды - то же самое намерение
            # ("поток развернулся против нас"), но по факту исполненных сделок,
            # а не по displayed-размеру заявок, и со своим debounce
            # (opposite_flow_confirm_ticks), а не завязана на общий
            # wall_dominance_confirm_ticks. Проверяется отдельно в
            # _watch_position, не здесь.
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

    def _opposing_wall_exit(self, pos: ManagedPosition, signal_snap, exec_snap) -> bool:
        """
        Фиксация прибыли у НОВОЙ встречной стенки, не связанной с исходным
        сигналом. _thesis_invalidated следит только за стенкой/уровнем, от
        которых был сигнал на вход - но по ходу движения цены в нашу пользу
        может появиться СВЕЖАЯ крупная стенка на противоположной стороне
        (сопротивление для лонга / поддержка для шорта), в которую цена упрётся
        и от которой развернётся. Без этой проверки бот ждёт либо TP1, либо
        развала исходного тезиса - а к этому моменту цена может откатить и
        отдать почти всю набежавшую прибыль (пример из прода: памп до 64230,
        тут же рядом выросла крупная стенка на шорт на 64225-64228, а бот
        закрыл сделку только на откате до 64210, вместо жёстко у стенки).
        Не ждёт thesis_grace_period_sec - это не про "тезис входа был неверным",
        а про новый факт на рынке, который появился уже после входа.

        Специально НЕ требуем от встречной стенки сильной "подложки" (в отличие
        от входа по ABSORPTION, см. signals.py) - логика тут обратная: если
        сопротивление слабое и вот-вот развалится, тем более нет смысла ждать
        подтверждения, лучше зафиксировать прибыль сейчас, пока она есть
        (пример пользователя: лонг упёрся в стенку 2М, а подложки под ней
        меньше 1.5М - именно повод закрыться, а не ждать).

        ВАЖНО: "в прибыли" - это не просто mid чуть выше входа (буквально любой
        шум в 0.001% с крупной стенкой на глубоком стакане Binance закрывал бы
        сделку немедленно - и после round-trip проскальзывания на входе/выходе
        такое закрытие гарантированно давало убыток, что и наблюдалось в проде:
        первые два opposing_wall закрытия дали -0.47 и -0.35 вместо прибыли).
        Требуем движение минимум на OPPOSING_WALL_MIN_PROFIT_PCT от цены входа -
        но это не фиксированное число для всех ситуаций: масштабируем порог
        волатильностью рынка на момент входа (0.15% при спокойном рынке и при
        резком движении - разные вещи, см. entry_volatility_pct/config.py).

        ВАЖНО #2: прибыль считаем от РЕАЛЬНОЙ цены исполнения (exec_snap = Lighter,
        тот же стакан, где avg_entry) - а не от signal_snap (Binance), иначе
        basis Binance/Lighter (наблюдался ~0.02%) искажает расчёт "в прибыли ли
        мы вообще", хоть и на небольшую величину. Структуру (встречную стенку)
        по-прежнему берём с Binance - там стакан глубже.

        ВАЖНО #3: порог прибыли не одинаковый для любой встречной стенки. Если
        стенка стоит ПРЯМО у цены (в пределах OPPOSING_WALL_CLOSE_DISTANCE_PCT) -
        это более срочный сигнал (цена уже физически упёрлась в неё), и порог
        снижается до OPPOSING_WALL_CLOSE_MIN_PROFIT_PCT. Для дальней стенки
        (0.2-0.3% и дальше) порог остаётся обычным effective_min_profit_pct -
        риск, что до неё вообще дойдёт, ниже, спешить закрываться незачем.
        Пониженный порог всё равно НЕ уходит ниже round-trip издержек на
        проскальзывание - иначе это снова гарантированный убыток после входа/
        выхода, ровно та регрессия, из-за которой появился минимальный порог
        прибыли изначально (см. комментарий в config.py).
        """
        if signal_snap is None or exec_snap is None:
            return False
        # для лонга встречная стенка - на продажу (ask), для шорта - на покупку (bid)
        opposing_walls = signal_snap.ask_walls if pos.side == "long" else signal_snap.bid_walls
        if not opposing_walls:
            return False

        effective_min_profit_pct = max(
            CFG.opposing_wall_min_profit_pct,
            pos.entry_volatility_pct * CFG.opposing_wall_vol_multiplier,
        )
        nearest_distance_pct = min(w.distance_pct for w in opposing_walls)
        if nearest_distance_pct <= CFG.opposing_wall_close_distance_pct:
            # стенка "в упор" - используем пониженный порог, но не задираем его
            # обратно вверх, если обычный порог и так был ниже (волатильность)
            effective_min_profit_pct = min(effective_min_profit_pct, CFG.opposing_wall_close_min_profit_pct)

        profit_pct = ((exec_snap.mid - pos.avg_entry) / pos.avg_entry * 100) if pos.side == "long" \
            else ((pos.avg_entry - exec_snap.mid) / pos.avg_entry * 100)
        return profit_pct >= effective_min_profit_pct

    def _opposite_flow_exit(self, pos: ManagedPosition, snap) -> bool:
        """
        Opposite-flow exit (19.08, финальный этап рестройки) - см. вызов и
        обоснование в _watch_position. В отличие от _thesis_invalidated
        (смотрит на структуру - размеры стенок у уровня входа) и
        _opposing_wall_exit (смотрит на появление НОВОЙ встречной стенки),
        здесь смотрим на РЕАЛЬНЫЙ исполненный поток (не displayed-размер
        заявок) рядом с текущей ценой - та же функция executed_usd_trend, что
        используется для классификации входа в signals.py, применённая уже
        ПОСЛЕ входа для проверки "не развернулся ли сам order flow".

        snap - стакан Lighter (цена исполнения) - тейп же общий по символу у
        BinanceTradeFeed, конкретная биржа-источник тут не выбирается, только
        текущая цена, вокруг которой смотрим окно.
        """
        if self.trade_feed is None or snap is None:
            return False
        try:
            buckets = self.trade_feed.executed_usd_trend(
                pos.symbol, snap.mid, CFG.opposite_flow_range_pct,
                lookback_sec=CFG.opposite_flow_lookback_sec, buckets=4)
        except Exception:
            return False
        total_buy = sum(b["buy_usd"] for b in buckets)
        total_sell = sum(b["sell_usd"] for b in buckets)
        if total_buy + total_sell < CFG.opposite_flow_min_total_usd:
            return False  # слишком разреженный поток, чтобы вообще судить о направлении

        supportive = total_buy if pos.side == "long" else total_sell
        opposing = total_sell if pos.side == "long" else total_buy
        if supportive <= 0:
            return opposing > 0
        return opposing >= supportive * CFG.opposite_flow_dominance_ratio

    def _effective_trailing_pct(self, pos: ManagedPosition) -> float:
        """
        Dynamic profit taking (19.08, финальный этап рестройки, прямой пункт
        запроса пользователя - "не жди только фиксированный TP/SL") - трейлинг
        подтягивается ТЕМ ПЛОТНЕЕ к пику, чем дальше сделка уже прошла в нашу
        пользу относительно исходного риска (стоп-дистанции). Отдавать один и
        тот же фиксированный % от пика разумно, пока путь небольшой - но чем
        больше уже набрано MFE относительно риска на сделку, тем больше в
        абсолютном выражении означает отдать ту же долю обратно. Ступенчато
        (не непрерывная функция) - проще объяснить и предсказать, чем плавная
        формула, при сопоставимом эффекте.
        """
        base = pos.plan.trailing_stop_pct
        stop_distance_pct = abs(pos.plan.entry_price - pos.plan.stop_price) / pos.plan.entry_price * 100
        if stop_distance_pct <= 0 or pos.mfe_pct <= 0:
            return base
        mfe_multiple = pos.mfe_pct / stop_distance_pct
        if mfe_multiple >= CFG.dynamic_trailing_mfe_mult_2:
            return base * CFG.dynamic_trailing_tighten_pct_2
        if mfe_multiple >= CFG.dynamic_trailing_mfe_mult_1:
            return base * CFG.dynamic_trailing_tighten_pct_1
        return base

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

        # Dynamic profit taking - см. _effective_trailing_pct: подтягиваем
        # трейлинг плотнее по мере роста MFE относительно исходного риска,
        # вместо одного и того же фиксированного % пути всегда.
        trailing_pct = self._effective_trailing_pct(pos)
        trail_dist = pos.trailing_extreme * trailing_pct / 100
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

    def _close_price(self, snap, exit_is_ask: bool, fallback_price: float, stall_count: int = 0) -> float:
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

        stall_count - см. pos.close_stall_count. Базовый буфер гарантирует
        пересечение СПРЕДА, но не гарантирует пересечение достаточной ГЛУБИНЫ
        книги, если объём позиции больше того, что стоит на самой верхушке.
        Найдено в проде 18.08: позиция ETH на 1.5788 не могла закрыться >3
        минут - каждая попытка с одним и тем же крошечным буфером снова
        исполнялась лишь на ~0.12 ETH. Эскалируем буфер с каждой неудачной
        попыткой (шире буфер = глубже внутрь книги), чтобы закрытие в итоге
        гарантированно прошло целиком, вместо неопределённо долгого зависания.
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
        вызывающий код (TP1 в _watch_position) использует это, чтобы не считать
        частичный/нулевой филл полным закрытием (см. pos.tp1_filled)."""
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
        if reason in ("stop_loss", "opposing_wall", "structure_invalidated", "time_exit", "opposite_flow"):
            # НЕ трогаем при reversal_signal - см. CFG.reentry_cooldown_sec/
            # комментарий у self._last_close_ts в __init__. opposite_flow
            # добавлен 19.08 (финальный этап рестройки) - тот же принцип: не
            # влезать сразу обратно, пока не пройдёт кулдаун после закрытия.
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
        # может в этот самый момент тикать по стопу/встречной стенке на той же
        # позиции и гонка с закрытием ниже (см. self._close_locks в __init__ -
        # тот же принцип, здесь дополнительно глушим watcher'ов заранее, а не
        # только полагаемся на лок, раз уж всё равно вызываем их отмену чуть ниже).
        for t in self._watchers.values():
            t.cancel()
        self._watchers.clear()

        for symbol, pos in list(self.positions.items()):
            async with self._get_close_lock(symbol):
                # Пока ждали лок, позицию мог уже закрыть параллельный вызов
                # (reversal_signal из handle_signal, если он успел проскочить
                # до того, как kill_switch.active стало True) - перепроверяем.
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
        self._last_reversal_ts.clear()
        self._entering.clear()

        if self.kill_switch and self.kill_switch.active:
            self.kill_switch.reset()

        log.warning("СЧЁТ СБРОШЕН (%s): баланс -> $%.2f, позиции и история сделок обнулены.",
                    reason, CFG.account_equity_usd)

    async def shutdown(self):
        for t in self._watchers.values():
            t.cancel()
