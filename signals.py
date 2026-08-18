"""
Сигнальный движок: превращает поток снапшотов стакана (market_data.BookSnapshot)
в торговые сигналы long/short.

Два паттерна:
  - ABSORPTION (поглощение): цена подходит к крупной стенке, стенка держится
    (не тает и не убегает) несколько снапшотов подряд, цена стопорится рядом ->
    сигнал в сторону ОТ стенки (фейд).
  - BREAKOUT (пробой): стенка, которая долго стояла, резко исчезает
    (съедена агрессивным потоком, а не просто отодвинулась) -> сигнал ПО тренду.

Фильтр спуфинга: если стенка исчезла, а цена к ней даже не приближалась
(дистанция почти не менялась) — это, скорее всего, снятая "фейковая" заявка,
такой уход стенки сигналом не считается. Плюс отдельная история отмен по зоне
цены (см. _record_cancel/_zone_cancel_count) - если в одной и той же зоне
заявки регулярно снимаются именно при подходе цены, это подозрительная зона.

Помимо самого факта "стенка есть/нет", размер стенки сам по себе - слабый
сигнал (см. обсуждение с пользователем и рекомендации по итогам разбора).
Поэтому дополнительно считаем:
  - replenishment: восстанавливается ли displayed size после проседания
    (recurring refill) - признак настоящего интереса/iceberg, а не разовой заявки;
  - executed-объём рядом со стенкой по реальным сделкам (BinanceTradeFeed) -
    сколько на самом деле прошло объёма, а не только что видно в стакане;
  - "зона", а не точная цена - маркет-мейкер может подвинуть заявку на пару
    центов, это не значит что стенка пропала (см. _find_shifted_match);
  - составной WALL_SCORE из этих компонент - логируется для каждого кандидата
    (прошёл фильтры или нет), НЕ используется пока как жёсткий гейт (см.
    коммент у CFG.wall_backup_min_ratio - подкручивание порогов вслепую уже
    один раз положило вход в сделки на 0, лучше сначала накопить данные по
    логам WALL_CANDIDATE/WALL_OUTCOME и откалибровать осмысленно).
"""
import asyncio
import itertools
import logging
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Deque, Dict, Optional, Tuple

from config import CFG
from market_data import BookSnapshot, Wall
from trend_filter import TrendFilter

log = logging.getLogger("signals")

PRICE_BUCKET_DECIMALS = 2  # группировка уровней стакана в "стенки" для трекинга во времени

# Для калибровки порогов по фактам, а не на глаз (см. WALL_CANDIDATE/WALL_OUTCOME
# ниже) - каждому кандидату присваивается id, и через 1/3/5с логируется, куда
# фактически ушла цена, независимо от того, прошёл кандидат фильтры или нет.
# Так видно survivorship bias - что было бы, если бы фильтр пропустил и то, что он отсёк.
_next_candidate_id = itertools.count(1)
CANDIDATE_OUTCOME_DELAYS_SEC = (1.0, 3.0, 5.0)
CANDIDATE_LOG_COOLDOWN_SEC = 5.0  # не спамить лог по одной и той же стенке каждые 100мс

# Ширина "зоны" для истории отмен - в % от цены (не абсолютное число, чтобы
# одинаково работало и для BTC (~64000), и для ETH (~3000)).
CANCEL_ZONE_WIDTH_PCT = 0.05
CANCEL_HISTORY_LOOKBACK_SEC = 300.0  # 5 минут


@dataclass
class Signal:
    symbol: str
    side: str  # "long" | "short"
    signal_type: str  # "absorption" | "breakout"
    reference_price: float  # цена стенки, от которой сигнал
    mid: float
    confidence: float  # 0..1
    ts: float
    # Волатильность рынка (state.volatility_pct) на момент сигнала - используется
    # order_manager, чтобы масштабировать opposing_wall_min_profit_pct под текущий
    # режим рынка вместо одного фиксированного % на все случаи (см. обсуждение
    # с пользователем - 0.15% при спокойном рынке и при резком движении не одно и то же).
    volatility_pct: float = 0.0


@dataclass
class _TrackedWall:
    wall: Wall
    first_seen: float
    last_seen: float
    max_usd: float
    stall_count: int = 0  # сколько снапшотов подряд цена "топчется" рядом со стенкой
    # --- replenishment / persistence, для WALL_SCORE и STATIC/ACTIVE классификации ---
    initial_usd: float = 0.0
    prev_usd: float = 0.0
    min_usd_seen: float = 0.0
    update_count: int = 0
    refill_count: int = 0  # сколько раз displayed size заметно восстановился после просадки
    last_candidate_log_ts: float = 0.0


class SignalEngine:
    def __init__(self, symbol: str, history_len: int = 30, trend_filter: TrendFilter = None,
                 trade_feed=None):
        self.symbol = symbol
        self.history: Deque[BookSnapshot] = deque(maxlen=history_len)
        self.tracked: Dict[float, _TrackedWall] = {}  # bucketed price -> _TrackedWall
        self.min_wall_age_sec = 3.0  # стенка должна простоять хотя бы столько, чтобы считаться "реальной"
        self.trend_filter = trend_filter or TrendFilter()
        self.last_trend_state = None
        # BinanceTradeFeed - реальный исполненный объём у стенки (см. binance_trades.py).
        # None, если не подключён (например MODE=collect или Binance-сигналы выключены).
        self.trade_feed = trade_feed
        # История отмен по price-зоне (side, zone_bucket) -> deque[(ts, distance_pct_at_cancel)].
        # Если в одной и той же зоне заявки регулярно снимаются именно при подходе
        # цены - подозрительно на спуфинг, см. _zone_cancel_count.
        self.cancel_zones: Dict[Tuple[str, float], Deque[Tuple[float, float]]] = {}
        # Порог "крупной стенки", который реально применялся при формировании этого
        # потока снапшотов - для нормировки WALL_SCORE (Binance-порог на порядок
        # выше Lighter-порога, доля от него значит разное в абсолютных USD).
        self.base_wall_min_usd = CFG.binance_wall_min_usd if CFG.use_binance_signals else CFG.wall_min_usd

    @staticmethod
    def _bucket(price: float) -> float:
        return round(price, PRICE_BUCKET_DECIMALS)

    def _zone_key(self, side: str, price: float) -> Tuple[str, float]:
        bucket_width = max(price * CANCEL_ZONE_WIDTH_PCT / 100, 1e-9)
        return (side, round(price / bucket_width))

    def _record_cancel(self, tw: "_TrackedWall"):
        key = self._zone_key(tw.wall.side, tw.wall.price)
        hist = self.cancel_zones.setdefault(key, deque(maxlen=20))
        hist.append((time.time(), tw.wall.distance_pct))

    def _zone_cancel_count(self, side: str, price: float, lookback_sec: float = CANCEL_HISTORY_LOOKBACK_SEC) -> int:
        key = self._zone_key(side, price)
        hist = self.cancel_zones.get(key)
        if not hist:
            return 0
        cutoff = time.time() - lookback_sec
        return sum(1 for ts, _ in hist if ts >= cutoff)

    def _find_shifted_match(self, tw: "_TrackedWall", seen_now: dict, claimed_keys: set):
        """
        Маркет-мейкер часто немного двигает заявку (на центы), из-за чего точная
        цена (bucket key) меняется каждый тик - формально "стенка пропала",
        хотя по факту это та же заявка. Ищем среди ещё не сматченных новых
        уровней этого тика похожую по цене (в пределах 0.03%) и размеру
        (0.4x-2.5x) стенку на той же стороне - если нашли, считаем это той же
        стенкой, просто переставленной, а не исчезновением/спуфингом.
        """
        for key, w in seen_now.items():
            if key in self.tracked or key in claimed_keys:
                continue
            if w.side != tw.wall.side:
                continue
            price_diff_pct = abs(w.price - tw.wall.price) / tw.wall.price * 100
            if price_diff_pct > 0.03:
                continue
            if tw.wall.usd <= 0:
                continue
            size_ratio = w.usd / tw.wall.usd
            if not (0.4 <= size_ratio <= 2.5):
                continue
            return key, w
        return None, None

    def on_snapshot(self, snap: BookSnapshot) -> Optional[Signal]:
        self.history.append(snap)
        now = snap.ts

        trend_state = self.trend_filter.update(snap.mid)
        self.last_trend_state = trend_state
        if trend_state.high_volatility:
            # аномальный всплеск волатильности - стакан ведёт себя нештатно, сигналы не генерируем
            return None

        seen_now = {}
        for w in (*snap.bid_walls, *snap.ask_walls):
            key = self._bucket(w.price)
            seen_now[key] = w

        breakout_signal = None
        claimed_keys = set()

        # обновляем/детектируем исчезновение стенок
        for key, tw in list(self.tracked.items()):
            if key in seen_now:
                w = seen_now[key]
                tw.wall = w
                tw.last_seen = now
                tw.max_usd = max(tw.max_usd, w.usd)
                tw.update_count += 1
                if w.usd < tw.min_usd_seen or tw.min_usd_seen == 0:
                    tw.min_usd_seen = w.usd
                # replenishment: usd заметно вырос после того как до этого просело -
                # признак, что заявку восполняют (iceberg/реальный интерес), а не
                # что она просто одна и та же неизменная заявка либо тает без возврата.
                if tw.prev_usd > 0 and w.usd > tw.prev_usd * 1.15 and tw.prev_usd < tw.max_usd * 0.9:
                    tw.refill_count += 1
                tw.prev_usd = w.usd
                # "топчется" рядом: mid почти не двигается последние снапшоты
                if len(self.history) >= 3:
                    mids = [s.mid for s in list(self.history)[-3:]]
                    stalled = (max(mids) - min(mids)) / snap.mid < 0.0006  # ~0.06%
                    if stalled and w.distance_pct < CFG.wall_max_distance_pct:
                        tw.stall_count += 1
                    else:
                        tw.stall_count = 0
            else:
                # Возможно, это та же заявка, просто немного переставленная -
                # не считаем ни спуфингом, ни исчезновением, переносим трекинг.
                new_key, new_w = self._find_shifted_match(tw, seen_now, claimed_keys)
                if new_key is not None:
                    claimed_keys.add(new_key)
                    del self.tracked[key]
                    tw.wall = new_w
                    tw.last_seen = now
                    tw.max_usd = max(tw.max_usd, new_w.usd)
                    tw.update_count += 1
                    self.tracked[new_key] = tw
                    continue

                # стенка реально пропала: проверяем спуфинг vs реальное поглощение/пробой
                age = tw.last_seen - tw.first_seen
                was_close = tw.wall.distance_pct < CFG.wall_max_distance_pct
                price_crossed = (
                    (tw.wall.side == "ask" and snap.mid >= tw.wall.price) or
                    (tw.wall.side == "bid" and snap.mid <= tw.wall.price)
                )
                if age >= self.min_wall_age_sec and was_close and price_crossed:
                    # реальный пробой: стенка простояла, была близко и цена через неё прошла.
                    # Пробой ПО тренду не фильтруем - фильтр только против контр-трендовых входов.
                    side = "long" if tw.wall.side == "ask" else "short"
                    breakout_signal = Signal(
                        symbol=self.symbol,
                        side=side,
                        signal_type="breakout",
                        reference_price=tw.wall.price,
                        mid=snap.mid,
                        confidence=self._confidence(snap, side, boost=0.15),
                        ts=now,
                        volatility_pct=trend_state.volatility_pct,
                    )
                    self._log_wall_candidate(tw, side, snap, age, passed=True, reason="",
                                              signal_type="breakout")
                    log.info("[%s] BREAKOUT %s у %.2f (стенка стояла %.1fs, %.0f USD)",
                              self.symbol, side.upper(), tw.wall.price, age, tw.max_usd)
                else:
                    # похоже на спуфинг (сняли заявку) или стенка была далеко от рынка.
                    # Если была близко - фиксируем как отмену в этой зоне (для spoof
                    # detection - см. _zone_cancel_count) и логируем как кандидата,
                    # чтобы видеть, куда фактически шла цена после таких отмен.
                    if was_close:
                        side = "short" if tw.wall.side == "ask" else "long"  # сторона, которую бы "фейдили"
                        self._record_cancel(tw)
                        self._log_wall_candidate(tw, side, snap, age, passed=False,
                                                  reason="cancelled_no_breakout")
                del self.tracked[key]

        for key, w in seen_now.items():
            if key not in self.tracked:
                self.tracked[key] = _TrackedWall(wall=w, first_seen=now, last_seen=now, max_usd=w.usd,
                                                  initial_usd=w.usd, prev_usd=w.usd, min_usd_seen=w.usd,
                                                  update_count=1)

        if breakout_signal:
            return breakout_signal

        # ABSORPTION: ищем стенку, простоявшую достаточно и с накопленным stall_count
        for tw in self.tracked.values():
            age = now - tw.first_seen
            if age >= self.min_wall_age_sec and tw.stall_count >= 3:
                side = "short" if tw.wall.side == "ask" else "long"
                if not TrendFilter.allows_fade(side, trend_state):
                    continue  # не фейдим против сильного тренда
                # Стенка формально крупная, но если сразу за ней (глубже в
                # стакане) почти нет объёма - это одиночная заявка без
                # реальной поддержки, и она может не удержать цену. Фейдим
                # только стенки с достаточной "подложкой" позади.
                min_backup = tw.wall.usd * CFG.wall_backup_min_ratio
                if tw.wall.backup_usd < min_backup:
                    log.info("[%s] ABSORPTION %s у %.2f ПРОПУЩЕН: тонкая подложка "
                              "(%.0f USD за стенкой, нужно >= %.0f)",
                              self.symbol, side.upper(), tw.wall.price, tw.wall.backup_usd, min_backup)
                    self._log_wall_candidate(tw, side, snap, age, passed=False, reason="thin_backup")
                    tw.stall_count = 0
                    continue
                sig = Signal(
                    symbol=self.symbol,
                    side=side,
                    signal_type="absorption",
                    reference_price=tw.wall.price,
                    mid=snap.mid,
                    confidence=self._confidence(snap, side),
                    ts=now,
                    volatility_pct=trend_state.volatility_pct,
                )
                self._log_wall_candidate(tw, side, snap, age, passed=True, reason="")
                log.info("[%s] ABSORPTION %s у %.2f (стенка стоит %.1fs, %.0f USD, stall=%d)",
                          self.symbol, side.upper(), tw.wall.price, age, tw.max_usd, tw.stall_count)
                tw.stall_count = 0  # чтобы не спамить сигналами каждую секунду
                return sig

        return None

    def _confidence(self, snap: BookSnapshot, side: str, boost: float = 0.0) -> float:
        # дисбаланс объёма как усиливающий/ослабляющий фактор уверенности
        imb = snap.imbalance if side == "long" else (1 - snap.imbalance)
        base = 0.5
        if imb >= CFG.imbalance_threshold:
            base += 0.25
        return min(1.0, base + boost)

    # ------------------------------------------------------------------ #
    # Логирование кандидатов + исход через 1/3/5с (калибровочные данные) и
    # составной WALL_SCORE - см. заголовок файла.
    # ------------------------------------------------------------------ #

    def _wall_class(self, tw: "_TrackedWall") -> str:
        """STATIC - долго стоит почти неизменной. ACTIVE - получает fills и
        восполняется (замечен хотя бы один refill) - для ABSORPTION это
        намного более сильный сигнал реального интереса, чем просто размер."""
        return "ACTIVE" if tw.refill_count >= 1 else "STATIC"

    def _wall_score(self, tw: "_TrackedWall", age: float, executed_usd: float, zone_cancels: int) -> float:
        """
        Композитный скор 0..1 из компонент, которые по отдельности слабо
        предсказывают исход (см. обсуждение с пользователем - размер стенки
        сам по себе почти ничего не говорит). НЕ используется пока как жёсткий
        гейт - только логируется в WALL_CANDIDATE для последующей калибровки
        по накопленным WALL_OUTCOME (порог "на глаз" здесь так же ненадёжен,
        как WALL_BACKUP_MIN_RATIO=0.7 оказался ненадёжен на практике).
        """
        size_component = min(tw.wall.usd / max(self.base_wall_min_usd, 1), 3.0) / 3.0
        persistence_component = min(age / (self.min_wall_age_sec * 3), 1.0)
        refill_component = min(tw.refill_count / 3.0, 1.0)
        backup_component = min(tw.wall.backup_usd / max(tw.wall.usd * 0.5, 1), 1.0)
        executed_component = min(executed_usd / max(tw.wall.usd * 0.5, 1), 1.0)
        spoof_penalty = min(zone_cancels / 5.0, 1.0)
        score = (0.15 * size_component + 0.15 * persistence_component + 0.2 * refill_component +
                 0.15 * backup_component + 0.2 * executed_component + 0.15) - 0.2 * spoof_penalty
        return max(0.0, min(1.0, score))

    def _log_wall_candidate(self, tw: "_TrackedWall", side: str, snap: BookSnapshot, age: float,
                             passed: bool, reason: str, signal_type: str = "absorption"):
        now = time.time()
        if now - tw.last_candidate_log_ts < CANDIDATE_LOG_COOLDOWN_SEC:
            return  # не спамить - одна и та же стенка иначе логируется каждые ~100-300мс
        tw.last_candidate_log_ts = now

        executed = {"buy_usd": 0.0, "sell_usd": 0.0, "total_usd": 0.0}
        if self.trade_feed is not None:
            try:
                executed = self.trade_feed.executed_usd_near(
                    self.symbol, tw.wall.price, CFG.wall_backup_range_pct, min(age, 30.0))
            except Exception:
                pass  # калибровочный лог не должен ронять основную логику сигналов

        zone_cancels = self._zone_cancel_count(tw.wall.side, tw.wall.price)
        score = self._wall_score(tw, age, executed["total_usd"], zone_cancels)
        wall_class = self._wall_class(tw)

        cid = next(_next_candidate_id)
        mid0 = snap.mid
        log.info(
            "[%s] WALL_CANDIDATE id=%d type=%s side=%s price=%.2f size_usd=%.0f backup_usd=%.0f "
            "age=%.1fs stall=%d updates=%d refills=%d class=%s executed_buy=%.0f executed_sell=%.0f "
            "zone_cancels_5m=%d score=%.3f passed=%s reason=%s mid=%.2f",
            self.symbol, cid, signal_type, side, tw.wall.price, tw.wall.usd, tw.wall.backup_usd,
            age, tw.stall_count, tw.update_count, tw.refill_count, wall_class,
            executed["buy_usd"], executed["sell_usd"], zone_cancels, score, passed, reason, mid0,
        )
        for delay in CANDIDATE_OUTCOME_DELAYS_SEC:
            try:
                asyncio.create_task(self._log_candidate_outcome(cid, mid0, side, delay))
            except RuntimeError:
                pass  # нет активного event loop (например, юнит-тест вне asyncio) - пропускаем

    async def _log_candidate_outcome(self, cid: int, mid0: float, side: str, delay: float):
        await asyncio.sleep(delay)
        if not self.history or mid0 <= 0:
            return
        mid_now = self.history[-1].mid
        delta_pct = (mid_now - mid0) / mid0 * 100
        favorable = delta_pct > 0 if side == "long" else delta_pct < 0
        log.info("[%s] WALL_OUTCOME id=%d t=+%.0fs mid=%.2f delta_pct=%.4f favorable=%s",
                  self.symbol, cid, delay, mid_now, delta_pct, favorable)
