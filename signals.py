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
import json
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
# Расширено 18.08 (аудит стратегии, этап 1.2) с (1.0, 3.0, 5.0) - три точки не
# отличали "цена дёрнулась и откатила за первую секунду" от "устойчиво пошла
# и продолжила" (нужно и для калибровки shadow-оценок в этапах 3/4, и для
# понимания, на каком горизонте вообще есть статистический эдж).
CANDIDATE_OUTCOME_DELAYS_SEC = (0.1, 0.25, 0.5, 1.0, 2.0, 5.0, 10.0)
CANDIDATE_LOG_COOLDOWN_SEC = 5.0  # не спамить лог по одной и той же стенке каждые 100мс
# Частота ABSORPTION/BREAKOUT сигналов - отдельный периодический лог (этап 1.5
# аудита). Раньше эти цифры можно было получить только руками через grep по
# логам Render - теперь есть и явная сводка, и (через wall_event_log.py)
# запрашиваемая по type колонке история.
FREQUENCY_LOG_INTERVAL_SEC = 300.0

# Ширина "зоны" для истории отмен - в % от цены (не абсолютное число, чтобы
# одинаково работало и для BTC (~64000), и для ETH (~3000)).
CANCEL_ZONE_WIDTH_PCT = 0.05
# Было 300.0 (5 минут) - на реальном живом стакане BTC счётчик отмен в зоне
# спокойно доходит до 10-15 за 5 минут просто от обычного шевеления заявок
# маркет-мейкеров (перестановка на пару центов и т.п.), а не от настоящего
# спуфинга - из-за этого SPOOF_ZONE_CANCEL_MAX=4 (см. config.py, этап 2.1
# аудита) резал почти все сигналы подряд, включая реально прибыльные (см.
# разбор в проде 18.08 - несколько ABSORPTION-кандидатов с явно живой
# структурой (refill, крупная стенка) были отклонены как "spoof_zone_cancels",
# хотя цена после этого честно пошла в нужную сторону). Снижено до 10 секунд
# по прямой просьбе пользователя - так счётчик ловит именно БЫСТРЫЕ повторные
# отмены (это и есть характерный почерк спуфинга - выставил/снял/выставил за
# секунды), а не накопленный за долгое время нормальный шум обычного рынка.
CANCEL_HISTORY_LOOKBACK_SEC = 10.0


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
    # Размер стенки и подложки за ней (см. Wall.usd/backup_usd в market_data.py) -
    # используется risk.build_plan для структурного стопа: раньше стоп считался
    # от "вход-стенка" (~0, мы же входим ПРЯМО у стенки) и почти всегда падал на
    # голый MIN_STOP_PCT floor, никак не связанный со стаканом (см. обсуждение с
    # пользователем - "заходим по заявке 2кк, а стоп где-то у другой заявки").
    # Теперь дистанция масштабируется реальной глубиной за стенкой.
    wall_usd: float = 0.0
    backup_usd: float = 0.0


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
    # Когда последний раз реально ОТДАВАЛИ сигнал по этой стенке (не лог
    # кандидата, а настоящий Signal). Раньше единственной защитой от повторного
    # срабатывания был tw.stall_count = 0 после выдачи сигнала - но в проде
    # снепшоты Binance WS иногда приходят "пачкой" (несколько сообщений почти
    # одновременно, в пределах миллисекунд, а не штатным интервалом
    # BINANCE_WS_SPEED_MS) - в такой пачке mid почти не меняется между
    # соседними снепшотами пачки, поэтому "топчется" (stall) условие снова
    # истинно почти сразу, и stall_count успевает повторно дойти до
    # ABSORPTION_STALL_TICKS ещё ВНУТРИ этой же пачки - сигнал по факту одной
    # и той же стенки выдавался по 10-40 раз за миллисекунды (см. лог-спам
    # "СИГНАЛ ... confidence=0.75" повторяющийся с одинаковым timestamp).
    # Кроме бесполезной нагрузки на CPU/логи, каждый такой повтор запускал
    # отдельный asyncio.create_task(handle_signal(...)) - гонка между ними
    # разбирается отдельно в order_manager.py, но правильнее в принципе не
    # выдавать по факту один и тот же сигнал десятки раз подряд.
    last_signal_ts: float = 0.0


class SignalEngine:
    def __init__(self, symbol: str, history_len: int = 30, trend_filter: TrendFilter = None,
                 trade_feed=None, event_log=None):
        self.symbol = symbol
        # Персистентное SQLite-хранилище WALL_CANDIDATE/WALL_OUTCOME/shadow_evals -
        # см. wall_event_log.py (этап 1.3 аудита). None допустим (например, в
        # юнит-тестах) - все обращения обёрнуты и не роняют сигнальную логику.
        self.event_log = event_log
        self._freq_counts = {"absorption": 0, "breakout": 0}
        self._last_freq_log_ts = 0.0
        self.history: Deque[BookSnapshot] = deque(maxlen=history_len)
        self.tracked: Dict[float, _TrackedWall] = {}  # bucketed price -> _TrackedWall
        # стенка должна простоять хотя бы столько, чтобы считаться "реальной" -
        # см. CFG.min_wall_age_sec (было жёстко зашито 3.0, вынесено в настройку
        # из-за жалобы на слишком позднее подтверждение сигнала).
        self.min_wall_age_sec = CFG.min_wall_age_sec
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
        # Троттлинг лога "рынок мёртвый" - см. CFG.dead_range_min_pct ниже в on_snapshot.
        self._last_dead_market_log_ts = 0.0

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

        trend_state = self.trend_filter.update(snap.mid, snap.ts)
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
                    # Спуфинг-гейт по истории отмен в этой зоне (этап 2.1 аудита,
                    # 18.08) - zone_cancels_5m раньше только логировался в
                    # WALL_SCORE, ни на что не влияя. Порог не откалиброван по
                    # выборке (первая прикидка, как и DEAD_RANGE_MIN_PCT в своё
                    # время) - см. CFG.spoof_zone_cancel_max, отсечённые кандидаты
                    # по-прежнему логируются (reason=spoof_zone_cancels), чтобы
                    # можно было измерить, сколько реально отсекается и куда шла
                    # цена по факту.
                    zone_cancels_here = self._zone_cancel_count(tw.wall.side, tw.wall.price)
                    if zone_cancels_here >= CFG.spoof_zone_cancel_max:
                        log.info("[%s] BREAKOUT %s у %.2f ПРОПУЩЕН: подозрение на спуфинг зоны "
                                  "(%d отмен за 5 мин >= %d)", self.symbol, side.upper(), tw.wall.price,
                                  zone_cancels_here, CFG.spoof_zone_cancel_max)
                        self._log_wall_candidate(tw, side, snap, age, passed=False,
                                                  reason="spoof_zone_cancels", signal_type="breakout",
                                                  price_crossed=True, is_wall_disappearance=True)
                    else:
                        breakout_signal = Signal(
                            symbol=self.symbol,
                            side=side,
                            signal_type="breakout",
                            reference_price=tw.wall.price,
                            mid=snap.mid,
                            confidence=self._confidence(snap, side, boost=0.15),
                            ts=now,
                            volatility_pct=trend_state.volatility_pct,
                            wall_usd=tw.wall.usd,
                            backup_usd=tw.wall.backup_usd,
                        )
                        self._log_wall_candidate(tw, side, snap, age, passed=True, reason="",
                                                  signal_type="breakout", price_crossed=True,
                                                  is_wall_disappearance=True)
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
                                                  reason="cancelled_no_breakout", price_crossed=False,
                                                  is_wall_disappearance=True)
                del self.tracked[key]

        for key, w in seen_now.items():
            if key not in self.tracked:
                self.tracked[key] = _TrackedWall(wall=w, first_seen=now, last_seen=now, max_usd=w.usd,
                                                  initial_usd=w.usd, prev_usd=w.usd, min_usd_seen=w.usd,
                                                  update_count=1)

        if breakout_signal:
            return breakout_signal

        if False and trend_state.is_dead:
            # Отключено СНОВА 18.08 (третий раз за день - см. git-историю):
            # порог DEAD_RANGE_MIN_PCT/DEAD_RANGE_LOOKBACK_SEC (0.08% за 90с)
            # ловит только резкий рывок ПРЯМО СЕЙЧАС, а не медленный снос,
            # растянутый на несколько минут (реальный случай в проде -
            # диапазон за 90с был 0.01-0.03%, но за 9 минут цена суммарно
            # прошла ~0.08%) - по ощущению пользователя это не мёртвый рынок,
            # хотя формально попадает под порог. Раньше пробовали то включать,
            # то выключать это правило целиком - в этот раз, если понадобится
            # вернуть, разумнее сначала пересчитать окно/порог (шире
            # DEAD_RANGE_LOOKBACK_SEC или ниже DEAD_RANGE_MIN_PCT), а не просто
            # включать обратно то же самое. Код оставлен (if False and ...).
            # "Мёртвый" рынок - см. TrendState.is_dead/CFG.dead_range_min_pct.
            # Не глушим BREAKOUT (он уже отработан выше, до этой проверки) -
            # пробой сам по себе означает, что цена ТОЛЬКО ЧТО вышла из
            # диапазона, это конец боковика, а не его продолжение. Глушим
            # только ABSORPTION - именно фейды в обе стороны на одном и том
            # же шуме и вызывали серию мелких убытков (см. комментарий у
            # CFG.dead_range_min_pct). Логируем раз в CFG.absorption_min_refire_sec
            # на стенку не имеет смысла - тут глушим сразу все кандидаты одним
            # логом, чтобы не спамить.
            if now - self._last_dead_market_log_ts >= 10.0:
                log.info("[%s] рынок мёртвый: диапазон %.3f%% за %.0fs < %.3f%% - ABSORPTION приостановлен",
                          self.symbol, trend_state.range_pct, CFG.dead_range_lookback_sec,
                          CFG.dead_range_min_pct)
                self._last_dead_market_log_ts = now
            return None

        # ABSORPTION: ищем стенку, простоявшую достаточно и с накопленным stall_count
        for tw in self.tracked.values():
            age = now - tw.first_seen
            if age >= self.min_wall_age_sec and tw.stall_count >= CFG.absorption_stall_ticks:
                # Минимальный интервал между повторными сигналами по ОДНОЙ И ТОЙ
                # ЖЕ стенке - защита от пачек снепшотов, приходящих почти
                # одновременно (см. комментарий у _TrackedWall.last_signal_ts).
                # Не влияет на скорость ПЕРВОГО входа (min_wall_age_sec/
                # absorption_stall_ticks по-прежнему решают это) - только
                # глушит бессмысленные повторы одного и того же вывода.
                if now - tw.last_signal_ts < CFG.absorption_min_refire_sec:
                    continue
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
                    # Тот же дебаунс, что и у выданного сигнала (см. last_signal_ts
                    # выше) - без этого ОТКЛОНЁННЫЙ по тонкой подложке кандидат
                    # спамит "ПРОПУЩЕН: тонкая подложка" точно так же пачками
                    # внутри одной группы почти одновременных снепшотов, просто с
                    # другим текстом лога - обнаружено в проде после первого
                    # раунда фикса (дебаунс тогда стоял только на пути success).
                    tw.last_signal_ts = now
                    continue
                # Тот же спуфинг-гейт, что и у BREAKOUT выше (этап 2.1 аудита) -
                # см. комментарий там.
                zone_cancels_here = self._zone_cancel_count(tw.wall.side, tw.wall.price)
                if zone_cancels_here >= CFG.spoof_zone_cancel_max:
                    log.info("[%s] ABSORPTION %s у %.2f ПРОПУЩЕН: подозрение на спуфинг зоны "
                              "(%d отмен за 5 мин >= %d)", self.symbol, side.upper(), tw.wall.price,
                              zone_cancels_here, CFG.spoof_zone_cancel_max)
                    self._log_wall_candidate(tw, side, snap, age, passed=False, reason="spoof_zone_cancels")
                    tw.stall_count = 0
                    tw.last_signal_ts = now
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
                    wall_usd=tw.wall.usd,
                    backup_usd=tw.wall.backup_usd,
                )
                self._log_wall_candidate(tw, side, snap, age, passed=True, reason="")
                log.info("[%s] ABSORPTION %s у %.2f (стенка стоит %.1fs, %.0f USD, stall=%d)",
                          self.symbol, side.upper(), tw.wall.price, age, tw.max_usd, tw.stall_count)
                tw.stall_count = 0  # чтобы не спамить сигналами каждую секунду
                tw.last_signal_ts = now
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

    def _absorption_shadow_eval(self, tw: "_TrackedWall", side: str, executed_buy: float, executed_sell: float,
                                 buy_recent: float, sell_recent: float, zone_cancels: int) -> Tuple[bool, dict]:
        """
        Этап 3 аудита - НЕ гейт, только теневая (WOULD_ENTER/WOULD_SKIP) оценка
        новых критериев ABSORPTION для последующего сравнения с реальным
        WALL_OUTCOME (см. AUDIT_2026-08-18.md). Условия по прямому списку из
        запроса: реальная крупная стенка, есть aggressive flow В стенку, стенка
        держится, есть refill, давление начинает ослабевать, spoof risk низкий -
        просто существования стенки недостаточно.
        """
        # "Атакующий" агрессорский поток - та сторона тейпа, что реально давит
        # НА стенку: для ask-стенки (блокирует движение вверх) это покупки, для
        # bid-стенки - продажи.
        attacking_total = executed_buy if tw.wall.side == "ask" else executed_sell
        attacking_recent = buy_recent if tw.wall.side == "ask" else sell_recent
        attacking_older = max(attacking_total - attacking_recent, 0.0)

        criteria = {
            "real_wall": tw.wall.usd >= self.base_wall_min_usd,
            "aggressive_flow_into_wall": attacking_total >= tw.wall.usd * CFG.shadow_min_executed_ratio,
            "wall_holds": tw.update_count >= CFG.absorption_stall_ticks,
            "refill": tw.refill_count >= 1,
            # давление ослабевает: недавняя половина окна кормит стенку заметно
            # меньше, чем более старая половина (после того как поток вообще был).
            "pressure_weakening": (
                attacking_older > 0 and attacking_recent <= attacking_older * CFG.shadow_weakening_flow_ratio
            ),
            "spoof_risk_low": zone_cancels < CFG.spoof_zone_cancel_max,
        }
        return all(criteria.values()), criteria

    def _breakout_shadow_eval(self, tw: "_TrackedWall", side: str, age: float, executed_buy: float,
                               executed_sell: float, buy_recent: float, sell_recent: float,
                               zone_cancels: int, price_crossed: bool) -> Tuple[bool, dict]:
        """
        Этап 4 аудита - НЕ гейт, теневая оценка новых критериев BREAKOUT.
        Исчезновение стенки САМО ПО СЕБЕ не считается пробоем - нужно
        подтверждение реальным исполненным объёмом, что стенку именно съели
        (а не сняли/отодвинули), продолжение потока в сторону движения, и
        отсутствие признаков немедленного возврата уровня (приближается через
        zone_cancels - см. докстринг ниже, это приближение).
        """
        total_executed = executed_buy + executed_sell
        # continuation flow - поток В СТОРОНУ предполагаемого движения ПОСЛЕ
        # пробоя: long (стенка была ask, съедена вверх) - покупки должны
        # доминировать в недавнем окне; short - продажи.
        continuation_recent = buy_recent if side == "long" else sell_recent
        opposite_recent = sell_recent if side == "long" else buy_recent

        criteria = {
            "wall_existed_long_enough": age >= CFG.shadow_breakout_min_age_sec,
            "real_executed_volume": total_executed >= tw.max_usd * CFG.shadow_breakout_min_executed_ratio,
            "wall_actually_eaten": total_executed >= tw.max_usd * CFG.shadow_breakout_eaten_ratio,
            "price_passed_level": price_crossed,
            # Приближение "не было мгновенного refill" - используем ту же
            # историю отмен по зоне (zone_cancels), а не прямое наблюдение за
            # тем, появился ли уровень заново через N секунд (это потребовало
            # бы отдельного forward-looking трекинга, которого сейчас нет) -
            # НЕДОСТАТОК, если понадобится точнее - надо добавить отдельный
            # трекер "уровень появился снова в течение Xс после пробоя".
            "no_recent_spoof_history": zone_cancels < CFG.spoof_zone_cancel_max,
            "continuation_flow": continuation_recent > opposite_recent,
        }
        return all(criteria.values()), criteria

    def _log_wall_candidate(self, tw: "_TrackedWall", side: str, snap: BookSnapshot, age: float,
                             passed: bool, reason: str, signal_type: str = "absorption",
                             price_crossed: bool = False, is_wall_disappearance: bool = False) -> Optional[int]:
        """Возвращает id залогированного кандидата (для привязки shadow_evals -
        см. этапы 3/4 аудита) или None, если лог пропущен из-за дебаунса.
        is_wall_disappearance=True - вызов из ветки "стенка пропала" (реальный
        BREAKOUT или cancelled_no_breakout), а не из ABSORPTION stall-цикла -
        именно на этом множестве событий имеет смысл breakout_v2 shadow-оценка
        (см. _breakout_shadow_eval)."""
        now = time.time()
        if now - tw.last_candidate_log_ts < CANDIDATE_LOG_COOLDOWN_SEC:
            return None  # не спамить - одна и та же стенка иначе логируется каждые ~100-300мс
        tw.last_candidate_log_ts = now

        executed = {"buy_usd": 0.0, "sell_usd": 0.0, "total_usd": 0.0}
        # trend-бакеты (этап 1.4 аудита) - последняя половина lookback-окна vs
        # вся история, чтобы отличать нарастающий поток от затухающего (см.
        # BinanceTradeFeed.executed_usd_trend). Используются в этапах 3/4 для
        # shadow-оценки "давление ослабевает"/"continuation flow", здесь пока
        # только логируются вместе с кандидатом.
        buy_recent = sell_recent = 0.0
        if self.trade_feed is not None:
            try:
                lookback = min(age, 30.0)
                executed = self.trade_feed.executed_usd_near(
                    self.symbol, tw.wall.price, CFG.wall_backup_range_pct, lookback)
                trend_buckets = self.trade_feed.executed_usd_trend(
                    self.symbol, tw.wall.price, CFG.wall_backup_range_pct,
                    lookback_sec=min(lookback, 10.0), buckets=4)
                half = len(trend_buckets) // 2
                recent_buckets = trend_buckets[half:] if half else trend_buckets
                buy_recent = sum(b["buy_usd"] for b in recent_buckets)
                sell_recent = sum(b["sell_usd"] for b in recent_buckets)
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
            "executed_buy_recent=%.0f executed_sell_recent=%.0f zone_cancels_5m=%d score=%.3f "
            "passed=%s reason=%s mid=%.2f",
            self.symbol, cid, signal_type, side, tw.wall.price, tw.wall.usd, tw.wall.backup_usd,
            age, tw.stall_count, tw.update_count, tw.refill_count, wall_class,
            executed["buy_usd"], executed["sell_usd"], buy_recent, sell_recent,
            zone_cancels, score, passed, reason, mid0,
        )
        if self.event_log is not None:
            self.event_log.log_candidate(
                cid, self.symbol, signal_type, side, tw.wall.price, tw.wall.usd, tw.wall.backup_usd,
                age, tw.stall_count, tw.update_count, tw.refill_count, wall_class,
                executed["buy_usd"], executed["sell_usd"], buy_recent, sell_recent,
                zone_cancels, score, passed, reason, mid0,
            )
        if passed:
            self._freq_counts[signal_type] = self._freq_counts.get(signal_type, 0) + 1
        self._maybe_log_frequency_summary(now)

        # Shadow-оценка новой ABSORPTION/BREAKOUT логики (этапы 3/4 аудита) -
        # считается для КАЖДОГО кандидата (не только тех, что прошли текущие
        # гейты), чтобы можно было сравнить WOULD_ENTER с реальным
        # WALL_OUTCOME по той же cid независимо от решения текущей логики.
        # НЕ влияет на sig/breakout_signal - только логирование.
        if self.event_log is not None:
            try:
                if is_wall_disappearance:
                    would_enter_b, criteria_b = self._breakout_shadow_eval(
                        tw, side, age, executed["buy_usd"], executed["sell_usd"],
                        buy_recent, sell_recent, zone_cancels, price_crossed)
                    self.event_log.log_shadow_eval(cid, "breakout_v2", would_enter_b, json.dumps(criteria_b))
                else:
                    would_enter, criteria = self._absorption_shadow_eval(
                        tw, side, executed["buy_usd"], executed["sell_usd"],
                        buy_recent, sell_recent, zone_cancels)
                    self.event_log.log_shadow_eval(cid, "absorption_v2", would_enter, json.dumps(criteria))
            except Exception:
                pass  # shadow-логирование не должно ронять основную логику сигналов

        for delay in CANDIDATE_OUTCOME_DELAYS_SEC:
            try:
                asyncio.create_task(self._log_candidate_outcome(cid, mid0, side, delay))
            except RuntimeError:
                pass  # нет активного event loop (например, юнит-тест вне asyncio) - пропускаем
        return cid

    def _maybe_log_frequency_summary(self, now: float):
        """Отдельная периодическая сводка частоты ABSORPTION vs BREAKOUT (этап
        1.5 аудита) - раньше это можно было увидеть только руками, посчитав
        вхождения type= в текстовых логах."""
        if now - self._last_freq_log_ts < FREQUENCY_LOG_INTERVAL_SEC:
            return
        self._last_freq_log_ts = now
        log.info("[%s] FREQUENCY за последние ~%.0fs: absorption=%d breakout=%d",
                  self.symbol, FREQUENCY_LOG_INTERVAL_SEC,
                  self._freq_counts.get("absorption", 0), self._freq_counts.get("breakout", 0))
        self._freq_counts = {"absorption": 0, "breakout": 0}

    async def _log_candidate_outcome(self, cid: int, mid0: float, side: str, delay: float):
        await asyncio.sleep(delay)
        if not self.history or mid0 <= 0:
            return
        mid_now = self.history[-1].mid
        delta_pct = (mid_now - mid0) / mid0 * 100
        favorable = delta_pct > 0 if side == "long" else delta_pct < 0
        log.info("[%s] WALL_OUTCOME id=%d t=+%.0fs mid=%.2f delta_pct=%.4f favorable=%s",
                  self.symbol, cid, delay, mid_now, delta_pct, favorable)
        if self.event_log is not None:
            self.event_log.log_outcome(cid, delay, mid_now, delta_pct, favorable)
