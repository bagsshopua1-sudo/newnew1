"""
Сигнальный движок: превращает поток снапшотов стакана (market_data.BookSnapshot)
в торговые сигналы long/short.

=== Архитектура (рестройка 19.08, финальный этап) ===
Раньше решение "входить или нет" принималось почти сразу по факту события в
стакане: стенка появилась/держится N тиков -> ABSORPTION; стенка исчезла и
цена её прошла -> BREAKOUT. Проблема (прямая формулировка пользователя): бот
реагирует на наличие крупной лимитки, но недостаточно хорошо понимает, что
реально происходит вокруг неё - решение было почти целиком завязано на факт
существования стенки и её displayed-размер.

Теперь решение перестроено вокруг события:
    NEW / CHANGED LIQUIDITY -> MARKET REACTION -> DECISION

Каждое значимое изменение стенки (появилась/долго держится/исчезла)
классифицируется РОВНО в одно из пяти состояний (см. _classify_persistence/
_classify_disappearance ниже):
    REAL_WALL  - заявка есть и защищает уровень, но недостаточно динамики
                 вокруг неё, чтобы на этом строить сделку.
    ABSORPTION - в стенку реально идёт агрессивный исполненный объём
                 (BinanceTradeFeed, не просто displayed size), стенка держится
                 и восполняется, а давление начинает ослабевать -> FADE.
    BREAKOUT   - стенка реально съедена исполненным объёмом (не просто
                 пропала из выдачи), цена прошла её уровень, и поток
                 продолжает идти в сторону движения -> вход ПО тренду.
    SPOOF      - стенка исчезла без достаточного реального исполнения -
                 похоже на снятую фейковую заявку, не сигнал.
    NO_EDGE    - ничего из вышеперечисленного не подтвердилось увереннно -
                 сделка НЕ открывается. Это ОСНОВНОЕ, ожидаемое состояние -
                 стратегия сознательно не пытается поднять winrate числом
                 фильтров, а ищет редкие моменты, где order flow реально даёт
                 edge (см. докстринги классификаторов).

Входит бот только по ABSORPTION (-> FADE) и BREAKOUT (-> CONTINUATION);
REAL_WALL/SPOOF/NO_EDGE логируются (см. _log_wall_event) для аудита, но не
порождают Signal - как и было задокументировано в этапах 3/4 аудита
(_absorption_shadow_eval/_breakout_shadow_eval, теперь переименованные в
_classify_persistence/_classify_disappearance): те критерии раньше только
считались и логировались как WOULD_ENTER, реального решения не меняли -
теперь это и есть реальный гейт (см. CFG.absorption_enabled=True и коммент
там же).

Ключевой принцип: НЕ размер стенки как главный сигнал, а динамика вокруг неё:
  - persistence (age/update_count/stall_count - сколько стенка реально стоит);
  - executed volume (BinanceTradeFeed.executed_usd_near - сколько реально
    прошло объёма через/у стенки, а не что видно в стакане);
  - refill (replenishment displayed size после просадки - признак реального
    интереса/iceberg, а не разовой заявки);
  - cancellation (_zone_cancel_count - история отмен в этой ценовой зоне);
  - aggressive flow trend (executed_usd_trend - нарастает или ослабевает
    поток В стенку, по бакетам времени, не одно число);
  - price reaction (price_crossed - прошла ли цена уровень фактически);
  - microprice (_microprice_bias/_microprice_weakening - независимое от
    тейпа подтверждение из самой книги: смещение "справедливой" цены
    относительно mid как опережающий индикатор давления);
  - nearby liquidity (backup_usd - подложка за стенкой, участвует в
    WALL_SCORE и confidence, но НЕ как жёсткий гейт - см. коммент у
    CFG.wall_backup_min_ratio, жёсткий порог здесь уже один раз резал
    100% сигналов);
  - Binance/Lighter price difference и латентность - учитываются НИЖЕ по
    потоку, в bot.py (signal_age_ms -> отмена входа, exchange_basis -> risk.py),
    не здесь: на этом уровне движок видит только книгу источника сигнала.

"Зона", а не точная цена - маркет-мейкер может подвинуть заявку на пару
центов, это не значит что стенка пропала (см. _find_shifted_match).
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

# Пять состояний классификации события ликвидности - см. докстринг модуля.
# Только ABSORPTION/BREAKOUT порождают реальный Signal.
STATE_REAL_WALL = "REAL_WALL"
STATE_ABSORPTION = "ABSORPTION"
STATE_BREAKOUT = "BREAKOUT"
STATE_SPOOF = "SPOOF"
STATE_NO_EDGE = "NO_EDGE"

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
    # Базис между биржей сигнала (Binance) и биржей исполнения (Lighter) на
    # момент сигнала: lighter_mid - binance_mid (знак важен). Проставляется в
    # bot.py._maybe_signal сразу после basis-проверки, тем же числом, что уже
    # используется для basis_pct. Нужен ТОЛЬКО для risk.build_plan - чтобы
    # скорректировать reference_price (цену стенки, снятую с Binance) на этот
    # базис перед расчётом дистанции стопа от РЕАЛЬНОЙ цены входа (Lighter).
    # Без этого raw_distance = |entry(Lighter) - wall(Binance)| включает в себя
    # межбиржевой разброс цен как будто это расстояние до уровня в стакане -
    # см. обсуждение с пользователем 19.08 (пример: стенка 68000 на Binance,
    # исполнение по 68300 на Lighter из-за базиса в 300$, "стоп" 300$ на самом
    # деле не про структуру рынка вообще). reference_price САМ не трогаем -
    # он всё ещё используется в Binance-пространстве для пост-входных проверок
    # тезиса (order_manager: price_broke_through, wall_max_distance и т.п.,
    # сравниваются с note_signal_snapshot - тоже Binance), там этот сдвиг был
    # бы уже не нужен и даже вреден.
    exchange_basis: float = 0.0


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

                # стенка реально пропала: классифицируем BREAKOUT / SPOOF / NO_EDGE
                # по реальной динамике, а не только по факту "пропала + цена прошла"
                # (см. _classify_disappearance и докстринг модуля).
                age = tw.last_seen - tw.first_seen
                was_close = tw.wall.distance_pct < CFG.wall_max_distance_pct
                price_crossed = (
                    (tw.wall.side == "ask" and snap.mid >= tw.wall.price) or
                    (tw.wall.side == "bid" and snap.mid <= tw.wall.price)
                )
                if was_close:
                    executed, buy_recent, sell_recent = self._executed_stats(tw.wall.price, age)
                    zone_cancels = self._zone_cancel_count(tw.wall.side, tw.wall.price)
                    state, side, reason = self._classify_disappearance(
                        tw, age, price_crossed, executed, buy_recent, sell_recent, zone_cancels)
                    self._log_wall_event(tw, side, snap, age, state, reason, executed, buy_recent,
                                          sell_recent, zone_cancels, event_kind="disappearance")
                    if state == STATE_BREAKOUT:
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
                        log.info("[%s] BREAKOUT %s у %.2f (стенка стояла %.1fs, %.0f USD, реально "
                                  "исполнено %.0f USD)", self.symbol, side.upper(), tw.wall.price, age,
                                  tw.max_usd, executed["total_usd"])
                    elif state == STATE_SPOOF:
                        # Похоже на снятую фейковую заявку - фиксируем как отмену в этой
                        # зоне (см. _zone_cancel_count), чтобы следующие кандидаты в той
                        # же зоне видели эту историю.
                        self._record_cancel(tw)
                        log.info("[%s] %s у %.2f: %s (side=%s, стояла %.1fs)", self.symbol,
                                  state, tw.wall.price, reason, side, age)
                    else:
                        log.info("[%s] %s у %.2f: %s (side=%s, стояла %.1fs)", self.symbol,
                                  state, tw.wall.price, reason, side, age)
                del self.tracked[key]

        for key, w in seen_now.items():
            if key not in self.tracked:
                self.tracked[key] = _TrackedWall(wall=w, first_seen=now, last_seen=now, max_usd=w.usd,
                                                  initial_usd=w.usd, prev_usd=w.usd, min_usd_seen=w.usd,
                                                  update_count=1)

        if breakout_signal:
            return breakout_signal

        # Старый блокировщик "мёртвого рынка" (is_dead по узкому диапазону цены)
        # УДАЛЁН 19.08 (финальный этап рестройки) - он трижды включался/
        # выключался за один день 18.08 (см. git-историю) и так и не нашёл
        # рабочего порога (то резал реальные медленные сносы, то пропускал
        # шум). Заменён по существу: новая классификация ABSORPTION ниже
        # (_classify_persistence) требует РЕАЛЬНОГО агрессивного исполненного
        # объёма именно в стенку и ослабевающего давления, а не просто "EMA не
        # разошлись" - фейды на шуме без реального потока теперь отсеиваются
        # тем, что для них попросту не набирается aggressive_flow_into_wall,
        # а не отдельным, плохо откалиброванным индикатором волатильности.
        # ABSORPTION_FLAT_MIN_VOLATILITY_PCT (см. ниже) остаётся отдельным,
        # более узким фильтром именно на случай trend=="flat" - тот факт, что
        # оба применяются вместе, не противоречие, а два разных среза одного
        # и того же требования "торговать на рывках, а не на боковике".

        # ABSORPTION: классификация РЕАЛЬНОЙ динамики вокруг стенки (см.
        # _classify_persistence и докстринг модуля) - НЕ факт наличия стенки и
        # НЕ её размер. Раньше это было отключено (CFG.absorption_enabled=False)
        # из-за слабой статистики старой (наивной) логики - см. коммент у
        # CFG.absorption_enabled в config.py, почему теперь снова включено.
        for tw in (self.tracked.values() if CFG.absorption_enabled else ()):
            age = now - tw.first_seen
            if tw.stall_count < CFG.absorption_stall_ticks:
                continue  # цена вообще ещё не топчется у этой стенки - рано классифицировать
            # Минимальный интервал между повторными классификациями по ОДНОЙ И
            # ТОЙ ЖЕ стенке - защита от пачек снепшотов, приходящих почти
            # одновременно (см. комментарий у _TrackedWall.last_signal_ts).
            if now - tw.last_signal_ts < CFG.absorption_min_refire_sec:
                continue

            side = "short" if tw.wall.side == "ask" else "long"
            if not TrendFilter.allows_fade(side, trend_state):
                continue  # не фейдим против сильного тренда - это не NO_EDGE, а просто не проверяем сейчас
            # allows_fade() блокирует фейд только ПРОТИВ явного тренда - при
            # trend=="flat" (боковик) пропускает в обе стороны. Требуем
            # минимальную реальную волатильность цены (не просто "EMA не
            # разошлись") - см. коммент выше про удалённый is_dead.
            if trend_state.trend == "flat" and \
                    trend_state.volatility_pct < CFG.absorption_flat_min_volatility_pct:
                log.info("[%s] ABSORPTION %s у %.2f ПРОПУЩЕН: боковик без движения "
                          "(volatility_pct=%.4f < %.4f, trend=flat)", self.symbol, side.upper(),
                          tw.wall.price, trend_state.volatility_pct,
                          CFG.absorption_flat_min_volatility_pct)
                tw.stall_count = 0
                tw.last_signal_ts = now
                continue

            executed, buy_recent, sell_recent = self._executed_stats(tw.wall.price, age)
            zone_cancels = self._zone_cancel_count(tw.wall.side, tw.wall.price)
            state, _side, reason = self._classify_persistence(
                tw, snap, age, executed, buy_recent, sell_recent, zone_cancels)
            self._log_wall_event(tw, side, snap, age, state, reason, executed, buy_recent,
                                  sell_recent, zone_cancels, event_kind="persistence")

            if state != STATE_ABSORPTION:
                # REAL_WALL/SPOOF/NO_EDGE - стенка есть/интересна, но edge не
                # подтверждён. NO TRADE - нормальное и частое состояние здесь,
                # не пытаемся дожать до сигнала дополнительными фильтрами.
                tw.stall_count = 0
                tw.last_signal_ts = now
                continue

            # НОВОЕ 19.08 - прямая жалоба пользователя ("какого хуя открывает
            # лонг когда лимитка в шорт на 3кк"): решение раньше не смотрело на
            # ПРОТИВОПОЛОЖНУЮ сторону стакана - если с другой стороны уже стоит
            # стенка того же/большего калибра, позиция тут же упрётся и
            # закроется в минус по dominance (см. _thesis_invalidated в
            # order_manager.py - та же логика "держащая vs встречная", тут
            # применяем ЕЁ ЖЕ на входе, а не только на выходе). НЕ применяется
            # к BREAKOUT - там своя стенка уже пробита, другая структура.
            opposing_walls = snap.ask_walls if side == "long" else snap.bid_walls
            if opposing_walls:
                nearest_opposing = min(opposing_walls, key=lambda w: w.distance_pct)
                if nearest_opposing.usd >= CFG.wall_min_usd and nearest_opposing.usd >= tw.wall.usd:
                    log.info("[%s] ABSORPTION %s у %.2f ПРОПУЩЕН: встречная стенка уже доминирует "
                              "(%.0f USD против нашей %.0f USD)", self.symbol, side.upper(),
                              tw.wall.price, nearest_opposing.usd, tw.wall.usd)
                    tw.stall_count = 0
                    tw.last_signal_ts = now
                    continue

            sig = Signal(
                symbol=self.symbol,
                side=side,
                signal_type="absorption",
                reference_price=tw.wall.price,
                mid=snap.mid,
                # Подмешиваем WALL_SCORE (см. _wall_score - size/persistence/
                # refill/backup/executed/spoof) как мягкую поправку к базовой
                # confidence, а не жёсткий гейт (см. докстринг модуля - nearby
                # liquidity/backup участвует именно так, не как hard-порог).
                confidence=self._confidence(snap, side, boost=0.1 * self._wall_score(
                    tw, age, executed["total_usd"], zone_cancels)),
                ts=now,
                volatility_pct=trend_state.volatility_pct,
                wall_usd=tw.wall.usd,
                backup_usd=tw.wall.backup_usd,
            )
            log.info("[%s] ABSORPTION %s у %.2f (стенка стоит %.1fs, %.0f USD, реально исполнено "
                      "%.0f USD, refills=%d)", self.symbol, side.upper(), tw.wall.price, age,
                      tw.max_usd, executed["total_usd"], tw.refill_count)
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
    # Классификация события ликвидности (REAL_WALL/ABSORPTION/BREAKOUT/SPOOF/
    # NO_EDGE - см. докстринг модуля) + логирование + исход через 1/3/5с.
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
        сам по себе почти ничего не говорит). Логируется для каждого события
        и подмешивается небольшой мягкой поправкой в Signal.confidence (см.
        on_snapshot) - НЕ используется как отдельный жёсткий гейт (см. коммент
        у CFG.wall_backup_min_ratio - жёсткий порог по этим же компонентам уже
        один раз резал почти 100% сигналов).
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

    def _executed_stats(self, price: float, age: float) -> Tuple[dict, float, float]:
        """
        Реальный исполненный объём (BinanceTradeFeed) рядом с уровнем price за
        последние lookback секунд, плюс отдельно "недавняя половина" окна
        (buy_recent/sell_recent) - нужно, чтобы отличать нарастающий поток от
        затухающего (см. BinanceTradeFeed.executed_usd_trend), а не одно
        суммарное число. Общий helper для классификаторов и логирования, чтобы
        не запрашивать trade_feed дважды за один и тот же тик по одной стенке.
        """
        executed = {"buy_usd": 0.0, "sell_usd": 0.0, "total_usd": 0.0}
        buy_recent = sell_recent = 0.0
        if self.trade_feed is not None:
            try:
                lookback = min(max(age, 0.5), 30.0)
                executed = self.trade_feed.executed_usd_near(
                    self.symbol, price, CFG.wall_backup_range_pct, lookback)
                trend_buckets = self.trade_feed.executed_usd_trend(
                    self.symbol, price, CFG.wall_backup_range_pct,
                    lookback_sec=min(lookback, 10.0), buckets=4)
                half = len(trend_buckets) // 2
                recent_buckets = trend_buckets[half:] if half else trend_buckets
                buy_recent = sum(b["buy_usd"] for b in recent_buckets)
                sell_recent = sum(b["sell_usd"] for b in recent_buckets)
            except Exception:
                pass  # калибровочные данные не должны ронять основную логику сигналов
        return executed, buy_recent, sell_recent

    def _microprice_bias(self, snap: BookSnapshot) -> float:
        """>0 - книга взвешена в сторону покупки (см. BookSnapshot.microprice),
        <0 - в сторону продажи, независимо от того, куда уже сдвинулся mid."""
        return (snap.microprice - snap.mid) / snap.mid * 100 if snap.mid else 0.0

    def _microprice_weakening(self, side: str) -> bool:
        """
        Независимое от исполненного тейпа подтверждение "давление ослабевает" -
        смотрит не на реальные сделки (BinanceTradeFeed), а на саму книгу
        (microprice) за последние снепшоты self.history. side - сторона ФЕЙДА
        (куда бы вошли): "short" значит фейдим ask-стенку, и там ожидаем, что
        покупательное давление (bias>0, тянет к ask) в недавних тиках слабее,
        чем было раньше в этом же окне - и наоборот для "long"/bid-стенки.
        Используется как ИЛИ вместе с pressure_weakening по тейпу в
        _classify_persistence - две независимые меры одного явления снижают
        риск того, что одна зашумленная метрика в одиночку решит классификацию.
        """
        if len(self.history) < 6:
            return False
        biases = [self._microprice_bias(s) for s in self.history]
        half = len(biases) // 2
        older, recent = biases[:half], biases[half:]
        if not older or not recent:
            return False
        older_avg = sum(older) / len(older)
        recent_avg = sum(recent) / len(recent)
        if side == "short":
            # фейдим ask - ослабевает покупательное давление (bias>0 -> к 0/отрицательному)
            return recent_avg <= older_avg * CFG.shadow_weakening_flow_ratio if older_avg > 0 else recent_avg <= 0
        # фейдим bid (side == "long") - ослабевает продавливающее давление (bias<0 -> к 0/положительному)
        return recent_avg >= older_avg * CFG.shadow_weakening_flow_ratio if older_avg < 0 else recent_avg >= 0

    def _classify_persistence(self, tw: "_TrackedWall", snap: BookSnapshot, age: float, executed: dict,
                               buy_recent: float, sell_recent: float, zone_cancels: int) -> Tuple[str, str, str]:
        """
        Стенка ещё в книге (не пропала) и цена топчется у неё - классифицирует
        в REAL_WALL / ABSORPTION / SPOOF / NO_EDGE. Раньше (до рестройки 19.08)
        это были критерии "теневой" оценки (_absorption_shadow_eval, этап 3
        аудита 18.08) - теперь это и есть реальный гейт входа (см. коммент у
        CFG.absorption_enabled и докстринг модуля).
        """
        side = "short" if tw.wall.side == "ask" else "long"
        # "Атакующий" агрессорский поток - та сторона тейпа, что реально давит
        # НА стенку: для ask-стенки (блокирует движение вверх) это покупки, для
        # bid-стенки - продажи.
        attacking_total = executed["buy_usd"] if tw.wall.side == "ask" else executed["sell_usd"]
        attacking_recent = buy_recent if tw.wall.side == "ask" else sell_recent
        attacking_older = max(attacking_total - attacking_recent, 0.0)

        real_wall = tw.wall.usd >= self.base_wall_min_usd
        wall_holds = tw.update_count >= CFG.absorption_stall_ticks
        spoof_risk_low = zone_cancels < CFG.spoof_zone_cancel_max
        aggressive_flow_into_wall = attacking_total >= tw.wall.usd * CFG.shadow_min_executed_ratio
        refill = tw.refill_count >= 1
        pressure_weakening_flow = (
            attacking_older > 0 and attacking_recent <= attacking_older * CFG.shadow_weakening_flow_ratio
        )
        pressure_weakening = pressure_weakening_flow or self._microprice_weakening(side)

        if not real_wall:
            return STATE_NO_EDGE, side, "wall_too_small"
        if not wall_holds:
            return STATE_NO_EDGE, side, "not_enough_persistence_yet"
        if not spoof_risk_low:
            return STATE_SPOOF, side, "spoof_zone_cancels"
        if aggressive_flow_into_wall and refill and pressure_weakening:
            return STATE_ABSORPTION, side, ""
        if not aggressive_flow_into_wall:
            return STATE_REAL_WALL, side, "holds_without_real_aggressive_flow"
        return STATE_REAL_WALL, side, "holds_but_no_refill_or_pressure_not_weakening_yet"

    def _classify_disappearance(self, tw: "_TrackedWall", age: float, price_crossed: bool, executed: dict,
                                 buy_recent: float, sell_recent: float, zone_cancels: int) -> Tuple[str, str, str]:
        """
        Стенка реально пропала из книги - классифицирует в BREAKOUT / SPOOF /
        NO_EDGE. Исчезновение стенки САМО ПО СЕБЕ не считается пробоем (это и
        была главная слабость старой логики) - нужно подтверждение реальным
        исполненным объёмом, что стенку именно съели (а не сняли/отодвинули),
        и продолжение потока в сторону движения ПОСЛЕ исчезновения. Раньше это
        были критерии "теневой" оценки (_breakout_shadow_eval, этап 4 аудита
        18.08) - теперь реальный гейт (см. докстринг модуля).
        """
        side = "long" if tw.wall.side == "ask" else "short"  # сторона пробоя
        total_executed = executed["total_usd"]
        # continuation flow - поток В СТОРОНУ предполагаемого движения ПОСЛЕ
        # пробоя: long (стенка была ask, съедена вверх) - покупки должны
        # доминировать в недавнем окне; short - продажи.
        continuation_recent = buy_recent if side == "long" else sell_recent
        opposite_recent = sell_recent if side == "long" else buy_recent

        existed_long_enough = age >= self.min_wall_age_sec
        real_executed_volume = total_executed >= tw.max_usd * CFG.shadow_breakout_min_executed_ratio
        wall_actually_eaten = total_executed >= tw.max_usd * CFG.shadow_breakout_eaten_ratio
        continuation_flow = continuation_recent > opposite_recent
        spoof_risk_low = zone_cancels < CFG.spoof_zone_cancel_max

        if not spoof_risk_low:
            return STATE_SPOOF, side, "spoof_zone_cancels"
        if not real_executed_volume:
            return STATE_SPOOF, side, "vanished_without_real_executed_volume"
        if existed_long_enough and price_crossed and wall_actually_eaten and continuation_flow:
            return STATE_BREAKOUT, side, ""
        if not price_crossed:
            return STATE_NO_EDGE, side, "eaten_but_price_did_not_follow_through"
        return STATE_NO_EDGE, side, "insufficient_confirmation"

    def _log_wall_event(self, tw: "_TrackedWall", side: str, snap: BookSnapshot, age: float, state: str,
                         reason: str, executed: dict, buy_recent: float, sell_recent: float,
                         zone_cancels: int, event_kind: str) -> Optional[int]:
        """Логирует классифицированное событие (WALL_CANDIDATE, имя оставлено
        для совместимости с существующими дашбордами/grep-запросами по
        Render-логам) + планирует запись исхода через
        CANDIDATE_OUTCOME_DELAYS_SEC (WALL_OUTCOME) для последующего аудита -
        независимо от того, породило ли событие реальный Signal. Возвращает id
        кандидата или None, если лог пропущен из-за дебаунса по стенке."""
        now = time.time()
        if now - tw.last_candidate_log_ts < CANDIDATE_LOG_COOLDOWN_SEC:
            return None  # не спамить - одна и та же стенка иначе логируется каждые ~100-300мс
        tw.last_candidate_log_ts = now

        score = self._wall_score(tw, age, executed["total_usd"], zone_cancels)
        wall_class = self._wall_class(tw)
        passed = state in (STATE_ABSORPTION, STATE_BREAKOUT)
        signal_type = "breakout" if event_kind == "disappearance" else "absorption"

        cid = next(_next_candidate_id)
        mid0 = snap.mid
        log.info(
            "[%s] WALL_CANDIDATE id=%d type=%s side=%s price=%.2f size_usd=%.0f backup_usd=%.0f "
            "age=%.1fs stall=%d updates=%d refills=%d class=%s executed_buy=%.0f executed_sell=%.0f "
            "executed_buy_recent=%.0f executed_sell_recent=%.0f zone_cancels_5m=%d score=%.3f "
            "state=%s passed=%s reason=%s mid=%.2f",
            self.symbol, cid, signal_type, side, tw.wall.price, tw.wall.usd, tw.wall.backup_usd,
            age, tw.stall_count, tw.update_count, tw.refill_count, wall_class,
            executed["buy_usd"], executed["sell_usd"], buy_recent, sell_recent,
            zone_cancels, score, state, passed, reason, mid0,
        )
        if self.event_log is not None:
            self.event_log.log_candidate(
                cid, self.symbol, signal_type, side, tw.wall.price, tw.wall.usd, tw.wall.backup_usd,
                age, tw.stall_count, tw.update_count, tw.refill_count, wall_class,
                executed["buy_usd"], executed["sell_usd"], buy_recent, sell_recent,
                zone_cancels, score, passed, f"{state}:{reason}" if reason else state, mid0,
            )
            try:
                self.event_log.log_shadow_eval(cid, "classification", passed,
                                                json.dumps({"state": state, "reason": reason}))
            except Exception:
                pass  # калибровочные данные не должны ронять основную логику сигналов
        if passed:
            self._freq_counts[signal_type] = self._freq_counts.get(signal_type, 0) + 1
        self._maybe_log_frequency_summary(now)

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
