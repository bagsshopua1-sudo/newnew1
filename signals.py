"""
Сигнальный движок - EDGE-механика, полностью переписана 21.08 по прямому
запросу пользователя. Вся предыдущая логика (REAL_WALL/ABSORPTION/BREAKOUT/
SPOOF классификация по динамике executed-volume, microprice, трендовый
фильтр, детектор мёртвого рынка, история отмен по зоне) убрана целиком -
пользователь явно попросил не усложнять и заменить всё одной простой
механикой:

    ORDER BOOK -> EDGE -> ENTRY

Смотрим крупнейшую ОТДЕЛЬНУЮ заявку (не сумму) на каждой стороне глубокого
стакана Binance рядом с ценой - BookSnapshot.bid_top_usd/ask_top_usd (см.
market_data.py::analyze_book, отфильтровано по CFG.wall_max_distance_pct от
mid). Пример пользователя: BID=$1.2M / ASK=$200K -> явный LONG bias.

Но наличие стенки само по себе не значит edge - нужен РЕАЛЬНЫЙ wall
advantage ratio (favor_usd / oppose_usd), а не просто факт, что одна сторона
больше нуля. Пример пользователя: BID=$1.2M / ASK=$1.0M -> ratio=1.2 -> NO
TRADE, встречная ликвидность слишком большая, преимущество недостаточное.
CFG.wall_advantage_ratio_min - порог этого отношения (по умолчанию 2.5,
между "1.2 недостаточно" и "6.0 явно достаточно" из примеров пользователя).

Анти-спуф / анти-случайная-заявка (прямая просьба пользователя "не входить
из-за одной случайной заявки", "проверять, что крупная заявка держится
несколько обновлений"): одна и та же сторона EDGE должна оставаться лучшей
CFG.wall_confirm_updates обновлений стакана ПОДРЯД И минимум
CFG.wall_min_persist_sec секунд, прежде чем это станет реальным Signal - см.
SignalEngine ниже. Стенка, которая появилась и тут же исчезла (типичный
спуф), физически не успевает набрать нужное число подтверждений - отдельного
"антиспуф"-флага не нужно, это прямое следствие самого требования
персистентности, а не отдельная эвристика поверх него.

EXIT (постоянный пересчёт LONG EDGE / SHORT EDGE на уже открытой позиции,
CLOSE/REDUCE/HOLD) использует ТУ ЖЕ функцию compute_edge() ниже, но живёт в
order_manager.py::_watch_position - у него своя, более быстрая
персистентность (CFG.edge_exit_confirm_ticks), т.к. пользователь прямо
просил реагировать на разворот edge немедленно, а не только на входе.
"""
import logging
from dataclasses import dataclass
from typing import Optional

from config import CFG
from market_data import BookSnapshot

log = logging.getLogger("signals")


@dataclass
class Signal:
    symbol: str
    side: str  # "long" | "short"
    signal_type: str  # всегда "wall_edge" - поле оставлено ради совместимости с trade_log/dashboard
    reference_price: float  # цена EDGE-стенки - risk.build_plan считает стоп от неё
    mid: float
    confidence: float  # всегда 1.0 - либо EDGE прошёл все пороги ниже, либо сигнала вообще нет
    ts: float
    wall_usd: float = 0.0     # размер EDGE-стенки (USD)
    backup_usd: float = 0.0   # новой механикой не используется - оставлено ради сигнатуры risk.build_plan
    ratio: float = 0.0        # wall advantage ratio на момент сигнала (лог/дашборд)
    opposing_usd: float = 0.0
    # Базис между биржей сигнала (Binance) и биржей исполнения (Lighter) -
    # проставляется в bot.py._maybe_signal, см. risk.build_plan.
    exchange_basis: float = 0.0


@dataclass
class EdgeState:
    """Результат compute_edge() для одного снепшота книги."""
    side: Optional[str]  # "long"/"short" - чья сторона крупнее, ДАЖЕ если ratio не прошёл порог ниже
    qualifies: bool       # True, только если И favor_usd >= wall_min_usd, И ratio >= WALL_ADVANTAGE_RATIO_MIN
    favor_usd: float
    favor_price: float
    oppose_usd: float
    ratio: float           # favor_usd / oppose_usd (inf, если oppose_usd == 0, а favor_usd > 0)


def compute_edge(snap: BookSnapshot, wall_min_usd: float) -> EdgeState:
    """ORDER BOOK -> EDGE. Общая функция для входа (SignalEngine ниже) и для
    постоянного пересчёта на открытой позиции (order_manager._watch_position)."""
    if snap.bid_top_usd >= snap.ask_top_usd:
        favor_side, favor_usd, favor_price = "long", snap.bid_top_usd, snap.bid_top_price
        oppose_usd = snap.ask_top_usd
    else:
        favor_side, favor_usd, favor_price = "short", snap.ask_top_usd, snap.ask_top_price
        oppose_usd = snap.bid_top_usd

    if favor_usd <= 0:
        return EdgeState(side=None, qualifies=False, favor_usd=0.0, favor_price=0.0,
                          oppose_usd=oppose_usd, ratio=0.0)

    ratio = favor_usd / oppose_usd if oppose_usd > 0 else float("inf")
    qualifies = favor_usd >= wall_min_usd and ratio >= CFG.wall_advantage_ratio_min
    return EdgeState(side=favor_side, qualifies=qualifies, favor_usd=favor_usd,
                      favor_price=favor_price, oppose_usd=oppose_usd, ratio=ratio)


class SignalEngine:
    """
    ENTRY: копит подтверждения EDGE по снепшотам одного символа и выдаёт
    Signal РОВНО один раз за эпизод - в момент, когда порог подтверждений
    (CFG.wall_confirm_updates обновлений подряд И CFG.wall_min_persist_sec
    секунд) только что пройден. Дальнейшее удержание того же EDGE больше не
    порождает повторные сигналы (self._fired) - ждём, пока сторона сменится
    или EDGE пропадёт, прежде чем снова считать подтверждения с нуля.
    """

    def __init__(self, symbol: str):
        self.symbol = symbol
        self.wall_min_usd = CFG.binance_wall_min_usd if CFG.use_binance_signals else CFG.wall_min_usd
        self._side: Optional[str] = None
        self._first_seen_ts: float = 0.0
        self._confirm_count: int = 0
        self._fired: bool = False

    def on_snapshot(self, snap: BookSnapshot) -> Optional[Signal]:
        edge = compute_edge(snap, self.wall_min_usd)
        now = snap.ts

        if not edge.qualifies or edge.side != self._side:
            # Новый эпизод: сторона сменилась, EDGE пропал, или перестал
            # квалифицироваться (ratio/размер просели) - счётчик начинается
            # заново. Если прямо сейчас появилась НОВАЯ квалифицирующая
            # сторона - это уже первое подтверждение нового эпизода.
            self._side = edge.side if edge.qualifies else None
            self._first_seen_ts = now
            self._confirm_count = 1 if edge.qualifies else 0
            self._fired = False
            if edge.qualifies:
                log.info("[%s] EDGE появился: %s favor=%.0f oppose=%.0f ratio=%.2f "
                          "(нужно %d обновлений подряд и %.1fs)", self.symbol, edge.side.upper(),
                          edge.favor_usd, edge.oppose_usd, edge.ratio,
                          CFG.wall_confirm_updates, CFG.wall_min_persist_sec)
            return None

        # Та же сторона всё ещё квалифицируется, как и на прошлом тике.
        self._confirm_count += 1
        persisted_sec = now - self._first_seen_ts
        if self._fired or self._confirm_count < CFG.wall_confirm_updates \
                or persisted_sec < CFG.wall_min_persist_sec:
            return None

        self._fired = True
        log.info("[%s] EDGE ПОДТВЕРЖДЁН: %s у %.2f favor=%.0f oppose=%.0f ratio=%.2f "
                  "(держался %.1fs, %d обновлений подряд) -> СИГНАЛ", self.symbol, edge.side.upper(),
                  edge.favor_price, edge.favor_usd, edge.oppose_usd, edge.ratio,
                  persisted_sec, self._confirm_count)

        return Signal(
            symbol=self.symbol,
            side=edge.side,
            signal_type="wall_edge",
            reference_price=edge.favor_price,
            mid=snap.mid,
            confidence=1.0,
            ts=now,
            wall_usd=edge.favor_usd,
            backup_usd=0.0,
            ratio=edge.ratio,
            opposing_usd=edge.oppose_usd,
        )
