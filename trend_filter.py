"""
Трендовый и волатильностный фильтр — работает только на потоке mid-цены
из стакана, без обращения к бирже за свечами.

Две функции:
  - не даёт сигналам "абсорбция" (фейд/контр-тренд) идти против сильного
    тренда (по кресту двух EMA);
  - глушит генерацию сигналов при аномальном всплеске волатильности
    (резкий скачок стандартного отклонения последних движений цены) —
    это как раз моменты вроде новостных свечей/ликвидаций, где стакан
    ведёт себя не по обычной логике "стенок".
"""
import statistics
from collections import deque
from dataclasses import dataclass
from typing import Deque, Optional


@dataclass
class TrendState:
    ema_fast: Optional[float] = None
    ema_slow: Optional[float] = None
    trend: str = "flat"  # "up" | "down" | "flat"
    volatility_pct: float = 0.0
    high_volatility: bool = False
    # "Мёртвый" рынок - цена уже DEAD_RANGE_LOOKBACK_SEC секунд не выходит за
    # пределы DEAD_RANGE_MIN_PCT (см. TrendFilter.dead_range_lookback_sec ниже).
    # allows_fade() блокирует фейд только ПРОТИВ тренда - при trend=="flat"
    # (ровно этот случай) ABSORPTION пропускается в ОБЕ стороны без ограничений
    # (см. комментарий у CFG.reentry_cooldown_sec в config.py - там же прямо
    # написано, что это отдельная нерешённая тема). Реальный случай в проде
    # 18.08: BTC простоял в коридоре ~25 пунктов (0.04%) 6 минут подряд, бот
    # за это время открыл 8 сделок (фейды в обе стороны на один и тот же
    # шум) - 8 из 8 в минус, только на проскальзывании/буфере исполнения,
    # реального движения ловить было нечего.
    range_pct: float = 0.0
    is_dead: bool = False


class TrendFilter:
    def __init__(self, fast_period_sec: float = 15, slow_period_sec: float = 90,
                 vol_lookback: int = 60, vol_spike_mult: float = 3.0,
                 dead_range_lookback_sec: float = 90.0, dead_range_min_pct: float = 0.08,
                 dead_range_min_coverage_sec: float = 45.0):
        self.fast_alpha = 2 / (fast_period_sec + 1)
        self.slow_alpha = 2 / (slow_period_sec + 1)
        self.ema_fast: Optional[float] = None
        self.ema_slow: Optional[float] = None
        self.returns: Deque[float] = deque(maxlen=vol_lookback)
        self.last_mid: Optional[float] = None
        self.vol_spike_mult = vol_spike_mult
        self.baseline_vol: Optional[float] = None
        # Скользящее окно (ts, mid) за последние dead_range_lookback_sec секунд -
        # по КОЛИЧЕСТВУ времени, а не по числу снепшотов (частота фидов от
        # Binance плавает, поэтому фиксированный maxlen давал бы то 10 секунд
        # истории, то минуту - в зависимости от того, как часто шлёт WS).
        self.mid_window: Deque[tuple] = deque()
        self.dead_range_lookback_sec = dead_range_lookback_sec
        self.dead_range_min_pct = dead_range_min_pct
        # Не считаем рынок "мёртвым" по первым секундам после старта/рестарта -
        # окно ещё не набралось, диапазон=0 был бы ложным срабатыванием.
        self.dead_range_min_coverage_sec = dead_range_min_coverage_sec

    def update(self, mid: float, ts: Optional[float] = None) -> TrendState:
        if self.ema_fast is None:
            self.ema_fast = mid
            self.ema_slow = mid
        else:
            self.ema_fast += self.fast_alpha * (mid - self.ema_fast)
            self.ema_slow += self.slow_alpha * (mid - self.ema_slow)

        if self.last_mid:
            self.returns.append((mid - self.last_mid) / self.last_mid)
        self.last_mid = mid

        vol = statistics.pstdev(self.returns) * 100 if len(self.returns) >= 10 else 0.0

        if self.baseline_vol is None and len(self.returns) >= min(30, self.returns.maxlen):
            self.baseline_vol = max(vol, 1e-6)
        high_vol = bool(self.baseline_vol and vol > self.baseline_vol * self.vol_spike_mult and vol > 0.03)

        diff_pct = (self.ema_fast - self.ema_slow) / self.ema_slow * 100 if self.ema_slow else 0.0
        if diff_pct > 0.03:
            trend = "up"
        elif diff_pct < -0.03:
            trend = "down"
        else:
            trend = "flat"

        range_pct = 0.0
        is_dead = False
        if ts is not None:
            self.mid_window.append((ts, mid))
            while self.mid_window and ts - self.mid_window[0][0] > self.dead_range_lookback_sec:
                self.mid_window.popleft()
            coverage_sec = ts - self.mid_window[0][0] if self.mid_window else 0.0
            if coverage_sec >= self.dead_range_min_coverage_sec:
                mids = [m for _, m in self.mid_window]
                range_pct = (max(mids) - min(mids)) / mid * 100 if mid else 0.0
                is_dead = range_pct < self.dead_range_min_pct

        return TrendState(ema_fast=self.ema_fast, ema_slow=self.ema_slow, trend=trend,
                           volatility_pct=vol, high_volatility=high_vol,
                           range_pct=range_pct, is_dead=is_dead)

    @staticmethod
    def allows_fade(side: str, state: TrendState) -> bool:
        """Не даём фейдить (absorption) против сильного тренда."""
        if state.trend == "up" and side == "short":
            return False
        if state.trend == "down" and side == "long":
            return False
        return True
