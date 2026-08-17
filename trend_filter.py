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


class TrendFilter:
    def __init__(self, fast_period_sec: float = 15, slow_period_sec: float = 90,
                 vol_lookback: int = 60, vol_spike_mult: float = 3.0):
        self.fast_alpha = 2 / (fast_period_sec + 1)
        self.slow_alpha = 2 / (slow_period_sec + 1)
        self.ema_fast: Optional[float] = None
        self.ema_slow: Optional[float] = None
        self.returns: Deque[float] = deque(maxlen=vol_lookback)
        self.last_mid: Optional[float] = None
        self.vol_spike_mult = vol_spike_mult
        self.baseline_vol: Optional[float] = None

    def update(self, mid: float) -> TrendState:
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

        return TrendState(ema_fast=self.ema_fast, ema_slow=self.ema_slow, trend=trend,
                           volatility_pct=vol, high_volatility=high_vol)

    @staticmethod
    def allows_fade(side: str, state: TrendState) -> bool:
        """Не даём фейдить (absorption) против сильного тренда."""
        if state.trend == "up" and side == "short":
            return False
        if state.trend == "down" and side == "long":
            return False
        return True
