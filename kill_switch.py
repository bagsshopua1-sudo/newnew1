"""
Kill switch — аварийная остановка. Срабатывает:
  - вручную (кнопка/POST-запрос на дашборде);
  - автоматически, когда RiskManager фиксирует пробитие дневного лимита
    убытка или серию убыточных сделок подряд;
  - автоматически при затяжном обрыве связи со стаканом (see market_data.py).

При срабатывании: отменяет все активные ордера на бирже и закрывает все
открытые позиции reduce-only ордерами, дальше блокирует новые входы.
Дашборд продолжает работать и после срабатывания — чтобы было видно, что
произошло и почему.
"""
import logging
import time
from typing import Optional

log = logging.getLogger("kill_switch")


class KillSwitch:
    def __init__(self):
        self.active = False
        self.reason: Optional[str] = None
        self.triggered_at: Optional[float] = None
        self._order_manager = None

    def bind(self, order_manager):
        self._order_manager = order_manager

    async def trigger(self, reason: str):
        if self.active:
            return
        self.active = True
        self.reason = reason
        self.triggered_at = time.time()
        log.warning("KILL SWITCH сработал (%s). Закрываю все позиции и отменяю ордера.", reason)
        if self._order_manager:
            try:
                await self._order_manager.flatten_all(reason)
            except Exception as e:
                log.error("Ошибка при аварийном закрытии позиций: %s", e)

    def reset(self):
        log.info("Kill switch сброшен вручную, торговля может возобновиться.")
        self.active = False
        self.reason = None
        self.triggered_at = None
