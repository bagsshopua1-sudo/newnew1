"""
Обёртка над lighter-sdk: подключение, определение market_index по символу,
масштабирование цены/размера, единый интерфейс create/modify/cancel ордера
для трёх режимов (collect / paper / live).
"""
import asyncio
import logging
from dataclasses import dataclass
from typing import Dict, Optional

import lighter

from config import CFG

log = logging.getLogger("exchange")


def _endpoint():
    return lighter.MAINNET if CFG.network == "mainnet" else lighter.TESTNET


@dataclass
class MarketInfo:
    market_index: int
    symbol: str
    price_decimals: int
    size_decimals: int
    min_base_amount: float

    def price_to_int(self, price: float) -> int:
        return int(round(price * (10 ** self.price_decimals)))

    def price_to_float(self, price_int: int) -> float:
        return price_int / (10 ** self.price_decimals)

    def size_to_int(self, size: float) -> int:
        return int(round(size * (10 ** self.size_decimals)))

    def size_to_float(self, size_int: int) -> float:
        return size_int / (10 ** self.size_decimals)


class ExchangeClient:
    """
    Единая точка входа к бирже Lighter.
    В режиме collect работает без ключей (только публичные REST/WS данные).
    В режиме paper использует lighter.PaperClient (симуляция на реальном стакане).
    В режиме live использует lighter.SignerClient (реальные сделки).
    """

    def __init__(self):
        self.endpoint = _endpoint()
        self.api_client = lighter.ApiClient(
            configuration=lighter.Configuration(host=self.endpoint.api_url)
        )
        self.order_api = lighter.OrderApi(self.api_client)
        self.account_api = lighter.AccountApi(self.api_client)
        self.markets: Dict[str, MarketInfo] = {}

        self.signer: Optional[lighter.SignerClient] = None
        self.paper: Optional[lighter.PaperClient] = None
        self._auth_token: Optional[str] = None

        if CFG.mode == "live":
            self.signer = lighter.SignerClient(
                url=self.endpoint.api_url,
                account_index=CFG.account_index,
                api_private_keys={CFG.api_key_index: CFG.api_private_key},
            )
        elif CFG.mode == "paper":
            # PaperClient сам поднимает отдельный WS-листенер на стакан для симуляции
            # исполнения - ему, как и основному MarketData, нужен ?encoding=json в URL,
            # иначе он получит бинарный фрейм и упадёт с TypeError при первом сообщении.
            raw_ws_url = self.endpoint.ws_url
            sep = "&" if "?" in raw_ws_url else "?"
            paper_ws_url = f"{raw_ws_url}{sep}encoding=json"
            self.paper = lighter.PaperClient(
                api_client=self.api_client,
                initial_collateral_usdc=CFG.account_equity_usd,
                order_api=self.order_api,
                ws_url=paper_ws_url,
            )

    async def resolve_markets(self):
        """Находит market_index и точность цены/размера для каждого символа из конфига."""
        resp = await self.order_api.order_books()
        by_symbol = {m.symbol: m for m in resp.order_books}
        for sym in CFG.symbols:
            if sym not in by_symbol:
                available = ", ".join(sorted(by_symbol.keys()))
                raise SystemExit(
                    f"Символ {sym} не найден на Lighter ({CFG.network}). "
                    f"Доступные рынки: {available}"
                )
            m = by_symbol[sym]
            details = await self.order_api.order_book_details(market_id=m.market_id)
            d = details.order_book_details[0]
            self.markets[sym] = MarketInfo(
                market_index=m.market_id,
                symbol=sym,
                price_decimals=d.price_decimals,
                size_decimals=d.size_decimals,
                min_base_amount=float(d.min_base_amount),
            )
            log.info("Рынок %s -> market_index=%s price_dec=%s size_dec=%s",
                      sym, m.market_id, d.price_decimals, d.size_decimals)

        if self.paper:
            # PaperClient требует явного track_market() перед create_paper_order -
            # без этого он падает с ValueError("market not tracked"). Это поднимает
            # собственный WS-листенер на стакан внутри PaperClient (независимо от
            # MarketData) и ждёт первый снепшот, поэтому может занять пару секунд.
            for sym, market in self.markets.items():
                await self.paper.track_market(market.market_index)
                log.info("Paper-трекинг стакана %s (market_index=%s) запущен", sym, market.market_index)

        return self.markets

    async def auth_token(self) -> str:
        if CFG.mode != "live":
            return ""
        token, err = self.signer.create_auth_token_with_expiry()
        if err:
            raise RuntimeError(f"Не удалось создать auth token: {err}")
        self._auth_token = token
        return token

    # ------------------------------------------------------------------ #
    # Единый интерфейс ордеров. paper/live выполняют реально,
    # collect только логирует (использовать в MODE=collect не нужно).
    # ------------------------------------------------------------------ #

    async def place_limit_order(self, market: MarketInfo, client_order_index: int,
                                 size: float, price: float, is_ask: bool,
                                 reduce_only: bool = False, post_only: bool = True):
        tif = (lighter.SignerClient.ORDER_TIME_IN_FORCE_POST_ONLY if post_only
               else lighter.SignerClient.ORDER_TIME_IN_FORCE_GOOD_TILL_TIME)
        if CFG.mode == "live":
            tx, resp, err = self.signer.create_order(
                market_index=market.market_index,
                client_order_index=client_order_index,
                base_amount=market.size_to_int(size),
                price=market.price_to_int(price),
                is_ask=is_ask,
                order_type=lighter.SignerClient.ORDER_TYPE_LIMIT,
                time_in_force=tif,
                reduce_only=reduce_only,
            )
            if err:
                log.error("create_order error: %s", err)
            return tx, resp, err
        elif CFG.mode == "paper":
            # У lighter.PaperOrderType нет LIMIT (есть только IOC и MARKET) - IOC
            # ближе всего к нашей семантике "лимитка по touch-цене, не исполнилось
            # сразу -> остаток отменяется" (chase-цикл сам переставляет её заново).
            # PaperOrderRequest также не принимает reduce_only - PaperClient сам
            # неттингует позицию по знаку размера, отдельный флаг не нужен.
            side = lighter.PaperOrderSide.SELL if is_ask else lighter.PaperOrderSide.BUY
            req = lighter.PaperOrderRequest(
                market_id=market.market_index,
                side=side,
                order_type=lighter.PaperOrderType.IOC,
                base_amount=round(size, market.size_decimals),
                price=round(price, market.price_decimals),
            )
            result = await self.paper.create_paper_order(req)
            return result, None, None
        else:
            log.info("[COLLECT] (симуляция лога) лимитка %s size=%s price=%s reduce_only=%s",
                      "SELL" if is_ask else "BUY", size, price, reduce_only)
            return None, None, None

    async def modify_order(self, market: MarketInfo, order_index: int, size: float, price: float):
        if CFG.mode == "live":
            return self.signer.modify_order(
                market_index=market.market_index,
                order_index=order_index,
                base_amount=market.size_to_int(size),
                price=market.price_to_int(price),
            )
        # PaperClient сейчас не даёт modify -> эмулируем cancel+create в order_manager.
        return None, None, "modify not supported in this mode, use cancel+create"

    async def cancel_order(self, market: MarketInfo, order_index: int):
        if CFG.mode == "live":
            return self.signer.cancel_order(market_index=market.market_index, order_index=order_index)
        log.info("[%s] отмена ордера order_index=%s (виртуально/лог)", CFG.mode, order_index)
        return None, None, None

    async def create_sl_order(self, market: MarketInfo, client_order_index: int,
                               size: float, trigger_price: float, is_ask: bool):
        """Нативный стоп-лосс (reduce-only, срабатывает по триггер-цене)."""
        if CFG.mode == "live":
            return self.signer.create_sl_order(
                market_index=market.market_index,
                client_order_index=client_order_index,
                base_amount=market.size_to_int(size),
                trigger_price=market.price_to_int(trigger_price),
                price=market.price_to_int(trigger_price),
                is_ask=is_ask,
                reduce_only=True,
            )
        log.info("[%s] SL выставлен: triggger=%s size=%s side=%s",
                  CFG.mode, trigger_price, size, "SELL" if is_ask else "BUY")
        return None, None, None

    async def create_tp_order(self, market: MarketInfo, client_order_index: int,
                               size: float, trigger_price: float, is_ask: bool):
        """Нативный тейк-профит (reduce-only)."""
        if CFG.mode == "live":
            return self.signer.create_tp_order(
                market_index=market.market_index,
                client_order_index=client_order_index,
                base_amount=market.size_to_int(size),
                trigger_price=market.price_to_int(trigger_price),
                price=market.price_to_int(trigger_price),
                is_ask=is_ask,
                reduce_only=True,
            )
        log.info("[%s] TP выставлен: trigger=%s size=%s side=%s",
                  CFG.mode, trigger_price, size, "SELL" if is_ask else "BUY")
        return None, None, None

    async def get_position(self, market: MarketInfo) -> Optional[dict]:
        """
        Возвращает текущую позицию по рынку: {"size": float (со знаком), "avg_entry": float}
        или None, если позиции нет. size > 0 = long, size < 0 = short.
        paper -> PaperClient.get_position; live -> AccountApi.account(...).positions.
        """
        if CFG.mode == "paper":
            # PaperClient.get_position - обычный (не async) метод, await на нём
            # падает с TypeError; поле размера у PaperPosition называется "size",
            # не "position".
            pos = self.paper.get_position(market.market_index)
            if pos is None:
                return None
            size = float(pos.size)
            if size == 0:
                return None
            return {"size": size, "avg_entry": float(pos.avg_entry_price)}

        if CFG.mode == "live":
            resp = await self.account_api.account(by="index", value=str(CFG.account_index))
            if not resp.accounts:
                return None
            acc = resp.accounts[0]
            for p in (acc.positions or []):
                if p.market_id == market.market_index:
                    size = float(p.position or 0)
                    if p.sign is not None and int(p.sign) < 0:
                        size = -abs(size)
                    if size == 0:
                        return None
                    return {"size": size, "avg_entry": float(p.avg_entry_price or 0)}
            return None

        return None  # collect mode: позиций нет

    async def get_active_order_by_client_index(self, market: MarketInfo, client_order_index: int):
        """Ищет активный ордер по client_order_index (для проверки исполнения лимитки, только live)."""
        if CFG.mode != "live":
            return None
        token = await self.auth_token()
        resp = await self.order_api.account_active_orders(
            authorization=token, account_index=CFG.account_index, market_id=market.market_index,
        )
        for o in (resp.orders or []):
            if o.client_order_index == client_order_index:
                return o
        return None

    async def close(self):
        if self.signer:
            await self.signer.close()
        if self.paper:
            await self.paper.close()
        await self.api_client.close()

    async def reset_paper_account(self):
        """
        Сброс "счёта" бота в paper-режиме по запросу с дашборда - см.
        Dashboard.handle_reset_account / OrderManager.reset_account.
        lighter.PaperClient хранит СВОЙ ВНУТРЕННИЙ баланс/позиции отдельно от
        RiskManager.equity (тот - только для расчёта размера позиции и показа
        на дашборде) - обнулить его можно только пересозданием клиента, метода
        "сбросить баланс" у самого PaperClient нет. Раньше единственным
        способом сбросить paper-счёт был полный рестарт процесса на Render
        (см. комментарий "paper equity/positions wiped on every restart") -
        это тянуло за собой разрыв WS Lighter и Binance, простой на пересборку
        и т.д. Здесь делаем то же самое, но без рестарта всего бота.
        """
        if CFG.mode != "paper" or self.paper is None:
            return
        raw_ws_url = self.endpoint.ws_url
        sep = "&" if "?" in raw_ws_url else "?"
        paper_ws_url = f"{raw_ws_url}{sep}encoding=json"
        new_paper = lighter.PaperClient(
            api_client=self.api_client,
            initial_collateral_usdc=CFG.account_equity_usd,
            order_api=self.order_api,
            ws_url=paper_ws_url,
        )
        # Трекаем рынки на НОВОМ клиенте ДО подмены self.paper - иначе в окне
        # между подменой и track_market() любой параллельный вызов
        # get_position/create_paper_order упадёт с "market not tracked".
        for market in self.markets.values():
            await new_paper.track_market(market.market_index)
        old_paper = self.paper
        self.paper = new_paper
        try:
            await old_paper.close()
        except Exception as e:
            log.warning("Не удалось корректно закрыть старый PaperClient при сбросе счёта: %s", e)
        log.warning("Paper-счёт пересоздан: баланс сброшен до $%.2f, все виртуальные позиции обнулены.",
                    CFG.account_equity_usd)
