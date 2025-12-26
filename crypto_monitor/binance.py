import asyncio
from collections import defaultdict
from typing import Dict, List

import numpy as np
from crypto_data_downloader.binance import CryptoDataDownloader, FuturesDataDownloader

from .utils import chunk, gather_n_cancel, my_round, retry

MAP1 = {
    "t": "open_time",
    "T": "close_time",
    "s": "symbol",
    "o": "open",
    "c": "close",
    "h": "high",
    "l": "low",
    "v": "volume",
    "n": "n_trades",
    "x": "is_closed",
    "q": "quote_volume",
    "V": "taker_buy_base_volume",
    "Q": "taker_buy_quote_volume",
    "B": "unused",
}
MAP2 = {v: k for k, v in MAP1.items()}


class CryptoMonitor(CryptoDataDownloader):
    ws_base = "wss://stream.binance.com:9443"
    chunk_size = 50

    market = "MARGIN"
    max_num = 1000

    async def get_info_filtered(s):
        def ok(x):
            allowed = s.market in x["permissions"] if s.is_spot else True
            return allowed and x["status"] == "TRADING"

        await s.get_info()
        s.filters = {
            x["symbol"]: {f["filterType"]: f for f in x["filters"]} for x in s.symbols
        }
        s.liq_fee = {x["symbol"]: float(x.get("liquidationFee", 0)) for x in s.symbols}
        s.symbols = list(filter(ok, s.symbols))[: s.max_num]

    def round_qty(s, sym, qty, dir):
        dq = s.filters[sym]["LOT_SIZE"]["stepSize"]
        return my_round(qty, dq, dir)

    def round_price(s, sym, price, dir):
        dp = s.filters[sym]["PRICE_FILTER"]["tickSize"]
        return my_round(price, dp, dir)

    def min_cash(s, sym):
        return float(s.filters[sym]["MIN_NOTIONAL"]["notional"])

    @property
    def syms(s) -> List[str]:
        return [x["symbol"] for x in s.symbols]

    @retry(sleep=60)
    async def watch(s):
        assert s.columns[0] == "open_time"
        s.data: Dict[str, np.ndarray] = {}
        s.update_time: Dict[str, int] = defaultdict(int)
        s.price: Dict[str, float] = defaultdict(float)

        async def watch_some(syms: List[str]):
            streams = [f"{sym.lower()}@kline_{s.interval}" for sym in syms]
            url = f"{s.ws_base}/stream?streams={'/'.join(streams)}"
            async with s.ses.ws_connect(url, heartbeat=20) as ws:
                async for msg in ws:
                    r = msg.json()
                    e_time = r["data"]["E"]
                    k = r["data"]["k"]
                    sym, t = k["s"], k["t"]
                    s.update_time[sym] = e_time
                    s.price[sym] = float(k["c"])
                    arr = s.data[sym]
                    if t != arr[-1, 0]:
                        arr[:] = np.roll(arr, -1, axis=0)
                    for i, col in enumerate(s.columns):
                        arr[-1, i] = float(k[MAP2[col]])
                    # asyncio.create_task(s.on_change(sym, arr, e_time))

        async def watch_info():
            while True:
                await asyncio.sleep(30 * 60)
                await s.get_info_filtered()
                assert set(s.syms) == set(s.data), "info update"

        await s.get_info_filtered()
        queries = [dict(symbol=sym) for sym in s.syms]
        for sym, res in zip(s.syms, await s.get_kline_many(queries)):
            s.data[sym] = res
        tasks = [watch_some(syms) for syms in chunk(s.syms, s.chunk_size)]
        tasks += [watch_info()]
        await gather_n_cancel(*tasks)

    async def on_change(s, sym: str, arr: np.ndarray, e_time: int):
        pass


class FuturesMonitor(FuturesDataDownloader, CryptoMonitor):
    ws_base = "wss://fstream.binance.com"
