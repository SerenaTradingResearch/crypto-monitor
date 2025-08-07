import asyncio
from typing import List

import numpy as np
from crypto_data_downloader.binance import ALL_COLUMNS
from crypto_data_downloader.utils import format_date, plot_crypto_data, timestamp
from PIL import Image

from crypto_monitor.binance import CryptoMonitor
from crypto_monitor.utils import safe_exit, save_gif


async def main():
    async def on_change(sym: str, arr: np.ndarray, e_time: int):
        if sym == "BTCUSDT":
            f = format_date
            print(
                f"BTC. my time: {f(timestamp())}, event time: {f(e_time)}, price: {arr[-1, 1]}"
            )

    x = CryptoMonitor()
    x.quote = "USDT"  # Quote asset
    x.interval = "5m"  # Kline time interval
    x.columns = ["open_time", "close"]  # Data columns to include
    print(f"All data columns: {ALL_COLUMNS}")
    x.kline_lim = 10  # Only show 10 time steps for clearer visualization
    x.market = ["SPOT", "MARGIN"][1]
    x.max_num = 20  # Only show 20 symbols for visualization
    x.on_change = on_change

    async def plot_task():
        frames: List[Image.Image] = []
        while True:
            await asyncio.sleep(2)
            safe_exit.check()
            if len(x.data):
                id = "data/CryptoMonitor"
                plot_crypto_data(x.data, id)
                frames.append(Image.open(f"{id}.png").copy())
                assert len(frames) < 150, save_gif(frames, id, fps=5)

    await asyncio.gather(x.watch(), plot_task())


asyncio.run(main())
