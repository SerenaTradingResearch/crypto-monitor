
## Intro

- Monitors the entire crypto market kline (candlestick) data, showing 20 symbols only below for illustration
- [Other tools](https://github.com/orgs/SerenaTradingResearch/repositories)

![](https://raw.githubusercontent.com/SerenaTradingResearch/crypto-monitor/main/test/data/CryptoMonitor.gif)

## Usage

```bash
pip install crypto-monitor
```

- You can either react to individual symbol updates by defining a callback `on_change` as shown below
- Or define a task like `plot_task` below that handles all symbols (`x.data`) repeatedly
    ```py
    x.data: Dict[str, np.ndarray]
    x.update_time: Dict[str, int]
    print(x.data["BTCUSDT"], x.update_time["BTCUSDT"])
    ```

```py
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

```

- Output

```bash
All data columns: ['open_time', 'open', 'high', 'low', 'close', 'volume', 'close_time', 'quote_volume', 'n_trades', 'taker_buy_base_volume', 'taker_buy_quote_volume', 'unused']
weight lim: 5000/6000, USDT symbols: 599, spot: 559, margin: 403
BTC. my time: 2025-08-07 04:10:22, event time: 2025-08-07 04:10:22, price: 114639.92
BTC. my time: 2025-08-07 04:10:24, event time: 2025-08-07 04:10:24, price: 114639.91
BTC. my time: 2025-08-07 04:10:26, event time: 2025-08-07 04:10:26, price: 114639.92
...
```
