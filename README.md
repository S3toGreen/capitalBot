## 群益 Ticker Backend

A lightweight ticker backend for listening to market data, enabling algo-trading, backtesting, and real-time data visualization. Currently implemented in pure Python with an intuitive GUI frontend.

<!-- Badges -->
<!--
![PyPI](https://img.shields.io/pypi/v/your-package)
![Build Status](https://img.shields.io/github/actions/workflow/status/your-repo/ci.yml)
![License](https://img.shields.io/github/license/your-repo)
-->
---

### 🔍 Overview

* **Purpose:** Provide a reliable, low-latency engine for market data ingestion and processing.
* **Use Cases:**

  1. **Live Trading:** Connect to broker feeds for real-time algo execution.
  2. **Backtesting:** Replay tick data for strategy validation.
  3. **Visualization:** Plot order flow, footprints, and custom charts.

### 🚀 Features

* **Modular Architecture:** Separate core engine and GUI for flexibility.
* **Real‑Time Ticking:** Buffer and batch‐process ticks every 0.3s for efficiency.
* **Footprint & OHLC Aggregation:** Generate minute bars with volume at price, delta, and trade-side breakdown.
* **Extensible Interface:** Python API to dispatch custom handlers or integrate with external modules.
<!--
### 🛠️ Installation

```bash
pip install syc-ticker  # Replace with actual package name
```

### ⚙️ Quick Start

```python
from st_ticker import TickerEngine

# Initialize engine with your broker credentials
engine = TickerEngine(api_key="YOUR_KEY", secret="YOUR_SECRET")

# Subscribe to symbols
engine.subscribe(["AAPL", "GOOG", "TXF1"])

# Register a callback for aggregated bars
def on_bar(symbol, bar):
    print(f"{symbol} {bar.time}: {bar.open}/{bar.high}/{bar.low}/{bar.close}, Vol={bar.vol}")

engine.on_minute_bar(on_bar)

# Start the engine and GUI loop
engine.start()
```
-->
### 📐 Architecture

1. **COM Callback Layer:** `OnNotifyTicksLONG` receives raw ticks from broker.
2. **Buffering:** Ticks are appended to in‐memory queues keyed by symbol.
3. **Batch Aggregator:** Every N miliseconds, `_agg_tick` processes new ticks into `Bar` objects.
4. **GUI Binding:** PySide6 visualizes incoming bars and orderflow charts.
5. **Redis:** In-memory database system for better IPC latency 

### 🔧 Roadmap

* [ ] **Built‑in Algo Trading App:** Develop a simple UI for strategy live monitoring. (optionally strategy configuration)
* [ ] **WebGL\WebGPU Migration:** Take advantage of browser GPU framework, cross-platform.(FastAPI, websocket)
* [ ] **Rust Migration:** Port hot‐path functions (`OnNotifyTicksLONG`, `_agg_tick`) to Rust for sub‐millisecond throughput while retaining Python bindings. simd, avx2
* [ ] **Plugin System:** Allow community‐driven extensions for custom indicators and data sources.

### 🤝 Contributing

Contributions, issues, and feature requests are welcome! 
<!--
Please:
1. Fork the repo and create a branch (`feat/YourFeature`).
2. Commit your changes with clear messages.
3. Open a pull request describing the improvement.
-->
<!-- ### 📄 License

Distributed under the MIT License. See [LICENSE](./LICENSE) for details. -->
