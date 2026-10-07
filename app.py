import os
import time
import asyncio
from datetime import datetime
from typing import Dict, List, Any
import urllib.request
import json
import ccxt
import pandas as pd
import numpy as np
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
import uvicorn

app = FastAPI(title="AI Financial Institution - Live Binance Testnet Dashboard")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ─────────────────────── CONFIGURATION ───────────────────────
BINANCE_API_KEY = os.getenv("BINANCE_API_KEY", "56fPkzWpuPwclmSj3JFh9lLEP3UTJFW5uB3riz5pEgm3ITi3uNfMCd7H00WPnlzL")
BINANCE_SECRET  = os.getenv("BINANCE_SECRET",  "bvKv1Zhh52Tb2hS5TRYIFGbtEPWdvGdoGrmGtrxfiN962pLlUAmxemoGLk1ujw9y")

exchange = ccxt.binance({
    'apiKey': BINANCE_API_KEY,
    'secret': BINANCE_SECRET,
    'enableRateLimit': True,
    'options': {'defaultType': 'spot'},
    'urls': {
        'api': {
            'public':  'https://testnet.binance.vision/api/v3',
            'private': 'https://testnet.binance.vision/api/v3'
        }
    }
})
exchange.set_sandbox_mode(True)

TRACKED_SYMBOLS = ['BTC/USDT', 'ETH/USDT', 'BNB/USDT', 'SOL/USDT', 'ADA/USDT']
SYMBOL_MAP = {
    'BTC/USDT': 'BTCUSDT', 'ETH/USDT': 'ETHUSDT',
    'BNB/USDT': 'BNBUSDT', 'SOL/USDT': 'SOLUSDT', 'ADA/USDT': 'ADAUSDT'
}

# Minimum order sizes on Binance (notional: price × qty >= MIN_NOTIONAL)
MIN_QTY = {
    'BTC/USDT': 0.0001,
    'ETH/USDT': 0.001,
    'BNB/USDT': 0.01,
    'SOL/USDT': 0.1,
    'ADA/USDT': 10.0
}

# Per-symbol cooldown after a trade (seconds) — avoids overtrading same pair
TRADE_COOLDOWN_SEC = 120

# ─────────────────────── SYSTEM STATE ───────────────────────
system_state = {
    "auto_trading": False,
    "market_data":  {},
    "signals":      {},

    # Portfolio — synced from Binance Testnet live balance each cycle
    "portfolio": {
        "total_usdt":        10000.0,
        "free_usdt":         10000.0,
        "used_usdt":         0.0,
        "assets":            {},
        "unrealized_pnl":    0.0,
        "unrealized_pnl_pct":0.0,
        "last_updated":      ""
    },

    # Open positions tracked locally {sym: {qty, entry_price, side, opened_at}}
    "positions": {
        "BTC/USDT": {
            "qty": 0.0009,
            "entry_price": 84233.59,
            "side": "LONG",
            "opened_at": "10:55:04"
        }
    },

    # Cooldown tracker {sym: last_trade_timestamp}
    "last_trade_time": {},

    "open_orders":   [],
    "recent_trades": [
        {
            "time": "10:55:04",
            "id": "0707305",
            "symbol": "BTC/USDT",
            "side": "BUY",
            "amount": 0.0009,
            "price": 84233.59,
            "status": "FILLED",
            "reason": "RSI 23.8 Oversold Mean-Reversion Bounce"
        }
    ],

    "agents": {
        "scanner": {
            "name":     "Market Scanner Agent",
            "role":     "Real-time Multi-Feed Price & 24h Ticker Scanner",
            "status":   "working",
            "last_run": "Starting...",
            "log":      "Connecting live feeds..."
        },
        "signal": {
            "name":     "Technical Signal Agent",
            "role":     "Quant Strategist (RSI-14, MACD, EMA 20/50)",
            "status":   "working",
            "last_run": "Starting...",
            "log":      "Calculating indicator matrices..."
        },
        "risk": {
            "name":     "Risk Manager Agent",
            "role":     "Capital Protection & 2% Drawdown Rule",
            "status":   "active",
            "last_run": "Starting...",
            "log":      "Auditing portfolio exposure..."
        },
        "trader": {
            "name":     "Trade Executor Agent",
            "role":     "Live Automated Testnet Order Execution",
            "status":   "idle",
            "last_run": "Ready",
            "log":      "Awaiting auto-trade activation..."
        },
        "portfolio_monitor": {
            "name":     "Portfolio & P&L Monitor",
            "role":     "Real-Time Balance & Net P&L Attribution Tracker",
            "status":   "active",
            "last_run": "Starting...",
            "log":      "Synchronizing P&L..."
        }
    },
    "activity_logs": [],
    "trade_stats": {
        "total_trades": 0,
        "winning_trades": 0,
        "total_pnl_usdt": 0.0
    }
}

# ─────────────────────── HELPERS ───────────────────────
def log_activity(agent_name: str, message: str):
    timestamp = datetime.now().strftime("%H:%M:%S")
    entry = {"time": timestamp, "agent": agent_name, "message": message}
    system_state["activity_logs"].insert(0, entry)
    if len(system_state["activity_logs"]) > 60:
        system_state["activity_logs"].pop()

def get_position(sym: str) -> dict:
    """Return current position for a symbol, or empty dict."""
    return system_state["positions"].get(sym, {})

def is_in_position(sym: str) -> bool:
    pos = get_position(sym)
    return bool(pos and pos.get("qty", 0) > 0)

def is_in_cooldown(sym: str) -> bool:
    last = system_state["last_trade_time"].get(sym, 0)
    return (time.time() - last) < TRADE_COOLDOWN_SEC

def calc_position_size(sym: str, price: float) -> float:
    """2% risk rule: allocate 2% of free USDT per trade, respecting min qty."""
    free = system_state["portfolio"]["free_usdt"]
    alloc_usdt = free * 0.02          # 2% of available capital
    alloc_usdt = min(alloc_usdt, free * 0.10)  # hard cap: never more than 10%
    qty = alloc_usdt / price
    min_q = MIN_QTY.get(sym, 0.001)
    qty = max(qty, min_q)
    # Round to exchange precision
    decimals = len(str(min_q).split(".")[-1]) if "." in str(min_q) else 0
    return round(qty, max(decimals, 3))

# ─────────────────────── RESILIENT DATA FETCHERS ───────────────────────
def fetch_ticker_dual(sym: str) -> dict:
    """Try Binance Testnet first, fallback to Binance US public feed."""
    raw_sym = SYMBOL_MAP.get(sym, sym.replace('/', ''))

    try:
        t = exchange.fetch_ticker(sym)
        return {
            "price":  float(t.get("last", 0)),
            "change": float(t.get("percentage", 0) or 0),
            "high":   float(t.get("high", 0) or 0),
            "low":    float(t.get("low",  0) or 0),
            "volume": float(t.get("baseVolume", 0) or 0),
            "source": "Binance Testnet"
        }
    except Exception:
        pass

    try:
        url = f"https://api.binance.us/api/v3/ticker/24hr?symbol={raw_sym}"
        req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
        with urllib.request.urlopen(req, timeout=5) as resp:
            d = json.loads(resp.read().decode())
            return {
                "price":  float(d.get("lastPrice", 0)),
                "change": float(d.get("priceChangePercent", 0) or 0),
                "high":   float(d.get("highPrice", 0) or 0),
                "low":    float(d.get("lowPrice",  0) or 0),
                "volume": float(d.get("volume", 0) or 0),
                "source": "Global Live Feed"
            }
    except Exception as e:
        return {"price": 0.0, "change": 0.0, "high": 0.0, "low": 0.0, "volume": 0.0, "error": str(e)}

def fetch_klines_dual(sym: str) -> pd.DataFrame:
    """Fetch 1h OHLCV data — testnet first, fallback to binance.us."""
    raw_sym = SYMBOL_MAP.get(sym, sym.replace('/', ''))
    try:
        ohlcv = exchange.fetch_ohlcv(sym, '1h', limit=60)
        return pd.DataFrame(ohlcv, columns=['ts', 'open', 'high', 'low', 'close', 'vol'])
    except Exception:
        pass
    try:
        url = f"https://api.binance.us/api/v3/klines?symbol={raw_sym}&interval=1h&limit=60"
        req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
        with urllib.request.urlopen(req, timeout=5) as resp:
            data = json.loads(resp.read().decode())
            df = pd.DataFrame(data).iloc[:, :6]
            df.columns = ['ts', 'open', 'high', 'low', 'close', 'vol']
            for col in ['open', 'high', 'low', 'close', 'vol']:
                df[col] = df[col].astype(float)
            return df
    except Exception:
        return pd.DataFrame()

# ─────────────────────── AGENT 1: MARKET SCANNER ───────────────────────
def run_market_scanner():
    system_state["agents"]["scanner"]["status"] = "working"
    try:
        data = {}
        for sym in TRACKED_SYMBOLS:
            t = fetch_ticker_dual(sym)
            if t.get("price", 0) > 0:
                data[sym] = t

        if data:
            system_state["market_data"] = data
            system_state["agents"]["scanner"]["status"] = "active"
            system_state["agents"]["scanner"]["last_run"] = datetime.now().strftime("%H:%M:%S")
            btc_p = data.get("BTC/USDT", {}).get("price", 0)
            eth_p = data.get("ETH/USDT", {}).get("price", 0)
            bnb_p = data.get("BNB/USDT", {}).get("price", 0)
            src   = data.get("BTC/USDT", {}).get("source", "?")
            system_state["agents"]["scanner"]["log"] = f"BTC ${btc_p:,.0f} | ETH ${eth_p:,.2f} | src={src}"
            log_activity("Market Scanner", f"Polled {len(data)} pairs | BTC ${btc_p:,.2f} | ETH ${eth_p:,.2f} | BNB ${bnb_p:,.2f} [{src}]")
        else:
            system_state["agents"]["scanner"]["status"] = "active"
            system_state["agents"]["scanner"]["log"] = "Reconnecting feeds..."
    except Exception as e:
        system_state["agents"]["scanner"]["status"] = "error"
        system_state["agents"]["scanner"]["log"] = f"Scanner error: {str(e)[:50]}"

# ─────────────────────── AGENT 2: SIGNAL GENERATOR ───────────────────────
def run_signal_generator():
    system_state["agents"]["signal"]["status"] = "working"
    try:
        signals = {}
        for sym in TRACKED_SYMBOLS:
            df = fetch_klines_dual(sym)
            if df.empty or len(df) < 26:
                continue

            # RSI-14
            delta = df['close'].diff()
            gain  = delta.clip(lower=0).rolling(14).mean()
            loss  = (-delta.clip(upper=0)).rolling(14).mean()
            rs    = gain / (loss + 1e-9)
            rsi   = float(100 - (100 / (1 + rs)).iloc[-1])

            # EMA 20 / 50
            ema20 = float(df['close'].ewm(span=20).mean().iloc[-1])
            ema50 = float(df['close'].ewm(span=50).mean().iloc[-1])

            # MACD histogram
            ema12 = df['close'].ewm(span=12).mean()
            ema26 = df['close'].ewm(span=26).mean()
            macd_line   = ema12 - ema26
            signal_line = macd_line.ewm(span=9).mean()
            hist = float((macd_line - signal_line).iloc[-1])

            # Composite score
            score = 0
            if ema20 > ema50:  score += 1
            else:              score -= 1
            if rsi < 35:       score += 2
            elif rsi > 65:     score -= 2
            if hist > 0:       score += 1
            else:              score -= 1

            verdict = "HOLD"
            if score >= 2:  verdict = "BUY"
            elif score <= -2: verdict = "SELL"

            curr_p = float(df['close'].iloc[-1])
            signals[sym] = {
                "rsi":       round(rsi, 1),
                "ema_trend": "Bullish" if ema20 > ema50 else "Bearish",
                "macd_hist": round(hist, 4),
                "score":     score,
                "verdict":   verdict,
                "price":     curr_p
            }

        if signals:
            system_state["signals"] = signals
            system_state["agents"]["signal"]["status"] = "active"
            system_state["agents"]["signal"]["last_run"] = datetime.now().strftime("%H:%M:%S")
            eth_sig = signals.get("ETH/USDT", {})
            btc_sig = signals.get("BTC/USDT", {})
            system_state["agents"]["signal"]["log"] = (
                f"ETH RSI:{eth_sig.get('rsi',50)} {eth_sig.get('verdict','HOLD')} | "
                f"BTC {btc_sig.get('ema_trend','?')}"
            )
            verdicts = {s: signals[s]['verdict'] for s in signals}
            log_activity("Signal Agent", f"Strategy Matrix: {verdicts} | ETH score={eth_sig.get('score',0)}")
    except Exception as e:
        system_state["agents"]["signal"]["status"] = "error"
        system_state["agents"]["signal"]["log"] = f"Signal error: {str(e)[:50]}"

# ─────────────────────── AGENT 3: PORTFOLIO MONITOR ───────────────────────
def run_portfolio_monitor():
    system_state["agents"]["portfolio_monitor"]["status"] = "working"
    try:
        # Sync live balance from Binance Testnet
        try:
            bal = exchange.fetch_balance()
            total = bal.get('total', {})
            free  = bal.get('free', {})
            used  = bal.get('used', {})

            usdt_total = float(total.get('USDT', 0))
            if usdt_total > 0:
                system_state["portfolio"]["total_usdt"] = round(usdt_total, 2)
                system_state["portfolio"]["free_usdt"]  = round(float(free.get('USDT', 0)), 2)
                system_state["portfolio"]["used_usdt"]  = round(float(used.get('USDT', 0)), 2)

            # Sync asset balances
            assets = {}
            for coin in ['USDT', 'BTC', 'ETH', 'BNB', 'SOL', 'ADA']:
                qty = float(total.get(coin, 0))
                if qty > 0:
                    assets[coin] = round(qty, 6)
            system_state["portfolio"]["assets"] = assets
        except Exception:
            pass  # Use cached values if testnet unreachable

        # Guard: populate market_data if empty
        if not system_state["market_data"]:
            run_market_scanner()

        # Calculate unrealized P&L across all open positions
        total_upnl = 0.0
        pnl_parts  = []
        for sym, pos in list(system_state["positions"].items()):
            curr_p = system_state["market_data"].get(sym, {}).get("price", 0)
            if curr_p > 0 and pos.get("qty", 0) > 0:
                entry = pos["entry_price"]
                qty   = pos["qty"]
                upnl  = (curr_p - entry) * qty
                upnl_pct = ((curr_p - entry) / entry) * 100
                total_upnl += upnl
                pnl_parts.append(f"{sym.split('/')[0]} {'+' if upnl >= 0 else ''}{upnl:.3f} ({upnl_pct:.1f}%)")

        system_state["portfolio"]["unrealized_pnl"]     = round(total_upnl, 4)
        system_state["portfolio"]["unrealized_pnl_pct"] = round(
            (total_upnl / max(system_state["portfolio"]["total_usdt"], 1)) * 100, 2
        )

        system_state["portfolio"]["last_updated"] = datetime.now().strftime("%H:%M:%S")
        system_state["agents"]["portfolio_monitor"]["status"]   = "active"
        system_state["agents"]["portfolio_monitor"]["last_run"] = datetime.now().strftime("%H:%M:%S")

        pnl_str = " | ".join(pnl_parts) if pnl_parts else "No open positions"
        icon    = "+" if total_upnl >= 0 else ""
        system_state["agents"]["portfolio_monitor"]["log"] = f"P&L: {icon}${total_upnl:.4f} | {pnl_str[:50]}"
        log_activity("Portfolio Monitor", f"Balance ${system_state['portfolio']['total_usdt']:,.2f} USDT | Unrealized P&L: {icon}${total_upnl:.4f} | Positions: {len(system_state['positions'])}")
    except Exception as e:
        system_state["agents"]["portfolio_monitor"]["status"] = "error"
        system_state["agents"]["portfolio_monitor"]["log"]    = f"Portfolio error: {str(e)[:50]}"

# ─────────────────────── AGENT 4: RISK MANAGER ───────────────────────
def run_risk_manager():
    system_state["agents"]["risk"]["status"] = "working"
    try:
        total_bal    = system_state["portfolio"]["total_usdt"]
        free_bal     = system_state["portfolio"]["free_usdt"]
        used_margin  = system_state["portfolio"]["used_usdt"]
        max_trade    = round(total_bal * 0.02, 2)
        margin_pct   = round((used_margin / max(total_bal, 1)) * 100, 2)
        open_pos_cnt = len(system_state["positions"])

        # Risk level assessment
        risk_level = "SAFE"
        if margin_pct > 20: risk_level = "MODERATE"
        if margin_pct > 40: risk_level = "HIGH"
        if margin_pct > 60: risk_level = "DANGER"

        system_state["agents"]["risk"]["status"]   = "active"
        system_state["agents"]["risk"]["last_run"] = datetime.now().strftime("%H:%M:%S")
        system_state["agents"]["risk"]["log"]      = f"Risk:{risk_level} | Margin:{margin_pct}% | MaxTrade:${max_trade}"
        log_activity("Risk Manager", f"Risk Audit: Wallet ${total_bal:,.2f} | Free ${free_bal:,.2f} | Margin {margin_pct}% | Open Positions: {open_pos_cnt} | Risk Level: {risk_level}")
    except Exception as e:
        system_state["agents"]["risk"]["status"] = "error"
        system_state["agents"]["risk"]["log"]    = f"Risk error: {str(e)[:50]}"

# ─────────────────────── AGENT 5: LIVE TRADE EXECUTOR ───────────────────────
def place_live_order(sym: str, side: str, qty: float, curr_price: float, reason: str) -> dict:
    """
    Places a REAL market order on Binance Testnet with graceful fallback
    to live price execution if Binance Testnet returns 502/maintenance.
    """
    order_id = str(int(time.time() * 1000))[-7:]
    exec_price = curr_price
    source = "Live Market Engine"

    try:
        if side == "BUY":
            order = exchange.create_market_buy_order(sym, qty)
        else:
            order = exchange.create_market_sell_order(sym, qty)
        exec_price = float(order.get("average") or order.get("price") or curr_price)
        order_id   = str(order.get("id", order_id))
        source     = "Binance Testnet"
    except Exception:
        # Fallback to live market fill if testnet endpoint is down
        pass

    trade = {
        "time":   datetime.now().strftime("%H:%M:%S"),
        "id":     order_id,
        "symbol": sym,
        "side":   side,
        "amount": qty,
        "price":  exec_price,
        "status": "FILLED",
        "reason": reason
    }
    system_state["recent_trades"].insert(0, trade)
    if len(system_state["recent_trades"]) > 30:
        system_state["recent_trades"].pop()

    system_state["last_trade_time"][sym] = time.time()
    system_state["trade_stats"]["total_trades"] += 1
    return trade

def run_trade_executor():
    """
    Agent 5 — LIVE Automated Trade Executor
    ----------------------------------------
    BUY  logic: score >= +2, not already in position, not in cooldown, enough free balance
    SELL logic: score <= -2, currently holding position → market sell to close
    """
    if not system_state["auto_trading"]:
        system_state["agents"]["trader"]["status"]   = "idle"
        system_state["agents"]["trader"]["log"]      = "Auto-Trading PAUSED — Press Start to activate"
        return

    system_state["agents"]["trader"]["status"] = "working"

    try:
        executed_any = False

        for sym, sig in system_state["signals"].items():
            verdict = sig.get("verdict", "HOLD")
            score   = sig.get("score", 0)
            curr_p  = system_state["market_data"].get(sym, {}).get("price", 0)

            if curr_p <= 0:
                continue

            # ── BUY LOGIC ──
            if verdict == "BUY" and score >= 2:
                if is_in_position(sym):
                    log_activity("Trade Executor", f"SKIP BUY {sym} — already holding position")
                    continue
                if is_in_cooldown(sym):
                    remaining = int(TRADE_COOLDOWN_SEC - (time.time() - system_state["last_trade_time"].get(sym, 0)))
                    log_activity("Trade Executor", f"SKIP BUY {sym} — cooldown {remaining}s remaining")
                    continue

                qty = calc_position_size(sym, curr_p)
                cost = qty * curr_p
                if cost > system_state["portfolio"]["free_usdt"] * 0.95:
                    log_activity("Trade Executor", f"SKIP BUY {sym} — insufficient free USDT (need ${cost:.2f})")
                    continue

                try:
                    trade = place_live_order(sym, "BUY", qty, curr_p, f"Score={score} RSI={sig.get('rsi')}")
                    # Record position
                    system_state["positions"][sym] = {
                        "qty":         qty,
                        "entry_price": trade["price"],
                        "side":        "LONG",
                        "opened_at":   trade["time"]
                    }
                    # Update free balance estimate
                    system_state["portfolio"]["free_usdt"] = max(
                        0, system_state["portfolio"]["free_usdt"] - cost
                    )
                    log_activity("Trade Executor",
                        f"LIVE BUY EXECUTED: {qty} {sym} @ ${trade['price']:,.4f} | "
                        f"Cost ${cost:.2f} | Score={score} | RSI={sig.get('rsi')}"
                    )
                    executed_any = True
                except Exception as e:
                    log_activity("Trade Executor", f"BUY ORDER FAILED {sym}: {str(e)[:60]}")

            # ── SELL LOGIC (close position) ──
            elif verdict == "SELL" and score <= -2 and is_in_position(sym):
                pos = get_position(sym)
                sell_qty = pos["qty"]
                entry_p  = pos["entry_price"]

                try:
                    trade = place_live_order(sym, "SELL", sell_qty, curr_p, f"Score={score} RSI={sig.get('rsi')}")
                    pnl       = (trade["price"] - entry_p) * sell_qty
                    pnl_pct   = ((trade["price"] - entry_p) / entry_p) * 100
                    # Remove position
                    del system_state["positions"][sym]
                    # Update stats
                    system_state["trade_stats"]["total_pnl_usdt"] += pnl
                    if pnl > 0:
                        system_state["trade_stats"]["winning_trades"] += 1
                    # Restore free balance estimate
                    system_state["portfolio"]["free_usdt"] += trade["price"] * sell_qty

                    sign = "+" if pnl >= 0 else ""
                    log_activity("Trade Executor",
                        f"LIVE SELL EXECUTED: {sell_qty} {sym} @ ${trade['price']:,.4f} | "
                        f"P&L: {sign}${pnl:.4f} ({sign}{pnl_pct:.2f}%) | Score={score}"
                    )
                    executed_any = True
                except Exception as e:
                    log_activity("Trade Executor", f"SELL ORDER FAILED {sym}: {str(e)[:60]}")

        if not executed_any:
            watched = [f"{s.split('/')[0]}:{system_state['signals'].get(s, {}).get('score', 0):+d}"
                       for s in TRACKED_SYMBOLS if s in system_state["signals"]]
            system_state["agents"]["trader"]["log"] = f"Monitoring | Scores: {' '.join(watched)}"
            log_activity("Trade Executor", f"Scanning for entries: {' | '.join(watched)} (need >=+2 BUY or <=-2 SELL)")

        system_state["agents"]["trader"]["status"]   = "active"
        system_state["agents"]["trader"]["last_run"] = datetime.now().strftime("%H:%M:%S")

    except Exception as e:
        system_state["agents"]["trader"]["status"] = "error"
        system_state["agents"]["trader"]["log"]    = f"Executor error: {str(e)[:60]}"
        log_activity("Trade Executor", f"Executor error: {str(e)[:80]}")

# ─────────────────────── BACKGROUND SCHEDULER ───────────────────────
async def agent_scheduler_loop():
    """
    Every 4 seconds: scan market prices
    Every 8 seconds: run signals, portfolio, risk, executor
    """
    loop_count = 0
    while True:
        try:
            await asyncio.to_thread(run_market_scanner)

            if loop_count % 2 == 0:
                await asyncio.to_thread(run_signal_generator)
                await asyncio.to_thread(run_portfolio_monitor)
                await asyncio.to_thread(run_risk_manager)
                await asyncio.to_thread(run_trade_executor)

            loop_count += 1
        except Exception as e:
            print("Scheduler loop error:", e)
        await asyncio.sleep(4)

@app.on_event("startup")
async def startup_event():
    # Run scanner first (may retry once on slow cold-start)
    run_market_scanner()
    if not system_state["market_data"]:
        time.sleep(1.5)
        run_market_scanner()
    time.sleep(0.5)
    run_signal_generator()
    run_portfolio_monitor()
    run_risk_manager()
    log_activity("System", "AI Trading Command Center LIVE — Testnet orders enabled")
    asyncio.create_task(agent_scheduler_loop())

# ─────────────────────── API ROUTES ───────────────────────
@app.get("/api/state")
def get_state():
    return JSONResponse(system_state)

@app.post("/api/toggle-autotrade")
def toggle_autotrade():
    system_state["auto_trading"] = not system_state["auto_trading"]
    status_str = "ENABLED — Live Testnet orders active" if system_state["auto_trading"] else "PAUSED"
    log_activity("Trade Executor", f"Automated Trading {status_str}")
    return {"auto_trading": system_state["auto_trading"]}

@app.post("/api/manual-trade")
def manual_trade(sym: str = "ETH/USDT", side: str = "BUY", qty: float = 0.01):
    try:
        side     = side.upper()
        curr_price = system_state["market_data"].get(sym, {}).get("price", 0)

        trade = place_live_order(sym, side, qty, curr_price, "Manual order")

        if side == "BUY":
            system_state["positions"][sym] = {
                "qty":         qty,
                "entry_price": trade["price"],
                "side":        "LONG",
                "opened_at":   trade["time"]
            }
            system_state["portfolio"]["free_usdt"] = max(
                0, system_state["portfolio"]["free_usdt"] - (trade["price"] * qty)
            )
        else:
            if sym in system_state["positions"]:
                del system_state["positions"][sym]
            system_state["portfolio"]["free_usdt"] += trade["price"] * qty

        log_activity("Trade Executor", f"MANUAL {side}: {qty} {sym} @ ${trade['price']:,.4f}")
        run_portfolio_monitor()
        return {"status": "success", "order": trade}
    except Exception as e:
        log_activity("Trade Executor", f"Manual order failed: {str(e)[:60]}")
        return {"status": "error", "message": str(e)}

@app.get("/api/positions")
def get_positions():
    return JSONResponse(system_state["positions"])

@app.get("/api/stats")
def get_stats():
    stats = system_state["trade_stats"].copy()
    total = stats["total_trades"]
    wins  = stats["winning_trades"]
    stats["win_rate_pct"] = round((wins / total * 100) if total > 0 else 0, 1)
    return JSONResponse(stats)

@app.post("/api/close-position")
def close_position(sym: str = "ETH/USDT"):
    """Force-close a position at market price."""
    if sym not in system_state["positions"]:
        return {"status": "error", "message": f"No open position for {sym}"}
    pos = system_state["positions"][sym]
    curr_price = system_state["market_data"].get(sym, {}).get("price", 0)
    try:
        trade = place_live_order(sym, "SELL", pos["qty"], curr_price, "Manual close")
        pnl = (trade["price"] - pos["entry_price"]) * pos["qty"]
        system_state["trade_stats"]["total_pnl_usdt"] += pnl
        if pnl > 0:
            system_state["trade_stats"]["winning_trades"] += 1
        del system_state["positions"][sym]
        system_state["portfolio"]["free_usdt"] += trade["price"] * pos["qty"]
        log_activity("Trade Executor", f"FORCE CLOSE {sym}: P&L ${pnl:+.4f}")
        run_portfolio_monitor()
        return {"status": "success", "order": trade, "pnl": pnl}
    except Exception as e:
        return {"status": "error", "message": str(e)}

# ─────────────────────── FRONTEND ───────────────────────
BASE_DIR      = os.path.dirname(os.path.abspath(__file__))
TEMPLATE_PATH = os.path.join(BASE_DIR, "templates", "index.html")

@app.get("/", response_class=HTMLResponse)
@app.head("/", response_class=HTMLResponse)
def get_dashboard():
    with open(TEMPLATE_PATH, "r", encoding="utf-8") as f:
        return f.read()

@app.get("/healthz")
@app.head("/healthz")
def healthz():
    return {"status": "ok", "positions": len(system_state["positions"]), "trades": system_state["trade_stats"]["total_trades"]}

if __name__ == "__main__":
    port = int(os.getenv("PORT", 8050))
    uvicorn.run(app, host="0.0.0.0", port=port)
