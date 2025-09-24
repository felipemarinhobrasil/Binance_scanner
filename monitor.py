#!/usr/bin/env python3
"""
Binance Spot Momentum Scanner (Docker-ready)
"""

import asyncio
import json
import math
import os
import signal
import time
import threading
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List, Optional, Tuple, Any

import requests
import websockets
from pytz import timezone

# === trader.py (compra + OCO + monitor de PnL) ===
try:
    from trader import buy_market_and_place_oco, monitor_open_trades  # type: ignore
except Exception as _imp_exc:
    print("[WARN] Falha ao importar trader.py:", _imp_exc)
    buy_market_and_place_oco = None
    monitor_open_trades = None  # type: ignore

# Dicionário compartilhado com trader.monitor_open_trades
open_trades: Dict[str, Any] = {}

# =========================
# Helpers de configuração
# =========================
def _get_env(k, d=None):
    v = os.getenv(k)
    return v if v is not None else ("" if d is None else str(d))

def _get_float(k, d):
    try:
        return float(os.getenv(k, d))
    except Exception:
        return float(d)

def _get_int(k, d):
    try:
        return int(float(os.getenv(k, d)))
    except Exception:
        return int(d)

def _get_bool(k, d):
    v = os.getenv(k)
    if v is None:
        return d
    return v.strip().lower() in ("1", "true", "yes", "y", "on")

def _auto_endpoints():
    """Escolhe BASE/WS default de acordo com USE_TESTNET, a menos que BASE_URL/WS_URL venham no .env."""
    use_testnet = _get_bool("USE_TESTNET", False)
    base = os.getenv("BASE_URL")
    ws = os.getenv("WS_URL")
    if not base or not ws:
        if use_testnet:
            base = base or "https://testnet.binance.vision"
            ws   = ws   or "wss://testnet.binance.vision/ws"
        else:
            base = base or "https://api.binance.com"
            ws   = ws   or "wss://stream.binance.com:9443/stream"
    return base, ws, use_testnet

_BASE_URL, _WS_URL, _IS_TESTNET = _auto_endpoints()

CONFIG = {
    "THRESHOLD_PCT": _get_float("THRESHOLD_PCT", 0.025),         # 2.5% no candle por padrão
    "VOLUME_MULTIPLIER": _get_float("VOLUME_MULTIPLIER", 1.15),  # >= 1.15x média
    "VOL_LOOKBACK": _get_int("VOL_LOOKBACK", 60),                # média de volume
    "INTERVAL": _get_env("INTERVAL", "1m"),
    "DEBUG_PRE_SIGNAL": _get_bool("DEBUG_PRE_SIGNAL", True),
    "PRE_SIGNAL_COOLDOWN": _get_int("PRE_SIGNAL_COOLDOWN", 15),
    "REPORT_TOP_MOVERS_EVERY": _get_int("REPORT_TOP_MOVERS_EVERY", 10),
    "TOP_MOVERS_N": _get_int("TOP_MOVERS_N", 15),
    "TELEGRAM_TOKEN": _get_env("TELEGRAM_BOT_TOKEN", ""),
    "TELEGRAM_CHAT_ID": _get_env("TELEGRAM_CHAT_ID", ""),
    "SEND_HEATMAP_TO_TELEGRAM": _get_bool("SEND_HEATMAP_TO_TELEGRAM", True),
    "HEATMAP_TELEGRAM_COOLDOWN": _get_int("HEATMAP_TELEGRAM_COOLDOWN", 120),
    "HEATMAP_MIN_PCT_FOR_TELEGRAM": _get_float("HEATMAP_MIN_PCT_FOR_TELEGRAM", 0.02),
    "HEATMAP_ONLY_WHEN_CHANGED": _get_bool("HEATMAP_ONLY_WHEN_CHANGED", True),
    "HEATMAP_TOP_SIGNATURE_SIZE": _get_int("HEATMAP_TOP_SIGNATURE_SIZE", 5),
    "TZ": _get_env("TZ", "America/Bahia"),
    "COOLDOWN_PER_SYMBOL": _get_int("COOLDOWN_PER_SYMBOL", 180),
    "BASE": _BASE_URL,
    "WS": _WS_URL,
    "QUOTE_FILTER": _get_env("QUOTE_FILTER", "USDT"),
    "INCLUDE_LEVERAGED": _get_bool("INCLUDE_LEVERAGED", False),
    "BATCH_SIZE": _get_int("BATCH_SIZE", 800),
    "USE_TESTNET": _IS_TESTNET,
    "TARGET_EXIT_MINUTES": _get_int("TARGET_EXIT_MINUTES", 15),
    "MIN_TARGET_RANGE_PCT": _get_float("MIN_TARGET_RANGE_PCT", 1.05),
    "MIN_TARGET_CUM_MOVE_PCT": _get_float("MIN_TARGET_CUM_MOVE_PCT", 1.4),
    "EXIT_CHECK_COOLDOWN": _get_int("EXIT_CHECK_COOLDOWN", 45),
}

def _print_effective_config():
    print("\n[CFG] Config efetiva:")
    keys = [
        "USE_TESTNET","BASE","WS","QUOTE_FILTER","INTERVAL",
        "THRESHOLD_PCT","VOLUME_MULTIPLIER","VOL_LOOKBACK",
        "COOLDOWN_PER_SYMBOL","BATCH_SIZE","TARGET_EXIT_MINUTES",
        "MIN_TARGET_RANGE_PCT","MIN_TARGET_CUM_MOVE_PCT",
        "SEND_HEATMAP_TO_TELEGRAM","HEATMAP_MIN_PCT_FOR_TELEGRAM","HEATMAP_TELEGRAM_COOLDOWN"
    ]
    for k in keys:
        print(f"  - {k} = {CONFIG[k]}")
    print()

@dataclass
class SymbolState:
    symbol: str
    open_price: float = 0.0
    high: float = 0.0
    low: float = 0.0
    close: float = 0.0
    volume: float = 0.0
    start_time: int = 0
    is_final: bool = False
    vol_avg: float = 0.0
    last_alert_ts: float = 0.0
    last_vol_fetch_ts: float = 0.0
    last_pre_log_ts: float = 0.0
    last_exit_check_ts: float = 0.0
    last_exit_check_ok: bool = False
    last_exit_metrics: Optional[Dict[str, float]] = None

REPORTER_LAST_SIG: Optional[Tuple] = None
REPORTER_LAST_TELEGRAM_TS: float = 0.0

def now_tz():
    return datetime.now(timezone(CONFIG["TZ"]))

def fmt_pct(x: float) -> str:
    return f"{x*100:.2f}%"

# =========================
# Telegram
# =========================
try:
    from telegram import Bot
except Exception:
    Bot = None

TELEGRAM_BOT = None
if CONFIG["TELEGRAM_TOKEN"] and Bot is not None:
    try:
        TELEGRAM_BOT = Bot(token=CONFIG["TELEGRAM_TOKEN"])
    except Exception as exc:
        print("[WARN] Telegram bot init failed:", exc)

def tg_send(msg: str):
    if TELEGRAM_BOT and CONFIG["TELEGRAM_CHAT_ID"]:
        try:
            TELEGRAM_BOT.send_message(chat_id=CONFIG["TELEGRAM_CHAT_ID"], text=msg)
        except Exception as exc:
            print("[WARN] Telegram send error:", exc)

# =========================
# REST helpers
# =========================
def get_exchange_info():
    r = requests.get(f"{CONFIG['BASE']}/api/v3/exchangeInfo", timeout=20)
    r.raise_for_status()
    return r.json()

def get_klines(symbol, interval, limit):
    r = requests.get(
        f"{CONFIG['BASE']}/api/v3/klines",
        params={"symbol": symbol, "interval": interval, "limit": limit},
        timeout=20,
    )
    r.raise_for_status()
    return r.json()

# =========================
# Universe de symbols
# =========================
def select_all_spot_symbols() -> List[str]:
    info = get_exchange_info()
    symbols: List[str] = []

    allowed = CONFIG.get("QUOTE_FILTER")
    if isinstance(allowed, str) and allowed:
        allowed_list = [allowed]
    elif isinstance(allowed, (list, tuple, set)):
        allowed_list = list(allowed)
    else:
        allowed_list = None

    include_lev = CONFIG.get("INCLUDE_LEVERAGED", False)

    for s in info.get("symbols", []):
        try:
            if s.get("status") == "TRADING" and s.get("isSpotTradingAllowed", False) and s.get("symbol"):
                qa = s.get("quoteAsset")
                if allowed_list and qa not in allowed_list:
                    continue
                sym = s["symbol"]
                if not include_lev and sym.endswith(("UPUSDT", "DOWNUSDT", "BULLUSDT", "BEARUSDT")):
                    continue
                symbols.append(sym)
        except Exception:
            continue
    return sorted(set(symbols))

def compute_vol_avg(symbol: str, lookback: int) -> float:
    try:
        kl = get_klines(symbol, CONFIG["INTERVAL"], limit=lookback)
        vols = [float(x[5]) for x in kl]  # baseVolume; se quiser quote: x[7]
        return (sum(vols) / max(1, len(vols))) if vols else 0.0
    except Exception as exc:
        print(f"[VOL-AVG] erro {symbol}: {exc}")
        return 0.0

def build_stream_url(symbols: List[str]) -> str:
    streams = "/".join(f"{s.lower()}@kline_{CONFIG['INTERVAL']}" for s in symbols)
    return f"{CONFIG['WS']}?streams={streams}"

def chunked(lst: List[str], n: int) -> List[List[str]]:
    return [lst[i:i + n] for i in range(0, len(lst), n)]

# =========================
# Checagem adicional de volatilidade (janela alvo)
# =========================
def _interval_to_minutes(interval: str) -> float:
    try:
        unit = interval[-1]
        value = float(interval[:-1])
    except Exception:
        return 1.0
    if unit == "m":
        return max(1.0, value)
    if unit == "h":
        return max(1.0, value * 60.0)
    if unit == "d":
        return max(1.0, value * 1440.0)
    if unit == "w":
        return max(1.0, value * 10080.0)
    if unit == "M":  # mês aproximado
        return max(1.0, value * 43200.0)
    return 1.0

def check_exit_window_volatility(symbol: str) -> Tuple[bool, Dict[str, float]]:
    interval_minutes = _interval_to_minutes(CONFIG["INTERVAL"])
    target_window_min = max(2, CONFIG["TARGET_EXIT_MINUTES"])
    bars_needed = int(math.ceil(target_window_min / interval_minutes))
    bars_needed = max(2, bars_needed)

    limit = max(CONFIG["VOL_LOOKBACK"], bars_needed)
    try:
        kl = get_klines(symbol, CONFIG["INTERVAL"], limit=limit)
    except Exception as exc:
        print(f"[VOL-EXIT] erro {symbol}: {exc}")
        return False, {
            "range_pct": 0.0,
            "cum_move_pct": 0.0,
            "avg_move_pct": 0.0,
            "bars": 0,
            "interval_minutes": interval_minutes,
            "window_minutes": 0.0,
            "reason": "request_error",
        }

    if len(kl) < 2:
        return False, {
            "range_pct": 0.0,
            "cum_move_pct": 0.0,
            "avg_move_pct": 0.0,
            "bars": len(kl),
            "interval_minutes": interval_minutes,
            "window_minutes": len(kl) * interval_minutes,
            "reason": "insufficient_bars",
        }

    subset = kl[-bars_needed:]
    highs = [float(x[2]) for x in subset]
    lows = [float(x[3]) for x in subset]
    closes = [float(x[4]) for x in subset]

    max_h = max(highs)
    min_l = min(lows)
    mid = (max_h + min_l) / 2.0 if (max_h + min_l) > 0 else (closes[-1] if closes else 0.0)
    range_pct = 0.0 if mid == 0 else (max_h - min_l) / mid * 100.0

    cum_move_pct = 0.0
    for i in range(1, len(closes)):
        prev = closes[i - 1]
        cur = closes[i]
        if prev > 0:
            cum_move_pct += abs((cur - prev) / prev * 100.0)

    avg_move_pct = cum_move_pct / max(1, len(closes) - 1)
    window_minutes = len(closes) * interval_minutes

    metrics = {
        "range_pct": range_pct,
        "cum_move_pct": cum_move_pct,
        "avg_move_pct": avg_move_pct,
        "bars": len(closes),
        "interval_minutes": interval_minutes,
        "window_minutes": window_minutes,
    }

    ok = (
        range_pct >= CONFIG["MIN_TARGET_RANGE_PCT"]
        and cum_move_pct >= CONFIG["MIN_TARGET_CUM_MOVE_PCT"]
    )

    if not ok:
        metrics["reason"] = "threshold"

    return ok, metrics

# =========================
# Processamento dos klines
# =========================
def process_kline_msg(state: Dict[str, SymbolState], msg: dict) -> Optional[str]:
    data = msg.get("data", {})
    if data.get("e") != "kline":
        return None
    k = data.get("k", {})
    symbol = k.get("s")
    if not symbol:
        return None

    st = state.get(symbol) or SymbolState(symbol=symbol)
    state[symbol] = st

    st.open_price = float(k.get("o", 0.0))
    st.high = float(k.get("h", 0.0))
    st.low = float(k.get("l", 0.0))
    st.close = float(k.get("c", 0.0))
    st.volume = float(k.get("v", 0.0))
    st.start_time = int(k.get("t", 0))
    st.is_final = bool(k.get("x", False))

    pct = (st.close - st.open_price) / st.open_price if st.open_price > 0 else 0.0
    now_ts = time.time()

    if CONFIG["DEBUG_PRE_SIGNAL"] and pct >= (CONFIG["THRESHOLD_PCT"] * 0.5):
        if now_ts - st.last_pre_log_ts > CONFIG["PRE_SIGNAL_COOLDOWN"]:
            print(f"[PRE] {symbol} preço {fmt_pct(pct)} — verificando volume...")
            st.last_pre_log_ts = now_ts

    if st.vol_avg <= 0.0 and pct >= (CONFIG["THRESHOLD_PCT"] * 0.5) and (now_ts - st.last_vol_fetch_ts) > 60:
        st.vol_avg = compute_vol_avg(symbol, CONFIG["VOL_LOOKBACK"])
        st.last_vol_fetch_ts = now_ts

    vol_ok = st.vol_avg > 0.0 and st.volume >= CONFIG["VOLUME_MULTIPLIER"] * st.vol_avg
    pct_ok = pct >= CONFIG["THRESHOLD_PCT"]

    if vol_ok and pct_ok:
        need_exit_check = (
            st.last_exit_metrics is None
            or (now_ts - st.last_exit_check_ts) > CONFIG["EXIT_CHECK_COOLDOWN"]
        )
        if need_exit_check:
            exit_ok, metrics = check_exit_window_volatility(symbol)
            st.last_exit_check_ts = now_ts
            st.last_exit_check_ok = exit_ok
            st.last_exit_metrics = metrics
        if not st.last_exit_check_ok:
            if CONFIG["DEBUG_PRE_SIGNAL"] and st.last_exit_metrics:
                print(
                    f"[PRE] {symbol} rejeitado no filtro {CONFIG['TARGET_EXIT_MINUTES']}m: "
                    f"range={st.last_exit_metrics.get('range_pct', 0.0):.2f}% "
                    f"cum={st.last_exit_metrics.get('cum_move_pct', 0.0):.2f}%"
                )
            return None
        if now_ts - st.last_alert_ts < CONFIG["COOLDOWN_PER_SYMBOL"]:
            return None
        st.last_alert_ts = now_ts
        ts = now_tz().strftime("%Y-%m-%d %H:%M:%S %Z")
        lines = [
            f"🚀 MOMENTUM {symbol}",
            f"Tempo: {ts}",
            f"Preço: {st.close:.8f}",
            f"Candle {CONFIG['INTERVAL']}: {fmt_pct(pct)} (de {st.open_price:.8f} p/ {st.close:.8f})",
            f"Volume atual: {st.volume:.3f} (média {st.vol_avg:.3f})",
        ]
        if st.last_exit_metrics:
            window_minutes = st.last_exit_metrics.get("window_minutes") or (
                CONFIG["TARGET_EXIT_MINUTES"]
            )
            lines.append(
                "Janela {wm:.0f}m: range {rng:.2f}% | mov acum {cum:.2f}%".format(
                    wm=window_minutes,
                    rng=st.last_exit_metrics.get("range_pct", 0.0),
                    cum=st.last_exit_metrics.get("cum_move_pct", 0.0),
                )
            )
            lines.append(
                f"Média por candle: {st.last_exit_metrics.get('avg_move_pct', 0.0):.2f}%"
            )
        return "\n".join(lines)
    return None

# =========================
# WebSocket worker
# =========================
async def ws_worker(symbols_batch: List[str], state: Dict[str, SymbolState], idx: int):
    url = build_stream_url(symbols_batch)
    reconnect_delay = 1
    while True:
        try:
            async with websockets.connect(url, ping_interval=15, ping_timeout=20) as ws:
                print(f"[WS-{idx:02d}] Conectado: {len(symbols_batch)} symbols")
                reconnect_delay = 1
                async for raw in ws:
                    try:
                        msg = json.loads(raw)
                        alert = process_kline_msg(state, msg)
                        if alert:
                            print("\n" + alert)
                            try:
                                tg_send(alert)
                            except Exception:
                                pass
                            try:
                                sym = msg.get("data", {}).get("k", {}).get("s")
                                if sym and buy_market_and_place_oco is not None:
                                    asyncio.create_task(handle_momentum_and_trade(sym, alert))
                                elif not sym:
                                    print("[TRADER] Não foi possível extrair symbol para trade.")
                            except Exception as exc:
                                print("[TRADER] Erro ao agendar trade:", exc)
                    except Exception as exc:
                        print(f"[WS-{idx:02d}] [WARN] Erro processando msg: {exc}")
        except Exception as exc:
            print(f"[WS-{idx:02d}] [ERROR] desconectado: {exc}")
            print(f"[WS-{idx:02d}] Reconectando em {reconnect_delay}s...")
            await asyncio.sleep(reconnect_delay)
            reconnect_delay = min(reconnect_delay * 2, 60)

# =========================
# Execução do trade (thread)
# =========================
async def handle_momentum_and_trade(symbol: str, alert_text: str) -> None:
    global open_trades

    if buy_market_and_place_oco is None:
        print("[TRADER] trader.py não encontrado ou import falhou. Ignorando execução.")
        return

    if open_trades.get(symbol):
        print(f"[TRADER] Já existe ordem aberta para {symbol}, pulando execução.")
        return

    open_trades[symbol] = "PENDING"
    try:
        usdt_amt = float(os.getenv("TRADE_USDT_AMOUNT", "10"))
        tp_pct = float(os.getenv("TAKE_PROFIT_PCT", "0.01"))
        sl_pct = float(os.getenv("STOP_LOSS_PCT")) if os.getenv("STOP_LOSS_PCT") else None

        print(f"[TRADER] Iniciando trade em background para {symbol}: {usdt_amt} USDT, TP {tp_pct}, SL {sl_pct}")
        result = await asyncio.to_thread(
            buy_market_and_place_oco, symbol, usdt_amt, tp_pct, sl_pct
        )

        print("[TRADER] Order result:", result)
        try:
            tg_send(alert_text + "\n\n[TRADER] Ordem enviada (veja logs para detalhes).")
        except Exception:
            pass

        open_trades[symbol] = result
    except Exception as e:
        print("[TRADER] Erro ao executar trade:", e)
        try:
            tg_send(f"[TRADER] Erro ao executar trade para {symbol}: {e}")
        except Exception:
            pass
        open_trades.pop(symbol, None)

# =========================
# Reporter (heatmap periódico)
# =========================
def _make_signature(top_items, size):  # para HEATMAP_ONLY_WHEN_CHANGED
    return tuple((sym, round(pct, 3)) for pct, sym, _ in top_items[:size])

REPORTER_LAST_SIG = None
REPORTER_LAST_TELEGRAM_TS = 0.0

async def reporter_loop(state: Dict[str, SymbolState]):
    global REPORTER_LAST_SIG, REPORTER_LAST_TELEGRAM_TS
    while True:
        await asyncio.sleep(CONFIG["REPORT_TOP_MOVERS_EVERY"])
        items = []
        for sym, st in state.items():
            if st.open_price > 0:
                pct = (st.close - st.open_price) / st.open_price
                items.append((pct, sym, st))
        if not items:
            continue
        items.sort(reverse=True, key=lambda x: x[0])
        top = items[: CONFIG["TOP_MOVERS_N"]]
        ts = now_tz().strftime("%H:%M:%S")
        header = f"[HEATMAP {ts}] Top {len(top)} movers (candle {CONFIG['INTERVAL']}):"
        print("\n" + header)
        report_lines = [header]
        for i, (pct, sym, st) in enumerate(top, 1):
            vol_info = f"vol {st.volume:.2f}"
            if st.vol_avg > 0:
                vol_info += f" (avg {st.vol_avg:.2f})"
            line = f" {i:>2}. {sym:<15} +{fmt_pct(pct)}  {vol_info}"
            print(line)
            report_lines.append(line)

        if CONFIG.get("SEND_HEATMAP_TO_TELEGRAM") and top:
            top_pct = top[0][0]
            min_pct = CONFIG.get("HEATMAP_MIN_PCT_FOR_TELEGRAM", 0.0)
            cooldown = CONFIG.get("HEATMAP_TELEGRAM_COOLDOWN", 0)
            only_when_changed = CONFIG.get("HEATMAP_ONLY_WHEN_CHANGED", False)
            sig_size = CONFIG.get("HEATMAP_TOP_SIGNATURE_SIZE", 5)
            now_ts = time.time()

            send_ok = top_pct >= min_pct and (now_ts - REPORTER_LAST_TELEGRAM_TS) >= cooldown
            if send_ok and only_when_changed:
                sig = _make_signature(top, sig_size)
                if REPORTER_LAST_SIG == sig:
                    send_ok = False
                else:
                    REPORTER_LAST_SIG = sig
            if send_ok:
                try:
                    tg_send("\n".join(report_lines))
                    REPORTER_LAST_TELEGRAM_TS = now_ts
                except Exception:
                    pass

# =========================
# Loop principal
# =========================
async def scanner_loop():
    _print_effective_config()
    print("[INFO] Buscando todas as symbols Spot TRADING...")
    all_symbols = select_all_spot_symbols()
    print(f"[INFO] Total de symbols selecionadas: {len(all_symbols)}")
    state: Dict[str, SymbolState] = {}
    batches = [all_symbols[i : i + CONFIG["BATCH_SIZE"]] for i in range(0, len(all_symbols), CONFIG["BATCH_SIZE"])]
    print(f"[INFO] Total de conexões WS: {len(batches)} (batch={CONFIG['BATCH_SIZE']})")
    tasks = [asyncio.create_task(ws_worker(batch, state, i)) for i, batch in enumerate(batches)]
    tasks.append(asyncio.create_task(reporter_loop(state)))
    await asyncio.gather(*tasks)

def handle_sigint(signum, frame):
    print("\n[INFO] Encerrando por SIGINT...")
    raise SystemExit(0)

# =========================
# Entrypoint
# =========================
if __name__ == "__main__":
    # inicia o monitor de PnL em background (apenas uma vez, aqui)
    try:
        if monitor_open_trades is not None:
            threading.Thread(target=monitor_open_trades, args=(open_trades, 5), daemon=True).start()
            print("[INFO] monitor_open_trades thread started")
        else:
            print("[WARN] monitor_open_trades não disponível (import falhou).")
    except Exception as _exc:
        print("[WARN] falha ao iniciar monitor_open_trades:", _exc)

    signal.signal(signal.SIGINT, handle_sigint)
    try:
        asyncio.run(scanner_loop())
    except KeyboardInterrupt:
        pass
