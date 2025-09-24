#!/usr/bin/env python3
# trader.py — versão consolidada e robusta (Parte 1/7)
# - BUY por valor (quoteOrderQty) com ajuste minNotional e retry NOTIONAL
# - OCO-first + fallback (TP LIMIT_MAKER e STOP_LOSS_LIMIT)
# - "heal" pós-criação: garante que TP e STOP existam mesmo se algo falhar
# - monitor que notifica no Telegram quando SELL fecha (TP/SL/OCO) e calcula PnL
# - guardas para não recriar STOP quando OCO/STOP já estão ativos
# - cancel_order(), cleanup_orphan_stops(), cancel_remaining_sells(), cancel_oco()
# - CLI com: --account --open-orders --all-orders --oco --cancel-order --cleanup-orphans
#            --cancel-sells --cancel-oco --price
# - tg_send() embutido (usa TELEGRAM_BOT_TOKEN/TELEGRAM_CHAT_ID; se não setado, só printa)
# - logs opcionais com DEBUG_BINANCE=1
import os
import time
import hmac
import hashlib
import requests
from urllib.parse import urlencode
from math import floor, log10
import threading
import datetime
from typing import List, Dict, Optional, Any, Iterable
try:
    import pytz
except Exception:
    pytz = None  # se não houver pytz, caímos para UTC
# =========================
# Configs / Env
# =========================
API_KEY = os.getenv("BINANCE_API_KEY", "")
API_SECRET = os.getenv("BINANCE_API_SECRET", "")
USE_TESTNET = os.getenv("USE_TESTNET", "false").lower() in ("1", "true", "yes", "y")
BASE = "https://testnet.binance.vision" if USE_TESTNET else "https://api.binance.com"
PUBLIC_BASE = os.getenv("PUBLIC_BASE", "https://api.binance.com")
HEADERS = {"X-MBX-APIKEY": API_KEY} if API_KEY else {}
DEBUG_HTTP = os.getenv("DEBUG_BINANCE")
TG_BOT = os.getenv("TELEGRAM_BOT_TOKEN", "")
TG_CHAT = os.getenv("TELEGRAM_CHAT_ID", "")
# Caminho do CSV (pode sobrescrever via env TRADE_LOG_PATH)
LOG_PATH = os.getenv("TRADE_LOG_PATH", "/app/data/trades_log.csv")
# ======= Tuning anti-"OCO zumbi" e anti-spam de recuperação =======
ZOMBIE_MIN_AGE_S        = int(os.getenv("ZOMBIE_MIN_AGE_S", "45"))   # não avaliar 'zumbi' logo após criar
ZOMBIE_GRACE_S          = int(os.getenv("ZOMBIE_GRACE_S", "15"))     # tempo contínuo abaixo do stop
ZOMBIE_TICKS_MARGIN     = int(os.getenv("ZOMBIE_TICKS", "3"))        # folga em ticks (last <= stop - N*tick)
ZOMBIE_CONFIRM_TRIES    = int(os.getenv("ZOMBIE_CONFIRM_TRIES", "3"))
ZOMBIE_CONFIRM_INTERVAL = float(os.getenv("ZOMBIE_CONFIRM_INTERVAL","0.8"))
RECOVER_COOLDOWN_S      = int(os.getenv("RECOVER_COOLDOWN_S", "180")) # evita recuperação repetida do mesmo símbolo
PARAQUEDAS_SLIP         = float(os.getenv("PANIC_SLIP", "0.003"))      # 0.3%
PARAQUEDAS_SECS         = int(os.getenv("PANIC_SECS", "3"))
STOP_LIMIT_TICKS = int(os.getenv("STOP_LIMIT_TICKS", "12"))
STOP_LIMIT_GAP_PCT = float(os.getenv("STOP_LIMIT_GAP_PCT", "0.0015"))
MIN_RISK_REWARD_RATIO = float(os.getenv("MIN_RISK_REWARD_RATIO", "1.3"))
# ======= Filtro de volatilidade/momento =======
VOL_FILTER_ENABLED = os.getenv("VOL_FILTER_ENABLED", "true").lower() in ("1","true","yes","y")
VOL_WINDOW_MIN = int(os.getenv("VOL_WINDOW_MIN", "5"))            # janela mínima de leitura
MIN_RANGE_PCT = float(os.getenv("MIN_RANGE_PCT", "0.50"))         # exemplo: 0.50%
MIN_MOMENTUM_PCT_1M = float(os.getenv("MIN_MOMENTUM_PCT_1M", "0.10"))  # mantém o nome por compatibilidade
MIN_QUOTE_VOL_5M = float(os.getenv("MIN_QUOTE_VOL_5M", "100.0"))  # volume em USDT somado no período
MOM_LOOKBACK_MIN = int(os.getenv("MOM_LOOKBACK_MIN", "15"))       # janela robusta para momentum (padrão 15m)
# cache para cooldown por símbolo
_last_recover_attempt: dict[str, float] = {}
# =========================
# CSV logger
# =========================
def _log_trade(symbol: str, buy_price: float, exit_price: float, qty: float, reason: str):
    """
    Grava um registro de trade no CSV:
    - symbol, buy_price, exit_price, qty, pnl_pct, pnl_usdt, reason, timestamp_utc
    """
    import csv
    try:
        pnl_usdt = (exit_price - buy_price) * qty
        pnl_pct = ((exit_price - buy_price) / buy_price) if buy_price else 0.0
        row = {
            "timestamp_utc": datetime.datetime.utcnow().replace(microsecond=0).isoformat(),
            "symbol": symbol.upper(),
            "buy_price": f"{buy_price:.12f}",
            "exit_price": f"{exit_price:.12f}",
            "qty": f"{qty:.12f}",
            "pnl_pct": f"{pnl_pct:.6f}",
            "pnl_usdt": f"{pnl_usdt:.6f}",
            "reason": reason,  # "TP", "SL", "OCO", "SELL", etc.
        }
        need_header = not os.path.exists(LOG_PATH)
        with open(LOG_PATH, "a", newline="") as f:
            w = csv.DictWriter(f, fieldnames=row.keys())
            if need_header:
                w.writeheader()
            w.writerow(row)
    except Exception as e:
        print("[LOG] erro escrevendo trades_log.csv:", e)
# =========================
# Telegram helper
# =========================
def tg_send(text: str):
    try:
        if not TG_BOT or not TG_CHAT:
            print("[TG]", text)
            return
        url = f"https://api.telegram.org/bot{TG_BOT}/sendMessage"
        payload = {"chat_id": TG_CHAT, "text": text, "parse_mode": "Markdown"}
        r = requests.post(url, json=payload, timeout=10)
        if r.status_code >= 300:
            print("[TG] HTTP", r.status_code, r.text)
    except Exception as e:
        print("[TG] erro:", e)
# =========================
# Low-level signed/public
# =========================
def _sign(params: dict) -> str:
    # Retorna a query + signature (string)
    query = urlencode(params, doseq=True)
    sig = hmac.new(API_SECRET.encode(), query.encode(), hashlib.sha256).hexdigest()
    return query + "&signature=" + sig
def _request_signed(method: str, path: str, params: dict):
    params = params.copy() if params else {}
    params["timestamp"] = int(time.time() * 1000)
    # Log antes de assinar
    try:
        if DEBUG_HTTP:
            print("[HTTP] signed params pre-sign:", params)
    except Exception:
        pass
    qs_with_sig = _sign(params)
    url = BASE + path + "?" + qs_with_sig
    # Log da URL final
    try:
        if DEBUG_HTTP:
            print("[HTTP]", method.upper(), url)
    except Exception:
        pass
    try:
        if method.upper() == "POST":
            r = requests.post(url, headers=HEADERS, timeout=20)
        elif method.upper() == "DELETE":
            r = requests.delete(url, headers=HEADERS, timeout=20)
        else:
            r = requests.get(url, headers=HEADERS, timeout=20)
        if r.status_code >= 300:
            body = getattr(r, "text", "")
            print("[HTTP-ERR] body:", body)
            r.raise_for_status()
        return r.json()
    except requests.HTTPError as e:
        body = getattr(e.response, "text", "")
        print(f"[HTTP-ERR] {e} :: body={body}")
        raise
def safe_request_signed(method: str, path: str, params: dict):
    try:
        return _request_signed(method, path, params)
    except requests.HTTPError as e:
        status = getattr(e.response, "status_code", None)
        text = getattr(e.response, "text", "")
        print(f"[SAFE_REQUEST] HTTPError status={status} path={path} params={params} resp={text}")
        return {"error": True, "status": status, "message": text}
    except Exception as e:
        print(f"[SAFE_REQUEST] Unexpected error: {e}")
        return {"error": True, "message": str(e)}
def _request_public(path: str, params: dict = None):
    url = BASE + path
    r = requests.get(url, params=params or {}, timeout=20)
    r.raise_for_status()
    return r.json()
# =========================
# Rounding helpers
# =========================
def _step_size_round(q: float, step: float) -> float:
    step = float(step)
    if step <= 0:
        return float(q)
    return float(floor(q / step) * step)
def _tick_decimals(x: float) -> int:
    x = float(x)
    if x <= 0:
        return 0
    # ex.: tick=0.001 -> 3 casas
    return max(0, int(-floor(log10(x))))
def _price_round(p: float, tick: float) -> float:
    if float(tick) <= 0:
        return float(p)
    dec = _tick_decimals(tick)
    return float(round(round(p / float(tick)) * float(tick), dec))
def _fmt_price(p: float, tick: float) -> str:
    dec = _tick_decimals(tick)
    return f"{p:.{dec}f}"
def _fmt_qty(q: float, step: float) -> str:
    dec = _tick_decimals(step)
    return f"{q:.{dec}f}"


def _calc_stop_limit_price(stop_price: Optional[float], tick_size: float) -> Optional[float]:
    """Calcula um preço limit válido para STOP_LOSS_LIMIT próximo ao stop."""
    if stop_price is None:
        return None
    try:
        stop_val = float(stop_price)
        tick = float(tick_size)
    except Exception:
        return float(stop_price)
    if tick <= 0:
        return float(stop_val)
    candidates: List[float] = []
    base_candidate = _price_round(stop_val - tick, tick)
    if base_candidate < stop_val:
        candidates.append(base_candidate)
    if STOP_LIMIT_TICKS > 0:
        tick_candidate = _price_round(stop_val - STOP_LIMIT_TICKS * tick, tick)
        if tick_candidate < stop_val:
            candidates.append(tick_candidate)
    if STOP_LIMIT_GAP_PCT > 0:
        pct_candidate = _price_round(stop_val * (1 - STOP_LIMIT_GAP_PCT), tick)
        if pct_candidate < stop_val:
            candidates.append(pct_candidate)
    if not candidates:
        return base_candidate
    return max(candidates)


def _enforce_risk_reward(
    take_profit_pct: Optional[float], stop_loss_pct: Optional[float]
) -> tuple[Optional[float], Optional[float]]:
    """Garante que o take profit tenha relação mínima com o stop loss."""
    try:
        tp = float(take_profit_pct) if take_profit_pct else None
    except Exception:
        tp = None
    try:
        sl = float(stop_loss_pct) if stop_loss_pct else None
    except Exception:
        sl = None
    if not tp or not sl or sl <= 0:
        return take_profit_pct, stop_loss_pct
    if MIN_RISK_REWARD_RATIO <= 0:
        return take_profit_pct, stop_loss_pct
    current_ratio = tp / sl if sl else float("inf")
    if current_ratio >= MIN_RISK_REWARD_RATIO:
        return tp, sl
    new_tp = sl * MIN_RISK_REWARD_RATIO
    print(
        "[RISK] Ajustando take_profit_pct de "
        f"{tp:.6f} para {new_tp:.6f} (RR mínimo {MIN_RISK_REWARD_RATIO:.2f})"
    )
    return new_tp, sl
# =========================
# Helpers de verificação/heal
# =========================
def _approx_equal_price(p1: float, p2: float, tick: float) -> bool:
    # considera iguais dentro de meio tick
    return abs(float(p1) - float(p2)) <= float(tick) * 0.51
def verify_and_heal_post_sell(
    symbol: str,
    quantity: float,
    tp_price: float,
    stop_price: float,
    info: dict,
    notify: bool = True,
):
    """
    Garante que existam TP (LIMIT/MAKER) e STOP_LOSS_LIMIT para o symbol.
    Se houver OCO, está ok. Caso contrário, cria o que estiver faltando.
    """
    qty_round = _step_size_round(quantity, info["stepSize"])
    if qty_round <= 0:
        return {"healed": False, "reason": "qty_round<=0"}
    qty_str = _fmt_qty(qty_round, info["stepSize"])
    tp_round = _price_round(tp_price, info["tickSize"]) if tp_price is not None else None
    sp_round = _price_round(stop_price, info["tickSize"]) if stop_price is not None else None
    stop_limit_price = _calc_stop_limit_price(sp_round, info["tickSize"]) if sp_round is not None else None
    tp_str = _fmt_price(tp_round, info["tickSize"]) if tp_round is not None else None
    sp_str = _fmt_price(sp_round, info["tickSize"]) if sp_round is not None else None
    stop_limit_str = _fmt_price(stop_limit_price, info["tickSize"]) if stop_limit_price is not None else None
    opens = []
    try:
        opens = get_open_orders(symbol)
    except Exception as e:
        print("[HEAL] erro get_open_orders:", e)
    # Se qualquer ordem tem orderListId != -1, já há OCO
    has_oco = any(int(o.get("orderListId", -1)) != -1 for o in opens)
    has_tp = False
    has_stop = False
    for o in opens:
        if o.get("side") == "SELL":
            typ = o.get("type")
            if typ in ("LIMIT_MAKER", "LIMIT"):
                if tp_round is not None and _approx_equal_price(float(o.get("price", 0)), tp_round, info["tickSize"]):
                    has_tp = True
            elif typ == "STOP_LOSS_LIMIT":
                ok_sp = sp_round is not None and _approx_equal_price(float(o.get("stopPrice", 0)), sp_round, info["tickSize"])
                ok_lp = stop_limit_price is not None and _approx_equal_price(float(o.get("price", 0)), stop_limit_price, info["tickSize"])
                if ok_sp and ok_lp:
                    has_stop = True
    if has_oco:
        return {"healed": False, "reason": "has_oco", "has_tp": True, "has_stop": True}
    healed = False
    creations = {}
    # cria TP se faltar
    if (tp_round is not None) and not has_tp:
        try:
            params_tp = {
                "symbol": symbol,
                "side": "SELL",
                "type": "LIMIT_MAKER",
                "quantity": qty_str,
                "price": tp_str,
            }
            print(f"[HEAL] criando TP LIMIT_MAKER {symbol} @ {tp_str}")
            creations["tp"] = _request_signed("POST", "/api/v3/order", params_tp)
            healed = True
        except Exception as e:
            print("[HEAL] erro criando TP LIMIT_MAKER:", e)
            # fallback LIMIT normal
            try:
                params_tp2 = {
                    "symbol": symbol,
                    "side": "SELL",
                    "type": "LIMIT",
                    "timeInForce": "GTC",
                    "quantity": qty_str,
                    "price": tp_str,
                }
                creations["tp"] = safe_request_signed("POST", "/api/v3/order", params_tp2)
                healed = True
            except Exception as e2:
                print("[HEAL] erro criando TP LIMIT:", e2)
    # cria STOP se faltar
    if (sp_round is not None) and not has_stop:
        try:
            params_stop = {
                "symbol": symbol,
                "side": "SELL",
                "type": "STOP_LOSS_LIMIT",
                "quantity": qty_str,
                "stopPrice": sp_str,
                "price": stop_limit_str,
                "timeInForce": "GTC",
            }
            print(f"[HEAL] criando STOP_LOSS_LIMIT {symbol} stop={sp_str} limit={stop_limit_str}")
            creations["stop"] = safe_request_signed("POST", "/api/v3/order", params_stop)
            healed = True
        except Exception as e:
            print("[HEAL] erro criando STOP_LOSS_LIMIT:", e)
    if healed and notify:
        try:
            tg_send(f"🩺 *Heal aplicado em {symbol}*\n• TP criado: {bool(creations.get('tp'))}\n• STOP criado: {bool(creations.get('stop'))}")
        except Exception:
            pass
    return {"healed": healed, "created": creations, "has_tp": has_tp, "has_stop": has_stop}
# =========================
# Exchange info
# =========================
def get_symbol_info(symbol: str):
    info = _request_public("/api/v3/exchangeInfo")
    for s in info.get("symbols", []):
        if s.get("symbol") == symbol:
            flt = {f["filterType"]: f for f in s.get("filters", [])}
            lot = flt.get("LOT_SIZE", {})
            pricef = flt.get("PRICE_FILTER", {})
            min_notional = flt.get("MIN_NOTIONAL", {})
            return {
                "stepSize": float(lot.get("stepSize", 0.0) or 0.0),
                "minQty": float(lot.get("minQty", 0.0) or 0.0),
                "tickSize": float(pricef.get("tickSize", 0.0) or 0.0),
                "minPrice": float(pricef.get("minPrice", 0.0) or 0.0),
                "minNotional": float(min_notional.get("minNotional", 0.0) or 0.0),
            }
    raise Exception("symbol not found in exchangeInfo")
# =========================
# Preço (last/bid/ask)
# =========================
def get_price(symbol: str) -> dict:
    # preço "last" e book (bid/ask) oficiais da Binance
    last = _request_public("/api/v3/ticker/price", params={"symbol": symbol})
    book = _request_public("/api/v3/ticker/bookTicker", params={"symbol": symbol})
    return {
        "symbol": symbol,
        "lastPrice": float(last.get("price", 0)),
        "bidPrice": float(book.get("bidPrice", 0)),
        "askPrice": float(book.get("askPrice", 0)),
    }
# =========================
# Helpers diversos (conta/ordens)
# =========================
def _request_klines(symbol: str, interval: str = "1m", limit: int = 5):
    try:
        return _request_public_mkt("/api/v3/klines", params={"symbol": symbol, "interval": interval, "limit": limit})
    except requests.HTTPError as e:
        sc = getattr(e.response, "status_code", None)
        if sc in (400, 404):
            return []  # sem dados/sem símbolo -> tratamos como "sem klines"
        raise
def _request_ticker24(symbol: str):
    # fallback quando o testnet não dá klines
    try:
        return _request_public_mkt("/api/v3/ticker/24hr", params={"symbol": symbol})
    except requests.HTTPError:
        return {}
# ==== [FUNÇÃO DE VOLATILIDADE ROBUSTA] ====
def check_symbol_volatility(symbol: str) -> tuple[bool, dict, str]:
    """
    1) Usa klines 1m (N=VOL_WINDOW_MIN) para métricas intraday.
    2) Se não houver klines (testnet), cai no 24h ticker (menos preciso).
    3) Filtros extras:
       - momentum robusto (máximo bar-a-bar em 15m, configurável)
       - spread bid/ask máximo
       - volume médio por candle mínimo (quote)
    Retorna (ok, metrics, reason)
    """
    try:
        N = max(5, VOL_WINDOW_MIN)  # janela mínima razoável
        ks = _request_klines(symbol, "1m", N)

        # --- BOOK para spread ---
        try:
            bt = _request_public("/api/v3/ticker/bookTicker", params={"symbol": symbol})
            bid = float(bt.get("bidPrice") or 0)
            ask = float(bt.get("askPrice") or 0)
            mid = (bid + ask) / 2.0 if (bid > 0 and ask > 0) else 0.0
            spread_pct = ((ask - bid) / mid * 100.0) if mid > 0 else 999.0
        except Exception:
            spread_pct = 999.0

        # thresholds extras (com defaults sensatos)
        MOM_LOOKBACK_MIN = int(os.getenv("MOM_LOOKBACK_MIN", "15"))  # 15 velas de 1m
        MAX_SPREAD_PCT = float(os.getenv("MAX_SPREAD_PCT", "0.12"))  # ex.: 0.12% máx
        MIN_AVG_QUOTE_VOL_PER_BAR = float(os.getenv("MIN_AVG_QUOTE_VOL_PER_BAR", "80.0"))

        def _pct(a: float, b: float) -> float:
            return 0.0 if a == 0 else (b - a) / a * 100.0

        if ks and len(ks) >= 2:
            highs   = [float(k[2]) for k in ks]
            lows    = [float(k[3]) for k in ks]
            opens   = [float(k[1]) for k in ks]
            closes  = [float(k[4]) for k in ks]
            qvols   = [float(k[7]) for k in ks]  # quoteAssetVolume por candle

            max_h = max(highs)
            min_l = min(lows)
            midr  = (max_h + min_l) / 2.0 if (max_h + min_l) > 0 else closes[-1]

            range_pct = 0.0 if midr == 0 else (max_h - min_l) / midr * 100.0
            mom_1m    = abs(_pct(closes[-2], closes[-1]))
            qvol_sum  = sum(qvols)
            qvol_avg  = (qvol_sum / len(qvols)) if qvols else 0.0

            # ---- momentum robusto 15m (máximo bar-a-bar + intra do último) ----
            look = max(1, min(MOM_LOOKBACK_MIN, len(closes) - 1))
            mom_max_k = 0.0
            start = max(1, len(closes) - look)
            for i in range(start, len(closes)):
                if closes[i-1] > 0:
                    mom_max_k = max(mom_max_k, abs(_pct(closes[i-1], closes[i])))
            intra_last = abs(_pct(opens[-1], closes[-1]))
            mom_robust = max(mom_max_k, intra_last)

            metrics = {
                "range_pct": range_pct,
                "momentum_1m_pct": mom_1m,
                "momentum_robust_15m_pct": mom_robust,
                "quote_vol_sum": qvol_sum,
                "avg_quote_vol_per_bar": qvol_avg,
                "spread_pct": spread_pct,
                "window_min": N,
                "source": "klines",
            }
        else:
            # ---- Fallback 24h (menos preciso) ----
            t = _request_ticker24(symbol) or {}
            try:
                high = float(t.get("highPrice", 0) or 0.0)
                low  = float(t.get("lowPrice", 0) or 0.0)
                last = float(t.get("lastPrice", 0) or 0.0)
                mom_1m = abs(float(t.get("priceChangePercent", 0) or 0.0))  # aproximação 24h
                qvol = float(t.get("quoteVolume", 0) or 0.0)
            except Exception:
                high = low = last = mom_1m = qvol = 0.0

            midr = (high + low) / 2.0 if (high + low) > 0 else last
            range_pct = 0.0 if midr == 0 else (high - low) / midr * 100.0

            metrics = {
                "range_pct": range_pct,
                "momentum_1m_pct": mom_1m,
                "momentum_robust_15m_pct": mom_1m,  # sem klines, usa proxy
                "quote_vol_sum": qvol,
                "avg_quote_vol_per_bar": qvol / max(1, N),  # aproximação grosseira
                "spread_pct": spread_pct,
                "window_min": N,
                "source": "24h",
            }

        # ---- Regras (inclui os novos cortes) ----
        if metrics["spread_pct"] > MAX_SPREAD_PCT:
            return False, metrics, f"spread {metrics['spread_pct']:.3f}% > {MAX_SPREAD_PCT:.3f}%"
        if metrics["avg_quote_vol_per_bar"] < MIN_AVG_QUOTE_VOL_PER_BAR:
            return False, metrics, f"avgQVol {metrics['avg_quote_vol_per_bar']:.2f} < {MIN_AVG_QUOTE_VOL_PER_BAR:.2f}"
        if metrics["range_pct"] < MIN_RANGE_PCT:
            return False, metrics, f"range {metrics['range_pct']:.3f}% < {MIN_RANGE_PCT:.3f}%"
        # mantém o 1m como “gatilho imediato”, mas exige robustez 15m também
        if metrics["momentum_1m_pct"] < MIN_MOMENTUM_PCT_1M:
            return False, metrics, f"mom1m {metrics['momentum_1m_pct']:.3f}% < {MIN_MOMENTUM_PCT_1M:.3f}%"

        # robustez 15m mínima (env separada para ajustar sem mexer no 1m)
        MIN_MOMENTUM_ROBUST_15M = float(os.getenv("MIN_MOMENTUM_ROBUST_15M", "0.25"))
        if metrics["momentum_robust_15m_pct"] < MIN_MOMENTUM_ROBUST_15M:
            return False, metrics, f"mom15m {metrics['momentum_robust_15m_pct']:.3f}% < {MIN_MOMENTUM_ROBUST_15M:.3f}%"

        return True, metrics, "ok"

    except Exception as e:
        m = {
            "window_min": VOL_WINDOW_MIN,
            "range_pct": 0.0,
            "momentum_1m_pct": 0.0,
            "momentum_robust_15m_pct": 0.0,
            "quote_vol_sum": 0.0,
            "avg_quote_vol_per_bar": 0.0,
            "spread_pct": 999.0,
            "source": "err",
        }
        return False, m, f"erro volatilidade: {e}"

def get_account():
    return _request_signed("GET", "/api/v3/account", {})
def get_open_orders(symbol: Optional[str] = None) -> List[Dict[str, Any]]:
    params = {}
    if symbol and symbol != "ALL":
        params["symbol"] = symbol
    return _request_signed("GET", "/api/v3/openOrders", params)
def get_all_orders(symbol: str, limit: int = 50) -> List[Dict[str, Any]]:
    params = {"symbol": symbol, "limit": limit}
    return _request_signed("GET", "/api/v3/allOrders", params)
def get_order(symbol: str, orderId: int) -> Dict[str, Any]:
    return _request_signed("GET", "/api/v3/order", {"symbol": symbol, "orderId": orderId})
def get_oco_list(orderListId: Optional[int] = None, originalClientOrderId: Optional[str] = None) -> Dict[str, Any]:
    # Para um OCO específico, o endpoint recomendado é /api/v3/orderList
    params = {}
    if orderListId:
        params["orderListId"] = orderListId
        return _request_signed("GET", "/api/v3/orderList", params)
    if originalClientOrderId:
        params["origClientOrderId"] = originalClientOrderId
        return _request_signed("GET", "/api/v3/orderList", params)
    # Fallback: lista todos (cuidado com volume)
    return _request_signed("GET", "/api/v3/allOrderList", {})
def cancel_order(symbol: str, order_id: int):
    return _request_signed("DELETE", "/api/v3/order", {"symbol": symbol, "orderId": int(order_id)})
def cancel_remaining_sells(symbol: str, exclude_order_id: Optional[int] = None):
    """
    Cancela TODAS as ordens SELL pendentes (NEW/WORKING) do símbolo,
    exceto, opcionalmente, uma ordem específica (exclude_order_id).
    Útil para remover STOPs que ficaram órfãos quando o TP já executou.
    """
    try:
        opens = get_open_orders(symbol)
    except Exception as e:
        print(f"[CANCEL-REMAINING] erro listando open_orders {symbol}: {e}")
        return
    for o in opens:
        try:
            if o.get("side") != "SELL":
                continue
            if o.get("status") not in ("NEW", "WORKING"):
                continue
            oid = int(o.get("orderId"))
            if exclude_order_id and oid == int(exclude_order_id):
                continue
            _request_signed("DELETE", "/api/v3/order", {"symbol": symbol, "orderId": oid})
            print(f"[CANCEL-REMAINING] Cancelada SELL pendente {symbol} #{oid}")
        except Exception as e:
            print(f"[CANCEL-REMAINING] erro cancelando {symbol} #{o.get('orderId')}: {e}")
# =========================
# OCO-first + fallback (com HEAL)
# =========================
def place_oco_or_fallback(symbol: str, quantity: float, tp_price: float, stop_price: float):
    info = get_symbol_info(symbol)
    qty_round = _step_size_round(quantity, info["stepSize"])
    if qty_round <= 0:
        raise Exception("Quantidade arredondada <= 0")
    qty_str = _fmt_qty(qty_round, info["stepSize"])
    tp_round = _price_round(tp_price, info["tickSize"])
    sp_round = _price_round(stop_price, info["tickSize"])
    stop_limit_price = _calc_stop_limit_price(sp_round, info["tickSize"])
    tp_str = _fmt_price(tp_round, info["tickSize"])
    sp_str = _fmt_price(sp_round, info["tickSize"])
    stop_limit_str = _fmt_price(stop_limit_price, info["tickSize"])
    # 1) Tenta OCO
    params_oco = {
        "symbol": symbol,
        "side": "SELL",
        "quantity": qty_str,
        "price": tp_str,
        "stopPrice": sp_str,
        "stopLimitPrice": stop_limit_str,
        "stopLimitTimeInForce": "GTC",
    }
    try:
        oco = _request_signed("POST", "/api/v3/order/oco", params_oco)
        print(f"[TRADER] OCO criado: {symbol} TP={tp_str} STOP={sp_str} (orderListId={oco.get('orderListId')})")
        return {"oco": oco, "tp": None, "stop": None}
    except Exception as e:
        try:
            if isinstance(e, requests.HTTPError) and getattr(e.response, "text", None):
                print(f"[TRADER] OCO fail resp: {e.response.text}")
            else:
                print(f"[TRADER] OCO fail: {e}")
        except Exception:
            print(f"[TRADER] OCO fail (no details): {e}")
    # 2) Fallback TP LIMIT_MAKER -> se falhar, LIMIT GTC
    tp_res = None
    try:
        params_tp = {
            "symbol": symbol,
            "side": "SELL",
            "type": "LIMIT_MAKER",
            "quantity": qty_str,
            "price": tp_str,
        }
        print(f"[TRADER] Fallback: LIMIT_MAKER TP {tp_str}")
        tp_res = _request_signed("POST", "/api/v3/order", params_tp)
    except Exception as e:
        print(f"[TRADER] Erro LIMIT_MAKER TP: {e}")
        try:
            params_tp2 = {
                "symbol": symbol,
                "side": "SELL",
                "type": "LIMIT",
                "timeInForce": "GTC",
                "quantity": qty_str,
                "price": tp_str,
            }
            print(f"[TRADER] Fallback: LIMIT GTC TP {tp_str}")
            tp_res = safe_request_signed("POST", "/api/v3/order", params_tp2)
        except Exception as e2:
            print(f"[TRADER] LIMIT TP erro: {e2}")
            tp_res = {"error": True, "message": str(e2)}
    # 3) STOP_LOSS_LIMIT
    stop_res = None
    try:
        params_stop_limit = {
            "symbol": symbol,
            "side": "SELL",
            "type": "STOP_LOSS_LIMIT",
            "quantity": qty_str,
            "stopPrice": sp_str,
            "price": stop_limit_str,
            "timeInForce": "GTC",
        }
        print(f"[TRADER] Fallback: STOP_LOSS_LIMIT stop={sp_str} limit={stop_limit_str}")
        stop_res = safe_request_signed("POST", "/api/v3/order", params_stop_limit)
    except Exception as e:
        print(f"[TRADER] Erro STOP_LOSS_LIMIT: {e}")
        stop_res = {"error": True, "message": str(e)}
    # 4) HEAL: confere e recria o que faltar (evita pares zumbis)
    try:
        verify_and_heal_post_sell(symbol, qty_round, tp_round, sp_round, info, notify=True)
    except Exception as e:
        print("[TRADER] heal pós-fallback falhou:", e)
    return {"oco": None, "tp": tp_res, "stop": stop_res}
# =========================
# Buy + place OCO/TP+STOP
# =========================
def buy_market_and_place_oco(
    symbol: str,
    usdt_amount: float,
    take_profit_pct: float = 0.01,
    stop_loss_pct: float = None
):
    if not API_KEY or not API_SECRET:
        raise Exception("BINANCE_API_KEY/SECRET not set")
    # Filtro de volatilidade/momento (bloqueia entradas em moeda "parada")
    if VOL_FILTER_ENABLED:
        ok, m, why = check_symbol_volatility(symbol)
        winm = int(m.get('window_min') or max(VOL_WINDOW_MIN, MOM_LOOKBACK_MIN))
        if not ok:
            print(f"[VOL-FILTER] {symbol} bloqueado: {why} "
                  f"(src={m.get('source','?')}, win={winm}m, "
                  f"range={m.get('range_pct',0):.3f}%, "
                  f"mom1m={m.get('momentum_1m_pct',0):.3f}%, "
                  f"qVolSum={m.get('quote_vol_sum',0):.2f})")
            raise Exception(f"[VOL-FILTER] {symbol} bloqueado: {why}")
        else:
            print(f"[VOL-FILTER] {symbol} OK "
                  f"(src={m.get('source','?')}) — range={m['range_pct']:.3f}% "
                  f"mom1m={m['momentum_1m_pct']:.3f}% qVolSum={m['quote_vol_sum']:.2f}")
    # usa ask (bookTicker) como referência de “preço de entrada” para cálculo TP/SL
    t = _request_public("/api/v3/ticker/bookTicker", params={"symbol": symbol})
    ask = float(t["askPrice"])
    price_ref = ask
    info = get_symbol_info(symbol)
    # Ajuste para minNotional
    min_notional = float(info.get("minNotional", 0.0) or 0.0)
    if min_notional and usdt_amount < min_notional:
        bump = round(min_notional * 1.02, 8)
        print(f"[TRADER] quoteOrderQty {usdt_amount} < minNotional {min_notional}; ajustando para {bump}")
        usdt_amount = bump
    # ===== BUY por valor (quoteOrderQty) =====
    buy_params = {
        "symbol": symbol,
        "side": "BUY",
        "type": "MARKET",
        "quoteOrderQty": str(usdt_amount),
    }
    print(f"[TRADER] MARKET BUY {symbol} quoteOrderQty={usdt_amount}")
    # Retry em NOTIONAL
    try:
        order = _request_signed("POST", "/api/v3/order", buy_params)
    except requests.HTTPError as e:
        body = getattr(e.response, "text", "")
        if "NOTIONAL" in str(body).upper():
            bump = round(max(float(usdt_amount), float(min_notional or 0.0) * 1.05, 11.0), 8)
            print(f"[TRADER] NOTIONAL falhou com {usdt_amount}; reajustando para {bump}")
            buy_params["quoteOrderQty"] = str(bump)
            order = _request_signed("POST", "/api/v3/order", buy_params)
        else:
            raise
    # qty/preço efetivos executados
    fills = order.get("fills", []) or []
    executed_qty = sum(float(f.get("qty", 0)) for f in fills) or float(order.get("executedQty", 0) or 0.0)
    if executed_qty <= 0:
        raise Exception("Executado 0. Verifique filtros/mercado.")
    if fills:
        numerator = sum(float(f["qty"]) * float(f["price"]) for f in fills)
        executed_price = numerator / executed_qty if executed_qty > 0 else price_ref
    else:
        executed_price = price_ref
    # Ajusta RR mínimo antes de calcular preços alvo
    take_profit_pct, stop_loss_pct = _enforce_risk_reward(take_profit_pct, stop_loss_pct)
    # Define TP/SL a partir do preço executado
    tp_price = _price_round(executed_price * (1 + (take_profit_pct or 0.0)), info["tickSize"]) if take_profit_pct else None
    stop_price = _price_round(executed_price * (1 - (stop_loss_pct or 0.0)), info["tickSize"]) if stop_loss_pct else None
    # prefer OCO quando possível
    if stop_price is not None and tp_price is not None:
        res = place_oco_or_fallback(symbol, executed_qty, tp_price, stop_price)
    else:
        # apenas TP
        qty_str = _fmt_qty(_step_size_round(executed_qty, info["stepSize"]), info["stepSize"])
        tp_str = _fmt_price(tp_price, info["tickSize"]) if tp_price else None
        try:
            params_tp = {
                "symbol": symbol,
                "side": "SELL",
                "type": "LIMIT_MAKER",
                "quantity": qty_str,
                "price": tp_str,
            }
            tp_res = _request_signed("POST", "/api/v3/order", params_tp)
            res = {"oco": None, "tp": tp_res, "stop": None}
        except Exception as e:
            res = {"oco": None, "tp": {"error": True, "message": str(e)}, "stop": None}
    # Verificação extra: garante TP+SL mesmo se criação falhar silenciosamente
    try:
        if tp_price is not None and stop_price is not None:
            verify_and_heal_post_sell(symbol, executed_qty, tp_price, stop_price, info, notify=False)
    except Exception as e:
        print("[TRADER] pós-buy heal falhou:", e)
    # Notificações
    try:
        if res.get("oco"):
            oco = res["oco"]
            tg_send(f"✅ OCO criado {symbol} — orderListId: {oco.get('orderListId')}")
        else:
            tp_ok = res.get("tp") and not (isinstance(res.get("tp"), dict) and res.get("tp").get("error"))
            stop_ok = res.get("stop") and not (isinstance(res.get("stop"), dict) and res.get("stop").get("error"))
            tg_send(f"✅ Fallback executado para {symbol}\nTP ok: {tp_ok} | STOP ok: {stop_ok}")
    except Exception:
        pass
    # iniciar monitor/recovery em background
    try:
        tp_res = res.get("tp")
        stop_res = res.get("stop")
        monitor_and_recover_trade(symbol, executed_qty, tp_res, stop_res)
    except Exception as e:
        print("[TRADER] erro iniciando monitor de recuperação:", e)
    return {"buy": order, "tp": res.get("tp"), "stop": res.get("stop"), "oco": res.get("oco")}
# =========================
# Monitor de recuperação
# =========================
def _format_ts(ms: int) -> str:
    try:
        return datetime.datetime.utcfromtimestamp(ms/1000).strftime("%Y-%m-%d %H:%M:%S UTC")
    except Exception:
        return str(ms)
def _safe_sell_stop_from_last(last_price: float, tick: float, ticks_buffer: int = 3):
    """
    Para STOP_LOSS_LIMIT de SELL:
    - stopPrice alguns 'ticks_buffer' abaixo do último preço de referência;
    - limit price 1 tick abaixo do stop.
    """
    if last_price is None or last_price <= 0:
        return None, None
    stop_price = _price_round(last_price - ticks_buffer * float(tick), tick)
    limit_price = _price_round(stop_price - 1 * float(tick), tick)
    return stop_price, limit_price
def _is_trigger_immediately(err: dict) -> bool:
    msg = (err or {}).get("message") or ""
    return "trigger immediately" in msg.lower()
def monitor_and_recover_trade(symbol: str, executed_qty: float, tp_order_res, stop_order_res, order_timeout: int = 8):
    def _worker():
        try:
            time.sleep(1)
            # Cooldown anti-spam
            now = time.time()
            if _last_recover_attempt.get(symbol, 0) + RECOVER_COOLDOWN_S > now:
                print(f"[RECOVER] {symbol}: em cooldown, não vou tentar agora.")
                return
            _last_recover_attempt[symbol] = now
            # Se já houver STOP/ OCO ativo, não recria
            try:
                opens = get_open_orders(symbol)
                has_oco = any(o.get("orderListId", -1) != -1 for o in opens)
                has_stop_working = any(
                    o.get("side") == "SELL"
                    and o.get("type") in ("STOP_LOSS_LIMIT",)
                    and o.get("status") in ("NEW", "WORKING")
                    for o in opens
                )
                if has_oco or has_stop_working:
                    print(f"[RECOVER] {symbol}: OCO/STOP ativo; não vou recriar.")
                else:
                    print(f"[RECOVER] {symbol}: sem OCO/STOP working; vou observar antes de agir.")
            except Exception:
                pass
            # ler ids dos legs recebidos
            stop_id = (stop_order_res or {}).get("orderId")
            tp_id   = (tp_order_res or {}).get("orderId")
            stop_ok = False
            tp_expired = False
            try:
                if stop_id:
                    o = get_order(symbol, int(stop_id))
                    if o and (o.get("isWorking") is True or o.get("status") in ("NEW", "WORKING")):
                        stop_ok = True
            except Exception:
                pass
            try:
                if tp_id:
                    t = get_order(symbol, int(tp_id))
                    if t and t.get("status") in ("EXPIRED",):
                        tp_expired = True
            except Exception:
                pass
            if stop_ok and not tp_expired:
                return
            # Descobre stopPrice de referência
            info = get_symbol_info(symbol)
            tick = float(info["tickSize"])
            stop_cfg = None
            try:
                if stop_id:
                    od = get_order(symbol, int(stop_id))
                    sp = od.get("stopPrice") or od.get("origStopPrice") or od.get("price")
                    if sp:
                        stop_cfg = float(sp)
            except Exception:
                pass
            if not stop_cfg or stop_cfg <= 0:
                print(f"[RECOVER] {symbol}: sem stop_cfg; não vou recriar nada.")
                return
            below_margin = (ZOMBIE_TICKS_MARGIN * tick) if tick > 0 else 0.0
            # Confirmação multi-amostra: last <= stop_cfg - margem por janela contínua
            observed_ok = False
            for _ in range(ZOMBIE_CONFIRM_TRIES):
                try:
                    lastp = float(_request_public("/api/v3/ticker/price", params={"symbol": symbol}).get("price") or 0.0)
                except Exception:
                    lastp = 0.0
                if lastp > 0 and lastp <= (stop_cfg - below_margin):
                    t0 = time.time()
                    good = True
                    while time.time() - t0 < ZOMBIE_GRACE_S:
                        try:
                            last2 = float(_request_public("/api/v3/ticker/price", params={"symbol": symbol}).get("price") or 0.0)
                        except Exception:
                            last2 = lastp
                        try:
                            leg_refresh = get_order(symbol, int(stop_id))
                            if leg_refresh.get("isWorking") is True or str(leg_refresh.get("status","")).upper() in ("FILLED","CANCELED","EXPIRED","REJECTED"):
                                good = False
                                break
                        except Exception:
                            pass
                        if not (last2 > 0 and last2 <= (stop_cfg - below_margin)):
                            good = False
                            break
                        time.sleep(0.5)
                    if good:
                        observed_ok = True
                        break
                time.sleep(ZOMBIE_CONFIRM_INTERVAL)
            if not observed_ok:
                print(f"[RECOVER] {symbol}: condição de recriar STOP não confirmada. Abortando.")
                return
            # Revalida se algum STOP ficou working enquanto observávamos
            try:
                o2 = get_open_orders(symbol)
                if any(
                    oo.get("side") == "SELL"
                    and oo.get("type") == "STOP_LOSS_LIMIT"
                    and oo.get("status") in ("NEW","WORKING")
                    for oo in o2
                ):
                    print(f"[RECOVER] {symbol}: STOP ficou working. Nada a fazer.")
                    return
            except Exception:
                pass
            # Recria STOP com buffer — sem market sell automático em falha
            try:
                tk = _request_public("/api/v3/ticker/bookTicker", params={"symbol": symbol})
                last_ref = float(tk.get("bidPrice") or tk.get("askPrice") or 0.0)
                if last_ref <= 0:
                    print(f"[RECOVER] {symbol}: sem referência de book. Abortando.")
                    return
                new_stop = _price_round(last_ref - max(3, STOP_LIMIT_TICKS) * tick, tick)
                new_limit = _calc_stop_limit_price(new_stop, tick)
                qty_str = _fmt_qty(_step_size_round(executed_qty, info["stepSize"]), info["stepSize"])
                params_stop = {
                    "symbol": symbol,
                    "side": "SELL",
                    "type": "STOP_LOSS_LIMIT",
                    "quantity": qty_str,
                    "stopPrice": _fmt_price(new_stop,  tick),
                    "price":     _fmt_price(new_limit, tick),
                    "timeInForce":"GTC",
                }
                stop_retry = safe_request_signed("POST", "/api/v3/order", params_stop)
                if isinstance(stop_retry, dict) and stop_retry.get("error") and _is_trigger_immediately(stop_retry):
                    new_stop = _price_round(last_ref - 5 * tick, tick)
                    new_limit = _calc_stop_limit_price(new_stop, tick)
                    params_stop["stopPrice"] = _fmt_price(new_stop,  tick)
                    params_stop["price"]     = _fmt_price(new_limit, tick)
                    stop_retry = safe_request_signed("POST", "/api/v3/order", params_stop)
                try:
                    tg_send(f"🔁 *Recuperação STOP* {symbol}\nNovo STOP_LOSS_LIMIT: stop `{params_stop['stopPrice']}` / limit `{params_stop['price']}`\nResp: `{stop_retry}`")
                except Exception:
                    pass
                if isinstance(stop_retry, dict) and stop_retry.get("error"):
                    print(f"[RECOVER] {symbol}: recriação de STOP falhou, mas não vou vender a mercado.")
                    return
            except Exception as e:
                print(f"[RECOVER] {symbol}: erro ao recriar STOP: {e}")
                return
        except Exception as e:
            print("[monitor_and_recover_trade] erro:", e)
    thread = threading.Thread(target=_worker, daemon=True)
    thread.start()
    return thread
# =========================
# Monitor de trades abertos (notifica PnL ao fechar + log em CSV)
# =========================
def monitor_open_trades(open_trades: dict, poll_interval: int = 5):
    print("[MONITOR] monitor_open_trades iniciado (poll_interval=", poll_interval, "s)")
    try:
        while True:
            for sym, info in list(open_trades.items()):
                try:
                    order_list_id = None
                    buy_price = None
                    qty = None
                    buy_ts_ms = None  # para filtrar allOrders por tempo
                    # ===== extrai dados da BUY / qty média
                    if isinstance(info, dict):
                        buy = info.get("buy", {}) or {}
                        buy_ts_ms = buy.get("transactTime")  # ms
                        fills = buy.get("fills", []) or []
                        if fills:
                            qty = sum(float(f.get("qty", 0)) for f in fills)
                            if qty > 0:
                                num = sum(float(f["qty"]) * float(f["price"]) for f in fills)
                                buy_price = num / qty
                        else:
                            qty = float(buy.get("executedQty", 0) or 0.0)
                            cqq = float(buy.get("cummulativeQuoteQty", 0) or 0.0)
                            buy_price = (cqq / qty) if qty > 0 else float(buy.get("price") or 0.0)
                        # OCO vinculado
                        oco = info.get("oco")
                        if isinstance(oco, dict):
                            order_list_id = oco.get("orderListId")
                    # ===== 1) Caminho OCO
                    if order_list_id:
                        try:
                            oco_res = get_oco_list(orderListId=int(order_list_id))
                            lstatus = (oco_res.get("listStatusType") or oco_res.get("listOrderStatus") or "").upper()
                            # ===== Detecta OCO "zumbi" (versão conservadora) =====
                            legs = oco_res.get("orders", []) or []
                            leg_ids = [int(x.get("orderId")) for x in legs if x.get("orderId") is not None]
                            leg_detail = []
                            for oid_ in leg_ids:
                                try:
                                    leg_detail.append(get_order(sym, oid_))
                                except Exception:
                                    pass
                            stop_leg = next((x for x in leg_detail if x.get("type") == "STOP_LOSS_LIMIT" and x.get("side") == "SELL"), None)
                            tp_leg   = next((x for x in leg_detail if x.get("type") in ("LIMIT_MAKER","LIMIT") and x.get("side") == "SELL"), None)
                            is_exec = (str(oco_res.get("listOrderStatus","")).upper() == "EXECUTING")
                            if is_exec and stop_leg and tp_leg:
                                # idade mínima para evitar falso-positivo logo após criação
                                try:
                                    created_ms = int(stop_leg.get("time") or 0)
                                except Exception:
                                    created_ms = 0
                                too_young = (created_ms > 0) and ((int(time.time()*1000) - created_ms) < ZOMBIE_MIN_AGE_S*1000)
                                zombie = False
                                reason = ""
                                if not too_young:
                                    stop_bad = str(stop_leg.get("status","")).upper() in ("EXPIRED","CANCELED","REJECTED")
                                    tp_open  = str(tp_leg.get("status","")).upper() in ("NEW","PARTIALLY_FILLED")
                                    if stop_bad and tp_open:
                                        zombie = True
                                        reason = f"STOP {stop_leg.get('status')} com TP aberto"
                                    else:
                                        info_s = get_symbol_info(sym)
                                        tick   = float(info_s["tickSize"])
                                        below_margin = (ZOMBIE_TICKS_MARGIN * tick) if tick > 0 else 0.0
                                        try:
                                            sp = float(stop_leg.get("stopPrice") or 0.0)
                                        except Exception:
                                            sp = 0.0
                                        if sp > 0:
                                            ok_confirm = 0
                                            for _ in range(ZOMBIE_CONFIRM_TRIES):
                                                try:
                                                    leg_refresh = get_order(sym, int(stop_leg.get("orderId")))
                                                    if leg_refresh.get("isWorking") is True or str(leg_refresh.get("status","")).upper() in ("FILLED","CANCELED","EXPIRED","REJECTED"):
                                                        ok_confirm = 0
                                                        break
                                                except Exception:
                                                    pass
                                                t0 = time.time()
                                                good_window = True
                                                while time.time() - t0 < ZOMBIE_GRACE_S:
                                                    try:
                                                        last2 = float(_request_public("/api/v3/ticker/price", params={"symbol": sym}).get("price") or 0.0)
                                                    except Exception:
                                                        last2 = 0.0
                                                    if not (last2 > 0 and last2 <= (sp - below_margin)):
                                                        good_window = False
                                                        break
                                                    time.sleep(0.5)
                                                if good_window:
                                                    ok_confirm += 1
                                                time.sleep(ZOMBIE_CONFIRM_INTERVAL)
                                            if ok_confirm >= max(1, ZOMBIE_CONFIRM_TRIES // 2):
                                                zombie = True
                                                reason = f"last abaixo do stop por {ZOMBIE_GRACE_S}s (x{ok_confirm})"
                                if zombie:
                                    print(f"[OCO-ZUMBI] {sym}: zumbi detectado ({reason}). Vou limpar ordens; sem sell automático agora.")
                                    try:
                                        cancel_oco(sym, int(order_list_id))
                                    except Exception as e:
                                        print(f"[OCO-ZUMBI] erro cancelando OCO {order_list_id}: {e}")
                                    try:
                                        cancel_remaining_sells(sym)
                                    except Exception as e:
                                        print(f"[OCO-ZUMBI] erro limpando SELLs remanescentes: {e}")
                                    try:
                                        tg_send(f"🧹 *OCO zumbi limpo* {sym}\nMotivo: {reason}\nApenas limpei as ordens. Sem sell de posição.")
                                    except Exception:
                                        pass
                                    open_trades.pop(sym, None)
                                    # segue para o próximo símbolo
                                    continue
                            # ===== Se o OCO foi concluído, computa PnL e remove =====
                            if lstatus in ("EXECUTED", "ALL_DONE"):
                                order_reports = oco_res.get("orderReports", []) or []
                                sell_filled = next(
                                    (r for r in order_reports if r.get("side") == "SELL" and r.get("status") == "FILLED"),
                                    None
                                )
                                if sell_filled:
                                    try:
                                        cancel_remaining_sells(sym)
                                    except Exception as _e:
                                        print(f"[MONITOR] erro limpando SELLs remanescentes de {sym}: {_e}")
                                    exit_price = 0.0
                                    try:
                                        exq = float(sell_filled.get("executedQty") or 0)
                                        cqq = float(sell_filled.get("cummulativeQuoteQty") or 0)
                                        if exq > 0 and cqq > 0:
                                            exit_price = cqq / exq
                                    except Exception:
                                        pass
                                    if not exit_price:
                                        try:
                                            exit_price = float(sell_filled.get("price") or 0)
                                        except Exception:
                                            exit_price = 0.0
                                    if (qty or 0) <= 0:
                                        try:
                                            qty = float(sell_filled.get("executedQty") or 0)
                                        except Exception:
                                            pass
                                    if (buy_price or 0) > 0 and (exit_price or 0) > 0 and (qty or 0) > 0:
                                        pnl_usdt = (exit_price - buy_price) * qty
                                        pnl_pct = (exit_price - buy_price) / buy_price * 100.0
                                        reason = "TP" if pnl_usdt > 0 else "SL"
                                        emoji = "🔥" if pnl_usdt > 0 else "🛑" if pnl_usdt < 0 else "⚪"
                                        _log_trade(sym, buy_price, exit_price, qty, reason)
                                        msg = (
                                            f"{emoji} *OCO finalizado!* {emoji}\n"
                                            f"📌 *Symbol:* `{sym}`\n"
                                            f"🛒 *Compra:* `{buy_price:.6f}` x `{qty}`\n"
                                            f"💵 *Venda:* `{exit_price:.6f}`\n"
                                            f"🎯 *PnL:* `{pnl_usdt:+.6f} USDT` (`{pnl_pct:+.2f}%`)\n"
                                            f"🔗 https://www.binance.com/en/trade/{sym.upper()}?type=spot"
                                        )
                                        try:
                                            tg_send(msg)
                                        except Exception:
                                            pass
                                    print(f"[MONITOR] {sym} OCO {order_list_id} finalizada -> removendo open_trades")
                                    open_trades.pop(sym, None)
                                    continue
                        except Exception as e:
                            print(f"[MONITOR] Erro checando OCO {order_list_id} {sym}: {e}")
                    # ===== 2) Caminho TP/STOP fora de OCO — checar IDs das ordens SELL retornadas
                    tp_id = (info.get("tp") or {}).get("orderId")
                    stop_id = (info.get("stop") or {}).get("orderId")
                    for oid in [tp_id, stop_id]:
                        if not oid:
                            continue
                        try:
                            o = get_order(sym, int(oid))
                        except Exception:
                            continue
                        if o and o.get("status") == "FILLED" and o.get("side") == "SELL":
                            exq = float(o.get("executedQty") or 0)
                            cqq = float(o.get("cummulativeQuoteQty") or 0)
                            exit_price = (cqq / exq) if exq > 0 and cqq > 0 else float(o.get("price") or 0)
                            if (qty or 0) <= 0:
                                qty = exq
                            if (buy_price or 0) > 0 and (exit_price or 0) > 0 and (qty or 0) > 0:
                                pnl_usdt = (exit_price - buy_price) * qty
                                pnl_pct = (exit_price - buy_price) / buy_price * 100.0
                                reason = "TP" if pnl_usdt > 0 else "SL"
                                emoji = "🔥" if pnl_usdt > 0 else "🛑" if pnl_usdt < 0 else "⚪"
                                _log_trade(sym, buy_price, exit_price, qty, reason)
                                msg = (
                                    f"{emoji} *Trade finalizado!* {emoji}\n"
                                    f"📌 *Symbol:* `{sym}`\n"
                                    f"🛒 *Compra:* `{buy_price:.6f}` x `{qty}`\n"
                                    f"💵 *Venda:* `{exit_price:.6f}`\n"
                                    f"🎯 *PnL:* `{pnl_usdt:+.6f} USDT` (`{pnl_pct:+.2f}%`)\n"
                                    f"🔗 https://www.binance.com/en/trade/{sym.upper()}?type=spot"
                                )
                                try:
                                    cancel_remaining_sells(sym)
                                    tg_send(msg)
                                except Exception as _e:
                                    print(f"[MONITOR] erro limpando SELLs remanescentes de {sym}: {_e}")
                            print(f"[MONITOR] {sym} SELL #{oid} FILLED -> removendo open_trades")
                            open_trades.pop(sym, None)
                            break  # sai do loop dos IDs
                    if sym not in open_trades:
                        continue  # já removido; vai para o próximo símbolo
                    # ===== 3) Se não achar nada acima, checa open orders
                    try:
                        opens = get_open_orders(sym)
                        if not opens:
                            # ===== 3a) Fallback: procurar SELL FILLED recente no histórico
                            try:
                                hist = get_all_orders(sym, limit=50) or []
                            except Exception:
                                hist = []
                            def _ts_ok(o):
                                try:
                                    tms = int(o.get("time", 0) or 0)
                                except Exception:
                                    tms = 0
                                return (buy_ts_ms is None) or (tms >= int(buy_ts_ms or 0))
                            sell_filled = None
                            for o in reversed(hist):
                                if o.get("side") == "SELL" and o.get("status") == "FILLED" and _ts_ok(o):
                                    sell_filled = o
                                    break
                            if sell_filled and (buy_price or 0) > 0:
                                exq = float(sell_filled.get("executedQty") or 0)
                                cqq = float(sell_filled.get("cummulativeQuoteQty") or 0)
                                exit_price = (cqq / exq) if exq > 0 and cqq > 0 else float(sell_filled.get("price") or 0)
                                if (qty or 0) <= 0:
                                    qty = exq
                                if (exit_price or 0) > 0 and (qty or 0) > 0:
                                    pnl_usdt = (exit_price - buy_price) * qty
                                    pnl_pct = (exit_price - buy_price) / buy_price * 100.0
                                    reason = "TP" if pnl_usdt > 0 else "SL"
                                    emoji = "🔥" if pnl_usdt > 0 else "🛑" if pnl_usdt < 0 else "⚪"
                                    _log_trade(sym, buy_price, exit_price, qty, reason)
                                    msg = (
                                        f"{emoji} *Trade finalizado (histórico)* {emoji}\n"
                                        f"📌 *Symbol:* `{sym}`\n"
                                        f"🛒 *Compra:* `{buy_price:.6f}` x `{qty}`\n"
                                        f"💵 *Venda:* `{exit_price:.6f}`\n"
                                        f"🎯 *PnL:* `{pnl_usdt:+.6f} USDT` (`{pnl_pct:+.2f}%`)\n"
                                        f"🔗 https://www.binance.com/en/trade/{sym.upper()}?type=spot"
                                    )
                                    try:
                                        cancel_remaining_sells(sym)
                                        tg_send(msg)
                                    except Exception as _e:
                                        print(f"[MONITOR] erro limpando SELLs remanescentes de {sym}: {_e}")
                            print(f"[MONITOR] {sym} não tem open orders -> removendo open_trades")
                            open_trades.pop(sym, None)
                            continue
                    except Exception as e:
                        print(f"[MONITOR] Erro checando open_orders {sym}: {e}")
                except Exception as e:
                    print("[MONITOR] Erro checando", sym, ":", e)
            time.sleep(poll_interval)
    except KeyboardInterrupt:
        print("[MONITOR] monitor interrompido pelo usuário")
# =========================
# Faxina de órfãs
# =========================
def cleanup_orphan_stops(symbol: str):
    try:
        opens = get_open_orders(symbol)
        acct = get_account()
        base = symbol.replace("USDT", "")
        bal = next((b for b in acct.get("balances", []) if b.get("asset") == base), None) or {}
        locked = float(bal.get("locked", 0) or 0)
        for o in opens:
            if o.get("side") == "SELL" and o.get("status") in ("NEW", "WORKING"):
                need = float(o.get("origQty", 0)) - float(o.get("executedQty", 0))
                if need > 0 and locked >= need:
                    try:
                        _request_signed("DELETE", "/api/v3/order", {"symbol": symbol, "orderId": int(o["orderId"])})
                        tg_send(f"🧹 Cancelada ordem órfã {symbol} #{o['orderId']} qty={need}")
                    except Exception as e:
                        print("[CLEANUP] erro cancelando", o.get("orderId"), e)
    except Exception as e:
        print("[CLEANUP] erro:", e)
# =========================
# Cancelar OCO (ambos os legs)
# =========================
def cancel_oco(symbol: str, order_list_id: int):
    try:
        return _request_signed("DELETE", "/api/v3/orderList", {
            "symbol": symbol,
            "orderListId": int(order_list_id)
        })
    except Exception as e:
        print(f"[CANCEL-OCO] erro cancelando OCO {symbol} {order_list_id}: {e}")
        return {"error": True, "message": str(e)}
# =========================
# Resumo diário (Telegram) a partir do CSV
# =========================
def _parse_csv_rows(path: str) -> list[dict]:
    rows = []
    try:
        import csv
        with open(path, newline="") as f:
            r = csv.DictReader(f)
            for row in r:
                rows.append(row)
    except FileNotFoundError:
        pass
    except Exception as e:
        print("[DAILY] erro lendo CSV:", e)
    return rows
def _to_tz(dt_utc: datetime.datetime, tz_name: str) -> datetime.datetime:
    # timestamp_utc no CSV está em ISO, UTC. Convertemos para timezone local.
    if dt_utc.tzinfo is None:
        dt_utc = dt_utc.replace(tzinfo=datetime.timezone.utc)
    if pytz and tz_name:
        try:
            tz = pytz.timezone(tz_name)
            return dt_utc.astimezone(tz)
        except Exception:
            return dt_utc.astimezone(datetime.timezone.utc)
    return dt_utc.astimezone(datetime.timezone.utc)
def _today_bounds(tz_name: str) -> tuple[datetime.datetime, datetime.datetime]:
    now = datetime.datetime.now(datetime.timezone.utc)
    local_now = _to_tz(now, tz_name)
    start_local = local_now.replace(hour=0, minute=0, second=0, microsecond=0)
    end_local = start_local + datetime.timedelta(days=1)
    # voltamos a UTC para comparar com timestamps do CSV (que são UTC)
    start_utc = start_local.astimezone(datetime.timezone.utc)
    end_utc = end_local.astimezone(datetime.timezone.utc)
    return start_utc, end_utc
def _summarize(rows: Iterable[dict], start_utc: datetime.datetime, end_utc: datetime.datetime) -> dict:
    items = []
    for r in rows:
        try:
            ts = datetime.datetime.fromisoformat(r.get("timestamp_utc", ""))
            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=datetime.timezone.utc)
        except Exception:
            continue
        if not (start_utc <= ts < end_utc):
            continue
        try:
            pnl_pct = float(r.get("pnl_pct", 0))
            pnl_usdt = float(r.get("pnl_usdt", 0))
            items.append({
                "symbol": r.get("symbol","?"),
                "pnl_pct": pnl_pct,
                "pnl_usdt": pnl_usdt,
                "buy_price": float(r.get("buy_price", 0)),
                "exit_price": float(r.get("exit_price", 0)),
                "qty": float(r.get("qty", 0)),
                "reason": r.get("reason",""),
                "ts": ts,
            })
        except Exception:
            continue
    total = len(items)
    wins = sum(1 for x in items if x["pnl_usdt"] > 0)
    win_rate = (wins/total*100) if total else 0.0
    pnl_sum = sum(x["pnl_usdt"] for x in items)
    avg_pct = (sum(x["pnl_pct"] for x in items)/total*100) if total else 0.0
    # top 3 ganhos/perdas por USDT
    top_win = sorted(items, key=lambda x: x["pnl_usdt"], reverse=True)[:3]
    top_lose = sorted(items, key=lambda x: x["pnl_usdt"])[:3]
    return {
        "count": total,
        "win_rate": win_rate,
        "pnl_sum": pnl_sum,
        "avg_pct": avg_pct,
        "top_win": top_win,
        "top_lose": top_lose,
    }
def _fmt_top(label: str, arr: list[dict]) -> str:
    if not arr:
        return f"*{label}:* —"
    lines = [f"*{label}:*"]
    for x in arr:
        lines.append(f"• `{x['symbol']}`  PnL `{x['pnl_usdt']:+.4f}`  ({x['pnl_pct']*100:+.2f}%)")
    return "\n".join(lines)
async def daily_summary_loop(tz_name: str = os.getenv("TZ","UTC"), send_at: str = "21:00"):
    """
    Envia 1 resumo diário no horário local `send_at` (HH:MM).
    Use: asyncio.create_task(daily_summary_loop("America/Bahia","21:00"))
    """
    import asyncio
    # calcula próximo disparo
    def _next_run(now_local: datetime.datetime) -> datetime.datetime:
        hh, mm = map(int, send_at.split(":"))
        target = now_local.replace(hour=hh, minute=mm, second=0, microsecond=0)
        if now_local >= target:
            target += datetime.timedelta(days=1)
        return target
    while True:
        try:
            now_utc = datetime.datetime.now(datetime.timezone.utc)
            now_local = _to_tz(now_utc, tz_name)
            nxt = _next_run(now_local)
            wait_s = (nxt - now_local).total_seconds()
            if wait_s > 0:
                await asyncio.sleep(wait_s)
            # Quando der a hora, computa "hoje" (local)
            start_utc, end_utc = _today_bounds(tz_name)
            rows = _parse_csv_rows(LOG_PATH)
            summary = _summarize(rows, start_utc, end_utc)
            date_str = _to_tz(start_utc, tz_name).strftime("%Y-%m-%d")
            msg = (
                f"📊 *Resumo diário* — `{date_str}`\n"
                f"• Trades: *{summary['count']}*\n"
                f"• Win rate: *{summary['win_rate']:.2f}%*\n"
                f"• PnL total: *{summary['pnl_sum']:+.6f} USDT*\n"
                f"• PnL médio: *{summary['avg_pct']:+.2f}%*\n\n"
                f"{_fmt_top('TOP ganhos', summary['top_win'])}\n\n"
                f"{_fmt_top('TOP perdas', summary['top_lose'])}"
            )
            try:
                tg_send(msg)
            except Exception as e:
                print("[DAILY] erro enviando Telegram:", e)
            # espera ~60s para evitar duplo envio em relógios oscilantes
            await asyncio.sleep(60)
        except Exception as e:
            print("[DAILY] loop erro:", e)
            # em caso de erro, tenta novamente em 60s
            await asyncio.sleep(60)
# util para disparo manual (sincrono) — bom para testes
def send_daily_summary_now(tz_name: str = os.getenv("TZ","UTC")):
    start_utc, end_utc = _today_bounds(tz_name)
    rows = _parse_csv_rows(LOG_PATH)
    summary = _summarize(rows, start_utc, end_utc)
    date_str = _to_tz(start_utc, tz_name).strftime("%Y-%m-%d")
    msg = (
        f"📊 *Resumo diário (manual)* — `{date_str}`\n"
        f"• Trades: *{summary['count']}*\n"
        f"• Win rate: *{summary['win_rate']:.2f}%*\n"
        f"• PnL total: *{summary['pnl_sum']:+.6f} USDT*\n"
        f"• PnL médio: *{summary['avg_pct']:+.2f}%*\n\n"
        f"{_fmt_top('TOP ganhos', summary['top_win'])}\n\n"
        f"{_fmt_top('TOP perdas', summary['top_lose'])}"
    )
    try:
        tg_send(msg)
    except Exception as e:
        print("[DAILY] erro enviando Telegram:", e)
# =========================
# CLI
# =========================
if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="trader.py util (account/orders checker)")
    parser.add_argument("--account", action="store_true", help="Mostrar saldo da conta")
    parser.add_argument("--open-orders", nargs="?", const="ALL", help="Listar ordens abertas (opcional: SYMBOL ou ALL)")
    parser.add_argument("--all-orders", nargs=1, help="Listar histórico de ordens para SYMBOL")
    parser.add_argument("--oco", nargs=1, help="Consultar OCO por orderListId")
    parser.add_argument("--cancel-order", nargs=2, metavar=("SYMBOL", "ORDER_ID"), help="Cancelar ordem por orderId")
    parser.add_argument("--cleanup-orphans", nargs=1, metavar=("SYMBOL",), help="Cancelar ordens órfãs do SYMBOL (heurística)")
    parser.add_argument("--cancel-sells", nargs=1, metavar=("SYMBOL",), help="Cancelar TODAS as SELL pendentes do SYMBOL")
    parser.add_argument("--cancel-oco", nargs=2, metavar=("SYMBOL", "ORDER_LIST_ID"), help="Cancelar OCO por orderListId")
    parser.add_argument("--price", nargs=1, help="Mostrar preço atual do símbolo (last/bid/ask)")
    args = parser.parse_args()
    if args.account:
        print("Account:", get_account())
    if args.open_orders is not None:
        sym = None if args.open_orders == "ALL" else args.open_orders
        print("Open orders:", get_open_orders(sym))
    if args.all_orders:
        sym = args.all_orders[0]
        print("All orders:", get_all_orders(sym))
    if args.oco:
        print("OCO:", get_oco_list(orderListId=int(args.oco[0])))
    if args.cancel_order:
        sym, oid = args.cancel_order
        print("Cancel result:", cancel_order(sym, int(oid)))
    if args.cleanup_orphans:
        sym = args.cleanup_orphans[0]
        cleanup_orphan_stops(sym)
        print("Done cleanup.")
    if args.cancel_sells:
        sym = args.cancel_sells[0]
        cancel_remaining_sells(sym)
        print(f"Canceled remaining SELLs for {sym}.")
    if args.cancel_oco:
        sym, olid = args.cancel_oco
        print("Cancel OCO result:", cancel_oco(sym, int(olid)))
    if args.price:
        sym = args.price[0]
        p = get_price(sym)
        print("Price:", p)
def _request_public_mkt(path: str, params: dict = None):
    # sempre usa mainnet para dados públicos (klines/24h), pois testnet raramente tem histórico
    url = PUBLIC_BASE + path
    r = requests.get(url, params=params or {}, timeout=20)
    r.raise_for_status()
    return r.json()
