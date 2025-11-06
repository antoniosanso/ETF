#!/usr/bin/env python3
# scripts/fetch_yahoo_v4.py
import os, sys, datetime as dt
import pandas as pd
import yfinance as yf

START_DATE = dt.date(2010, 1, 1)

def guess_order_from_env() -> list[str]:
    # Priorità ai simboli di Milano .MI (puoi cambiare via env)
    order_env = os.environ.get("YAHOO_SUFFIX_ORDER", ".MI,.AS,.PA,.DE,.IR,")
    suffixes = [s.strip() for s in order_env.split(",")]
    return suffixes

def load_mapping(path: str) -> dict:
    if not os.path.exists(path): return {}
    df = pd.read_csv(path)
    out = {}
    for _, r in df.iterrows():
        t = str(r["ticker"]).strip()
        out[t] = {
            "yahoo": ("" if pd.isna(r.get("yahoo","")) else str(r["yahoo"]).strip()),
            "sector": ("" if pd.isna(r.get("sector","")) else str(r["sector"]).strip()),
            "currency": ("" if pd.isna(r.get("currency","")) else str(r["currency"]).strip()),
        }
    return out

def resolve_symbol(ticker: str, mapping: dict) -> tuple[str, str, str]:
    """Ritorna (yahoo_symbol, sector_hint, currency_hint)."""
    if ticker in mapping and mapping[ticker]["yahoo"]:
        m = mapping[ticker]
        return m["yahoo"], m.get("sector",""), m.get("currency","")
    cands = []
    for suf in guess_order_from_env():
        cands.append(f"{ticker}{suf}" if suf != "" else ticker)
    # de-dup
    seen, uniq = set(), []
    for c in cands:
        if c and c not in seen:
            uniq.append(c); seen.add(c)
    for sym in uniq:
        try:
            tk = yf.Ticker(sym)
            fi = tk.fast_info
            if getattr(fi, "last_price", None) is not None:
                return sym, "", ""
        except Exception:
            pass
    return "", "", ""

def fetch_history_for_symbol(sym: str) -> pd.DataFrame:
    """
    Scarica lo storico 1D e crea SEMPRE una singola colonna 'close' se esiste
    almeno una colonna di prezzo utilizzabile.
    """
    try:
        df = yf.download(
            sym,
            start=str(START_DATE),
            interval="1d",
            auto_adjust=False,
            progress=False,
            threads=False,
            group_by="column",
        )
    except Exception as e:
        print(f"[WARN] {sym}: download failed ({e})")
        return pd.DataFrame()

    if df is None or df.empty:
        print(f"[WARN] {sym}: empty dataframe from Yahoo.")
        return pd.DataFrame()

    # Porta la data a colonna 'date'
    if "Date" in df.columns:
        df = df.rename(columns={"Date": "date"})
    else:
        df = df.reset_index().rename(columns={"Date": "date"})
    if "date" not in df.columns:
        df.insert(0, "date", df.index)

    # Trova una colonna prezzo utilizzabile
    close_series = None
    for cand in ["Adj Close", "Close", "close", "Price", "Last", "Value", "Close*"]:
        if cand in df.columns:
            s = df[cand]
            if isinstance(s, pd.DataFrame):
                if s.shape[1] == 0:
                    continue
                s = s.iloc[:, 0]
            if not pd.isna(s).all():
                close_series = s
                break

    if close_series is None:
        print(f"[WARN] {sym}: no usable price column (Adj Close/Close/Price...).")
        return pd.DataFrame()

    df["close"] = pd.to_numeric(close_series, errors="coerce")
    df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.date.astype(str)
    df = df[df["close"].notna()]           # ← nessun KeyError; filtra solo se 'close' esiste
    return df[["date", "close"]]

def fetch_meta(sym: str) -> tuple[str, str]:
    """Ritorna (sector, currency) da Yahoo con fallback."""
    sector, currency = "", ""
    tk = yf.Ticker(sym)
    try:
        fi = tk.fast_info
        currency = getattr(fi, "currency", "") or ""
    except Exception:
        pass
    try:
        info = tk.info or {}
        currency = info.get("currency") or currency or ""
        sector = info.get("category") or info.get("fundCategory") or ""
        if not sector:
            ln = (info.get("longName") or "").lower()
            for k in ["banks","oil","gold","copper","coffee","euro stoxx 50","dax","emerging markets","bund","btp","ftse 100"]:
                if k in ln:
                    sector = k.title(); break
    except Exception:
        pass
    return sector, currency

def main():
    etf_csv = os.environ.get("ETF_CSV", "ETF.csv")
    map_csv = os.environ.get("MAP_CSV", "yahoo_map.csv")
    out_csv = os.environ.get("OUT_CSV", "datasets/investing_history.csv")
    os.makedirs(os.path.dirname(out_csv), exist_ok=True)

    universe = pd.read_csv(etf_csv)
    mapping = load_mapping(map_csv)

    parts = []
    for _, r in universe.iterrows():
        name = str(r["name"])
        ticker = str(r["ticker"]).strip()
        print(f"[INFO] {ticker}: resolving Yahoo symbol...", flush=True)
        sym, sector_hint, curr_hint = resolve_symbol(ticker, mapping)
        if not sym:
            print(f"[WARN] {ticker}: no Yahoo symbol found. Skipping.")
            continue
        print(f"[INFO] {ticker}: using {sym}", flush=True)

        hist = fetch_history_for_symbol(sym)
        if hist.empty:
            print(f"[WARN] {ticker}: empty history for {sym}.")
            continue

        sector, currency = fetch_meta(sym)
        if not sector:   sector = sector_hint
        if not currency: currency = curr_hint or "EUR"

        hist["name"] = name
        hist["ticker"] = ticker
        hist["sector"] = sector
        hist["currency"] = currency
        parts.append(hist[["name","ticker","date","close","sector","currency"]])

    if not parts:
        print("Error: no data fetched.", file=sys.stderr)
        sys.exit(1)

    out = pd.concat(parts, ignore_index=True).sort_values(["ticker","date"])
    out.to_csv(out_csv, index=False, encoding="utf-8")
    print(f"[OK] saved {len(out)} rows -> {out_csv}")

if __name__ == "__main__":
    main()
