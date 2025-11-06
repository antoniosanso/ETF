#!/usr/bin/env python3
# scripts/fetch_yahoo_v4.py
import os, sys, datetime as dt
import pandas as pd
import yfinance as yf

START_DATE = dt.date(2010,1,1)

def load_mapping(path:str)->dict:
    if not os.path.exists(path): return {}
    df = pd.read_csv(path)
    m = {}
    for _,r in df.iterrows():
        t = str(r["ticker"]).strip()
        y = str(r["yahoo"]).strip() if not pd.isna(r["yahoo"]) else ""
        sector = "" if pd.isna(r.get("sector","")) else str(r.get("sector",""))
        curr = "" if pd.isna(r.get("currency","")) else str(r.get("currency",""))
        if t:
            m[t] = {"yahoo": y, "sector": sector, "currency": curr}
    return m

def guess_yahoo_symbol(ticker: str) -> list:
    """
    Prova i suffissi Yahoo nell'ordine specificato da env YAHOO_SUFFIX_ORDER.
    Default: .MI, .AS, .PA, .DE, .IR, (nessun suffisso).
    """
    order_env = os.environ.get("YAHOO_SUFFIX_ORDER", ".MI,.AS,.PA,.DE,.IR,")
    suffixes = [s.strip() for s in order_env.split(",") if s is not None]

    candidates = []
    for suf in suffixes:
        if suf == "":   # caso 'nessun suffisso'
            candidates.append(ticker)
        else:
            candidates.append(f"{ticker}{suf}")
    # de-dup preservando l’ordine
    seen, uniq = set(), []
    for c in candidates:
        if c and c not in seen:
            uniq.append(c); seen.add(c)
    return uniq

def resolve_symbol(ticker:str, mapping:dict)->tuple[str, str, str]:
    # returns (symbol, sector, currency_override)
    info = mapping.get(ticker, {})
    if info and info.get("yahoo"):
        return info["yahoo"], info.get("sector",""), info.get("currency","")
    # try heuristics
    for sym in guess_yahoo_symbol(ticker):
        tk = yf.Ticker(sym)
        try:
            qi = tk.fast_info  # cheap probe
            if qi is not None and getattr(qi,"last_price", None) is not None:
                return sym, "", ""
        except Exception:
            pass
    return "", "", ""

def fetch_history_for_symbol(sym: str) -> pd.DataFrame:
    df = yf.download(
        sym,
        start=str(START_DATE),
        progress=False,
        interval="1d",
        auto_adjust=False,
        threads=False,
        group_by="column",
    )
    if df.empty:
        return pd.DataFrame()

    df = df.reset_index().rename(columns={"Date": "date"})

    close_series = None
    if "Adj Close" in df.columns:
        adj = df["Adj Close"]
        # se per qualche motivo è un DataFrame, prendo la prima colonna
        if isinstance(adj, pd.DataFrame):
            adj = adj.iloc[:, 0]
        if not pd.isna(adj).all():
            close_series = adj

    if close_series is None:
        close_series = df["Close"]
        if isinstance(close_series, pd.DataFrame):
            close_series = close_series.iloc[:, 0]

    df["close"] = pd.to_numeric(close_series, errors="coerce")
    df["date"] = pd.to_datetime(df["date"]).dt.date.astype(str)
    df = df.dropna(subset=["close"])

    return df[["date", "close"]]

def fetch_meta(sym: str) -> tuple[str, str]:
    """
    Ritorna (sector, currency). Per ETF/ETP Yahoo spesso non ha 'sector':
    uso 'category' o 'fundCategory'; fallback grezzo dal 'longName'.
    """
    sector, currency = "", ""
    tk = yf.Ticker(sym)

    # currency veloce
    try:
        fi = tk.fast_info
        currency = getattr(fi, "currency", "") or ""
    except Exception:
        pass

    # dettagli (lenti)
    try:
        info = tk.info or {}
        currency = info.get("currency") or currency or ""
        sector = info.get("category") or info.get("fundCategory") or ""
        if not sector:
            ln = (info.get("longName") or "").lower()
            keys = [
                "banks","oil","gold","copper","coffee",
                "euro stoxx 50","dax","emerging markets","bund","btp","ftse 100"
            ]
            for k in keys:
                if k in ln:
                    sector = k.title()
                    break
    except Exception:
        pass

    return sector, currency

def main():
    etf_csv = os.environ.get("ETF_CSV","ETF.csv")
    map_csv = os.environ.get("MAP_CSV","yahoo_map.csv")
    out_csv = os.environ.get("OUT_CSV","datasets/investing_history.csv")
    os.makedirs(os.path.dirname(out_csv), exist_ok=True)

    universe = pd.read_csv(etf_csv)
    mapping = load_mapping(map_csv)

    all_rows = []
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
        if not sector: sector = sector_hint
        if not currency: currency = curr_hint or "EUR"

        hist["name"] = name
        hist["ticker"] = ticker
        hist["sector"] = sector
        hist["currency"] = currency
        hist = hist[["name","ticker","date","close","sector","currency"]]
        all_rows.append(hist)

    if not all_rows:
        print("Error: no data fetched.", file=sys.stderr)
        sys.exit(1)

    out = pd.concat(all_rows, ignore_index=True).sort_values(["ticker","date"])
    out.to_csv(out_csv, index=False, encoding="utf-8")
    print(f"[OK] saved {len(out)} rows -> {out_csv}")

if __name__ == "__main__":
    main()
