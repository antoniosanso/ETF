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

def guess_yahoo_symbol(ticker:str)->list:
    # Heuristic: WisdomTree/Boost ETPs Italy usually have .MI
    cands = []
    if ticker and ticker[0].isdigit():
        cands.append(f"{ticker}.MI")
    # other exchanges fallbacks
    cands += [f"{ticker}.AS", f"{ticker}.PA", f"{ticker}.DE", f"{ticker}.IR", ticker]
    return cands

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

def fetch_history_for_symbol(sym:str)->pd.DataFrame:
    df = yf.download(sym, start=str(START_DATE), progress=False, interval="1d", auto_adjust=False)
    if df.empty:
        return pd.DataFrame()
    df = df.reset_index().rename(columns={"Date":"date","Adj Close":"close","Adj Close":"close"})
    if "Adj Close" in df.columns:
        df["close"] = df["Adj Close"]
    elif "Close" in df.columns:
        df["close"] = df["Close"]
    df["date"] = df["date"].dt.date.astype(str)
    return df[["date","close"]]

def fetch_meta(sym:str)->tuple[str,str]:
    try:
        tk = yf.Ticker(sym)
        info = tk.fast_info
        currency = getattr(info,"currency", None) or ""
        # sector can be empty for ETFs; try longBusinessSummary as fallback (not ideal)
        sector = ""
        try:
            meta = tk.info
            sector = meta.get("category") or meta.get("sector") or ""
            currency = (meta.get("currency") or currency) or ""
        except Exception:
            pass
        return sector, currency
    except Exception:
        return "", ""

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
