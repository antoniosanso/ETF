#!/usr/bin/env python3
# scripts/fetch_investing.py
"""
Scarica dati storici giornalieri da it.investing.com per i ticker elencati in ETF.csv,
cercando per TICKER (non per nome) e salvando i campi:
name, ticker, date, close, sector, currency

Finestra temporale: dal 2010-01-01 alla data odierna.
Nota: lo scraping di siti terzi può essere soggetto a cambiamenti.
"""

import os, sys, re, time, random
import datetime as dt
from dataclasses import dataclass
from typing import Optional, Tuple
import requests
from bs4 import BeautifulSoup
import pandas as pd

BASE = "https://it.investing.com"

SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/119.0.0.0 Safari/537.36",
    "Accept-Language": "it-IT,it;q=0.9,en-US;q=0.8,en;q=0.7",
})

def jitter(a=0.6, b=1.6):
    time.sleep(random.uniform(a, b))

@dataclass
class InstrumentInfo:
    name: str
    ticker: str
    url: str
    pair_id: str
    sml_id: Optional[str]
    currency: Optional[str]
    sector: Optional[str]

def find_first_etf_link(html: str) -> Optional[str]:
    soup = BeautifulSoup(html, "html.parser")
    for a in soup.select("a"):
        href = a.get("href") or ""
        if href.startswith("/etfs/"):
            return BASE + href
    return None

def extract_pair_and_meta(html: str) -> Tuple[Optional[str], Optional[str], Optional[str], Optional[str]]:
    pair_id = None; sml_id = None; currency = None; sector = None
    m = re.search(r"pairId\s*[:=]\s*([0-9]+)", html)
    if m: pair_id = m.group(1)
    m2 = re.search(r'name="smlID"\s+value="([0-9]+)"', html)
    if m2: sml_id = m2.group(1)
    soup = BeautifulSoup(html, "html.parser")

    # Try common detail rows
    # Currency
    for li in soup.select("li"):
        txt = li.get_text(" ", strip=True)
        low = txt.lower()
        if "valuta" in low and ":" in txt:
            currency = txt.split(":")[-1].strip()
        if any(k in low for k in ["categoria", "settore", "tipo"]):
            sector = txt.split(":")[-1].strip()

    # Breadcrumb fallback for sector
    bc = [b.get_text(strip=True) for b in soup.select("div.breadcrumb a")]
    if bc and not sector and len(bc) >= 3:
        sector = " / ".join(bc[1:3])

    return pair_id, sml_id, currency, sector

def get_instrument_from_ticker(ticker: str) -> Optional[InstrumentInfo]:
    url = f"{BASE}/search/?q={ticker}"
    r = SESSION.get(url, timeout=25)
    if r.status_code != 200: return None
    jitter()
    instrument_url = find_first_etf_link(r.text)
    if not instrument_url: return None

    r2 = SESSION.get(instrument_url, timeout=25)
    if r2.status_code != 200: return None
    pair_id, sml_id, currency, sector = extract_pair_and_meta(r2.text)
    if not pair_id:
        # Try historical-data page
        hd = instrument_url.rstrip("/") + "-historical-data"
        r3 = SESSION.get(hd, timeout=25, headers={"Referer": instrument_url})
        if r3.status_code == 200:
            p2, s2, c2, sec2 = extract_pair_and_meta(r3.text)
            pair_id = pair_id or p2
            sml_id = sml_id or s2
            currency = currency or c2
            sector = sector or sec2

    soup = BeautifulSoup(r2.text, "html.parser")
    h1 = soup.select_one("h1")
    name = h1.get_text(" ", strip=True) if h1 else ticker

    if not currency: currency = "EUR"
    return InstrumentInfo(name=name, ticker=ticker, url=instrument_url, pair_id=pair_id or "", sml_id=sml_id, currency=currency, sector=sector)

def fetch_historical(info: InstrumentInfo, start: dt.date, end: dt.date) -> pd.DataFrame:
    hist_url = f"{BASE}/instruments/HistoricalDataAjax"
    headers = {
        "User-Agent": SESSION.headers["User-Agent"],
        "X-Requested-With": "XMLHttpRequest",
        "Referer": info.url.rstrip("/") + "-historical-data",
        "Origin": BASE,
    }
    form = {
        "action": "historical_data",
        "pair_id": info.pair_id,
        "smlID": info.sml_id or "0",
        "header": "Historical Data",
        "st_date": start.strftime("%d/%m/%Y"),
        "end_date": end.strftime("%d/%m/%Y"),
        "interval_sec": "Daily",
        "sort_col": "date",
        "sort_ord": "DESC",
    }
    r = SESSION.post(hist_url, data=form, headers=headers, timeout=35)
    if r.status_code != 200:
        raise RuntimeError(f"HTTP {r.status_code} for {info.ticker}")
    soup = BeautifulSoup(r.text, "html.parser")
    rows = soup.select("table tbody tr")
    out = []
    for tr in rows:
        tds = [td.get_text(strip=True) for td in tr.select("td")]
        if len(tds) < 2: continue
        date_s, close_s = tds[0], tds[1]
        close_s = close_s.replace(".", "").replace(",", ".")
        try:
            close = float(close_s)
        except:
            continue
        # Parse multiple date formats
        parsed = None
        for fmt in ("%d/%m/%Y", "%d.%m.%Y", "%b %d, %Y", "%d %b %Y"):
            try:
                parsed = dt.datetime.strptime(date_s, fmt).date()
                break
            except:
                pass
        if not parsed: continue
        out.append({"date": parsed.isoformat(), "close": close})
    return pd.DataFrame(out)

def main():
    in_csv = os.environ.get("ETF_CSV", "ETF.csv")
    out_csv = os.environ.get("OUT_CSV", "datasets/investing_history.csv")
    os.makedirs(os.path.dirname(out_csv), exist_ok=True)

    tickers = pd.read_csv(in_csv)
    all_parts = []
    # Fixed start date as requested
    start = dt.date(2010, 1, 1)
    end = dt.date.today()

    for _, r in tickers.iterrows():
        name = str(r["name"])
        ticker = str(r["ticker"]).strip()
        print(f"[INFO] {ticker}: search on it.investing.com")
        try:
            info = get_instrument_from_ticker(ticker)
            if not info or not info.pair_id:
                print(f"[WARN] {ticker}: pair_id not found, skipping.")
                continue
            df = fetch_historical(info, start, end)
            if df.empty:
                print(f"[WARN] {ticker}: empty history, skipping.")
                continue
            df["name"] = name
            df["ticker"] = ticker
            df["sector"] = info.sector if info.sector else ""
            df["currency"] = info.currency if info.currency else "EUR"
            df = df[["name","ticker","date","close","sector","currency"]]
            all_parts.append(df)
            jitter(0.8, 1.8)
        except Exception as e:
            print(f"[ERROR] {ticker}: {e}", file=sys.stderr)
            continue

    if not all_parts:
        print("[ERROR] no data fetched.", file=sys.stderr)
        sys.exit(1)

    out_df = pd.concat(all_parts, ignore_index=True)
    out_df.sort_values(["ticker","date"], inplace=True)
    out_df.to_csv(out_csv, index=False, encoding="utf-8")
    print(f"[OK] Saved {len(out_df)} rows to {out_csv}")

if __name__ == "__main__":
    main()
