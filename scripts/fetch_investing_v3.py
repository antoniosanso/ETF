#!/usr/bin/env python3
# scripts/fetch_investing_v3.py
import os, sys, re, time, random
import datetime as dt
from typing import Optional, Tuple
import requests
from bs4 import BeautifulSoup
import pandas as pd

BASE = "https://it.investing.com"

S = requests.Session()
S.headers.update({
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept-Language": "it-IT,it;q=0.9,en-US;q=0.8,en;q=0.7",
})
def jitter(a=0.7, b=1.8): time.sleep(random.uniform(a,b))

def load_map(path: str) -> dict:
    if not os.path.exists(path): return {}
    m = {}
    df = pd.read_csv(path)
    for _, r in df.iterrows():
        t = str(r["ticker"]).strip()
        url = str(r["hist_url"]).strip() if not pd.isna(r["hist_url"]) else ""
        if t and url: m[t] = url
    return m

def html_search_hist_url(ticker: str) -> Optional[str]:
    r = S.get(f"{BASE}/search/?q={ticker}", timeout=25)
    if r.status_code != 200: return None
    soup = BeautifulSoup(r.text, "html.parser")
    a = soup.select_one('a[href^="/etfs/"]')
    if not a: return None
    overview = BASE + a["href"]
    if overview.endswith("-historical-data"): return overview
    return overview.rstrip("/") + "-historical-data"

def parse_pair_currency_sector(html: str) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    pair=None; curr=None; sect=None
    m = re.search(r"pairId\s*[:=]\s*([0-9]+)", html)
    if m: pair = m.group(1)
    soup = BeautifulSoup(html, "html.parser")
    for li in soup.select("li"):
        t = li.get_text(" ", strip=True)
        low = t.lower()
        if "valuta" in low and ":" in t: curr = t.split(":")[-1].strip()
        if any(k in low for k in ["categoria","settore","tipo"]) and ":" in t: sect = t.split(":")[-1].strip()
    if not sect:
        bc = [b.get_text(strip=True) for b in soup.select("div.breadcrumb a")]
        if len(bc) >= 3: sect = " / ".join(bc[1:3])
    return pair, curr, sect

def parse_hist_table(html: str) -> pd.DataFrame:
    soup = BeautifulSoup(html, "html.parser")
    rows = soup.select("table tbody tr")
    out = []
    for tr in rows:
        tds = [td.get_text(strip=True) for td in tr.select("td")]
        if len(tds) < 2: continue
        ds, cs = tds[0], tds[1]
        cs = cs.replace(".", "").replace(",", ".")
        try: close = float(cs)
        except: continue
        parsed = None
        for fmt in ("%d/%m/%Y","%d.%m.%Y","%b %d, %Y","%d %b %Y"):
            try:
                parsed = dt.datetime.strptime(ds, fmt).date()
                break
            except: pass
        if parsed is None: continue
        out.append({"date": parsed.isoformat(), "close": close})
    return pd.DataFrame(out)

def fetch_by_ajax(pair_id: str, start: dt.date, end: dt.date) -> pd.DataFrame:
    url = f"{BASE}/instruments/HistoricalDataAjax"
    headers = {
        "User-Agent": S.headers["User-Agent"],
        "X-Requested-With": "XMLHttpRequest",
        "Origin": BASE,
        "Referer": BASE,
    }
    form = {
        "action": "historical_data",
        "pair_id": pair_id,
        "smlID": "0",
        "header": "Historical Data",
        "st_date": start.strftime("%d/%m/%Y"),
        "end_date": end.strftime("%d/%m/%Y"),
        "interval_sec": "Daily",
        "sort_col": "date",
        "sort_ord": "DESC",
    }
    r = S.post(url, data=form, headers=headers, timeout=35)
    if r.status_code != 200:
        raise RuntimeError(f"HTTP {r.status_code}")
    return parse_hist_table(r.text)

def fetch_history(name: str, ticker: str, hist_url: Optional[str]) -> pd.DataFrame:
    url = hist_url or html_search_hist_url(ticker)
    if not url:
        print(f("[WARN] {ticker}: cannot resolve historical URL"))
        return pd.DataFrame()
    r = S.get(url, timeout=30)
    if r.status_code != 200:
        print(f"[WARN] {ticker}: fetch historical page failed {r.status_code}")
        return pd.DataFrame()
    pair, curr, sect = parse_pair_currency_sector(r.text)
    start = dt.date(2010,1,1); end = dt.date.today()
    df = pd.DataFrame()
    if pair:
        try:
            df = fetch_by_ajax(pair, start, end)
        except Exception as e:
            print(f"[WARN] {ticker}: ajax failed {e}; fallback to visible table")
    if df.empty:
        df = parse_hist_table(r.text)
    if df.empty:
        return df
    df["name"] = name; df["ticker"] = ticker
    df["sector"] = sect or ""; df["currency"] = curr or "EUR"
    return df[["name","ticker","date","close","sector","currency"]]

def main():
    etf_csv = os.environ.get("ETF_CSV","ETF.csv")
    map_csv = os.environ.get("MAP_CSV","investing_map.csv")
    out_csv = os.environ.get("OUT_CSV","datasets/investing_history.csv")
    os.makedirs(os.path.dirname(out_csv), exist_ok=True)
    etfs = pd.read_csv(etf_csv); mapping = load_map(map_csv)
    parts = []
    for _, r in etfs.iterrows():
        name = str(r["name"]); ticker = str(r["ticker"]).strip()
        print(f"[INFO] {ticker}: start")
        try:
            df = fetch_history(name, ticker, mapping.get(ticker))
            if df.empty:
                print(f"[WARN] {ticker}: no rows")
                continue
            parts.append(df); jitter()
        except Exception as e:
            print(f"[ERROR] {ticker}: {e}")
    if not parts:
        print("Error: no data fetched.", file=sys.stderr); sys.exit(1)
    out = pd.concat(parts, ignore_index=True).sort_values(["ticker","date"])
    out.to_csv(out_csv, index=False, encoding="utf-8")
    print(f"[OK] saved {len(out)} rows -> {out_csv}")

if __name__ == "__main__":
    main()
