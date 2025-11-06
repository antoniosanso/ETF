#!/usr/bin/env python3
# scripts/fetch_investing.py
import os, sys, re, time, random
import datetime as dt
from dataclasses import dataclass
from typing import Optional, Tuple
import requests
from bs4 import BeautifulSoup
import pandas as pd

BASE = "https://it.investing.com"

S = requests.Session()
S.headers.update({
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept-Language": "it-IT,it;q=0.9,en-US;q=0.8,en;q=0.7",
})

def jitter(a=0.7, b=1.8): time.sleep(random.uniform(a,b))

@dataclass
class Instrument:
    name: str
    ticker: str
    url: str
    pair_id: str
    sml_id: Optional[str]
    currency: Optional[str]
    sector: Optional[str]

def find_first_etf_link(html: str) -> Optional[str]:
    """Robustly find a /etfs/ link from the search page (new + legacy DOM)."""
    soup = BeautifulSoup(html, "html.parser")
    # 1) New search rows carry data-url and data-pair-id
    for row in soup.select('[data-type="etfs"], .js-search-row'):
        url = row.get('data-url') or ""
        if url.startswith("/etfs/"):
            return BASE + url
        a = row.find('a', href=True)
        if a and a['href'].startswith('/etfs/'):
            return BASE + a['href']
    # 2) Fallback: any /etfs/ link on page
    a = soup.select_one('a[href^="/etfs/"]')
    if a: return BASE + a['href']
    return None

def extract_pair_meta(html: str) -> Tuple[Optional[str], Optional[str], Optional[str], Optional[str]]:
    pair = None; sml=None; curr=None; sect=None
    m = re.search(r"pairId\\s*[:=]\\s*([0-9]+)", html)
    if m: pair = m.group(1)
    m2 = re.search(r'name="smlID"\\s+value="([0-9]+)"', html)
    if m2: sml = m2.group(1)

    soup = BeautifulSoup(html, "html.parser")

    # Currency / Sector scanning
    for li in soup.select("li"):
        t = li.get_text(" ", strip=True)
        low = t.lower()
        if "valuta" in low and ":" in t:
            curr = t.split(":")[-1].strip()
        if any(k in low for k in ["categoria","settore","tipo"]) and ":" in t:
            sect = t.split(":")[-1].strip()

    # Breadcrumb as sector fallback
    bc = [b.get_text(strip=True) for b in soup.select("div.breadcrumb a")]
    if bc and not sect and len(bc) >= 3:
        sect = " / ".join(bc[1:3])

    return pair, sml, curr, sect

def get_instrument_by_ticker(ticker: str) -> Optional[Instrument]:
    # Try autocomplete JSON (fast path). If blocked, we fallback to HTML search.
    try:
        j = S.get(f"{BASE}/search/service/search?query={ticker}", timeout=15,
                  headers={"Accept":"application/json"})
        if j.status_code==200:
            js = j.json()
            # Prefer ETFs
            for item in js.get("quotes", []):
                if item.get("link", "").startswith("/etfs/"):
                    url = BASE + item["link"]
                    r = S.get(url, timeout=20)
                    if r.status_code==200:
                        pair, sml, curr, sect = extract_pair_meta(r.text)
                        nm = BeautifulSoup(r.text, "html.parser").select_one("h1")
                        name = nm.get_text(" ", strip=True) if nm else ticker
                        if not curr: curr="EUR"
                        return Instrument(name, ticker, url, pair or "", sml, curr, sect)
    except Exception:
        pass

    # Fallback: HTML search page
    r = S.get(f"{BASE}/search/?q={ticker}", timeout=25)
    if r.status_code != 200:
        return None
    jitter()
    url = find_first_etf_link(r.text)
    if not url:
        return None
    r2 = S.get(url, timeout=25)
    if r2.status_code != 200:
        return None
    pair, sml, curr, sect = extract_pair_meta(r2.text)
    name_el = BeautifulSoup(r2.text, "html.parser").select_one("h1")
    name = name_el.get_text(" ", strip=True) if name_el else ticker
    if not curr: curr="EUR"
    return Instrument(name, ticker, url, pair or "", sml, curr, sect)

def fetch_history(inst: Instrument, start: dt.date, end: dt.date) -> pd.DataFrame:
    url = f"{BASE}/instruments/HistoricalDataAjax"
    headers = {
        "User-Agent": S.headers["User-Agent"],
        "X-Requested-With": "XMLHttpRequest",
        "Referer": inst.url.rstrip('/') + "-historical-data",
        "Origin": BASE,
    }
    form = {
        "action": "historical_data",
        "pair_id": inst.pair_id,
        "smlID": inst.sml_id or "0",
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
        parsed = None
        for fmt in ("%d/%m/%Y","%d.%m.%Y","%b %d, %Y","%d %b %Y"):
            try:
                parsed = dt.datetime.strptime(date_s, fmt).date()
                break
            except: pass
        if parsed is None: continue
        out.append({"date": parsed.isoformat(), "close": close})
    return pd.DataFrame(out)

def main():
    in_csv = os.environ.get("ETF_CSV","ETF.csv")
    out_csv = os.environ.get("OUT_CSV","datasets/investing_history.csv")
    os.makedirs(os.path.dirname(out_csv), exist_ok=True)

    df = pd.read_csv(in_csv)
    all_df = []
    start = dt.date(2010,1,1)
    end = dt.date.today()

    for _, r in df.iterrows():
        name = str(r["name"]); ticker = str(r["ticker"]).strip()
        print(f"[INFO] {ticker}: searching…", flush=True)
        try:
            inst = get_instrument_by_ticker(ticker)
            if not inst or not inst.pair_id:
                print(f"[WARN] {ticker}: pair_id not found, skipping.")
                continue
            hist = fetch_history(inst, start, end)
            if hist.empty:
                print(f"[WARN] {ticker}: empty history.")
                continue
            hist["name"] = name
            hist["ticker"] = ticker
            hist["sector"] = inst.sector or ""
            hist["currency"] = inst.currency or "EUR"
            hist = hist[["name","ticker","date","close","sector","currency"]]
            all_df.append(hist)
            jitter()
        except Exception as e:
            print(f"[ERROR] {ticker}: {e}", flush=True)

    if not all_df:
        print("Error: no data fetched.", file=sys.stderr)
        sys.exit(1)

    out = pd.concat(all_df, ignore_index=True).sort_values(["ticker","date"])
    out.to_csv(out_csv, index=False, encoding="utf-8")
    print(f"[OK] saved {len(out)} rows -> {out_csv}")

if __name__ == "__main__":
    main()
