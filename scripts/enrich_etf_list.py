import pandas as pd
import requests
import yfinance as yf
from bs4 import BeautifulSoup
import time
import re

def safe_get(url, headers=None):
    try:
        r = requests.get(url, headers=headers or {'User-Agent': 'Mozilla/5.0'}, timeout=10)
        if r.status_code == 200:
            return r.text
    except Exception:
        pass
    return None

def search_yahoo(name):
    try:
        data = yf.Ticker(name)
        info = data.info
        if info and "symbol" in info:
            return {
                "Ticker": info.get("symbol", ""),
                "ISIN": info.get("isin", ""),
                "Fonte": "Yahoo Finance"
            }
    except Exception:
        pass
    return None

def search_justetf(name):
    url = f"https://www.justetf.com/en/find-etf.html?query={name.replace(' ', '+')}"
    html = safe_get(url)
    if not html:
        return None
    soup = BeautifulSoup(html, "lxml")
    link = soup.select_one("a.result-link")
    if not link:
        return None
    detail = safe_get("https://www.justetf.com" + link["href"])
    if not detail:
        return None
    s = BeautifulSoup(detail, "lxml")
    isin_tag = s.find(string=lambda x: "ISIN" in x)
    isin = isin_tag.find_next().text.strip() if isin_tag else ""
    ticker_tag = s.find(string=lambda x: "Ticker" in x)
    ticker = ticker_tag.find_next().text.strip() if ticker_tag else ""
    return {"Ticker": ticker, "ISIN": isin, "Fonte": "JustETF"}

def search_generic(name, site, pattern):
    url = pattern.format(query=name.replace(" ", "+"))
    html = safe_get(url)
    if not html:
        return None
    s = BeautifulSoup(html, "lxml")
    text = s.get_text(" ", strip=True)
    isin = re.search(r"\b[A-Z]{2}[A-Z0-9]{9}\d\b", text)
    ticker = re.search(r"\b[A-Z]{2,6}\.[A-Z]{1,3}\b", text)
    if isin or ticker:
        return {
            "Ticker": ticker.group(0) if ticker else "",
            "ISIN": isin.group(0) if isin else "",
            "Fonte": site
        }
    return None

SOURCES = [
    ("Yahoo", search_yahoo),
    ("JustETF", search_justetf),
    ("Borsa Italiana", lambda n: search_generic(n, "Borsa Italiana", "https://www.borsaitaliana.it/borsa/etf/lista.html?search={query}")),
    ("Euronext", lambda n: search_generic(n, "Euronext", "https://live.euronext.com/en/search_instruments/{query}")),
    ("Xetra", lambda n: search_generic(n, "Xetra", "https://www.xetra.com/xetra-en/instruments/etf-finder/{query}")),
    ("Investing", lambda n: search_generic(n, "Investing.com", "https://www.investing.com/search/?q={query}")),
    ("Morningstar", lambda n: search_generic(n, "Morningstar", "https://www.morningstar.it/it/funds/snapshot/snapshot.aspx?id={query}")),
]

df = pd.read_csv("ETF_list.csv")
if "Ticker" not in df.columns: df["Ticker"] = ""
if "ISIN" not in df.columns: df["ISIN"] = ""
if "Fonte" not in df.columns: df["Fonte"] = ""

for i, row in df.iterrows():
    name = str(row["Name"]).strip()
    if not name:
        continue

    print(f"🔍 Ricerca: {name}")
    record = None
    for site, fn in SOURCES:
        record = fn(name)
        if record:
            print(f" → trovato su {record['Fonte']}: {record['Ticker']} ({record['ISIN']})")
            df.at[i, "Ticker"] = record["Ticker"]
            df.at[i, "ISIN"] = record["ISIN"]
            df.at[i, "Fonte"] = record["Fonte"]
            break
        time.sleep(1)

    if not record:
        print(f" ⚠️  Nessun risultato per {name}")
        df.at[i, "Ticker"] = ""
        df.at[i, "ISIN"] = ""
        df.at[i, "Fonte"] = "N/D"

df.to_csv("ETF_list.csv", index=False)
print("✅ ETF_list.csv aggiornato con le informazioni reali.")