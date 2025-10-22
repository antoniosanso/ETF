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

def search_justetf(name):
    """Ricerca più tollerante su JustETF"""
    url = f"https://www.justetf.com/en/find-etf.html?query={name.replace(' ', '+')}"
    html = safe_get(url)
    if not html:
        return None
    soup = BeautifulSoup(html, "lxml")
    link = soup.select_one("a.result-link")
    if not link:
        # prova a cercare nella tabella
        first_link = soup.select_one("table a")
        if not first_link:
            return None
        link = first_link
    detail_url = "https://www.justetf.com" + link["href"]
    detail = safe_get(detail_url)
    if not detail:
        return None
    s = BeautifulSoup(detail, "lxml")
    isin = ""
    ticker = ""
    for el in s.find_all(text=True):
        if "ISIN" in el:
            next_el = s.find(string="ISIN").find_next()
            if next_el:
                isin = next_el.text.strip()
        if "Ticker" in el:
            next_el = s.find(string="Ticker").find_next()
            if next_el:
                ticker = next_el.text.strip()
    if not isin and not ticker:
        return None
    return {"ticker_bi": ticker, "isin": isin, "source_url": detail_url}

SOURCES = [("JustETF", search_justetf),]

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
