import pandas as pd
import requests
from bs4 import BeautifulSoup
import re
import time

HEADERS = {'User-Agent': 'Mozilla/5.0'}

def safe_get(url):
    try:
        r = requests.get(url, headers=HEADERS, timeout=10)
        if r.status_code == 200:
            return r.text
    except Exception:
        pass
    return None

def search_borsa_italiana(isin):
    """Ricerca su Borsa Italiana"""
    url = f"https://www.borsaitaliana.it/borsa/etf/scheda/{isin}.html"
    html = safe_get(url)
    if not html:
        return None
    soup = BeautifulSoup(html, "lxml")
    text = soup.get_text(" ", strip=True)
    m = re.search(r"\b[A-Z0-9]{3,6}\.MI\b", text)
    if m:
        return {"ticker": m.group(0), "exchange": "Borsa Italiana", "fonte": "borsaitaliana.it"}
    return None

def search_justetf(isin):
    """Ricerca su JustETF"""
    url = f"https://www.justetf.com/en/etf-profile.html?isin={isin}"
    html = safe_get(url)
    if not html:
        return None
    soup = BeautifulSoup(html, "lxml")
    text = soup.get_text(" ", strip=True)
    ticker = ""
    exch = ""
    if "Ticker" in text:
        t = soup.find(string=re.compile("Ticker"))
        if t:
            nxt = t.find_next()
            ticker = nxt.text.strip() if nxt else ""
    if "Exchange" in text:
        e = soup.find(string=re.compile("Exchange"))
        if e:
            nxt = e.find_next()
            exch = nxt.text.strip() if nxt else ""
    if ticker:
        return {"ticker": ticker, "exchange": exch or "Unknown", "fonte": "justetf.com"}
    return None

def search_xetra(isin):
    """Ricerca su Xetra"""
    url = f"https://www.xetra.com/xetra-en/instruments/etf-finder/{isin}"
    html = safe_get(url)
    if not html:
        return None
    text = BeautifulSoup(html, "lxml").get_text(" ", strip=True)
    m = re.search(r"\b[A-Z0-9]{3,6}\.[A-Z]{2,3}\b", text)
    if m:
        return {"ticker": m.group(0), "exchange": "Xetra", "fonte": "xetra.com"}
    return None

def find_ticker(isin):
    """Ordine di ricerca e selezione"""
    sources = [search_borsa_italiana, search_justetf, search_xetra]
    candidates = []
    for fn in sources:
        result = fn(isin)
        if result:
            candidates.append(result)
        time.sleep(1)
    if not candidates:
        return {"ticker": "", "exchange": "", "fonte": "N/D"}
    # preferisci .MI o exchange italiano
    for c in candidates:
        if "MI" in c["ticker"] or "Italiana" in c["exchange"]:
            return c
    return candidates[0]

# --- MAIN ---
df = pd.read_csv("ETF_list prova.csv")
if "isin" not in df.columns:
    raise ValueError("Colonna 'isin' non trovata nel CSV")

results = []
for isin in df["isin"]:
    print(f"🔍 Ricerca per ISIN {isin}...")
    ticker_info = find_ticker(str(isin).strip())
    results.append(ticker_info)

df["ticker_bi"] = [r["ticker"] for r in results]
df["exchange"] = [r["exchange"] for r in results]
df["fonte_dati"] = [r["fonte"] for r in results]

df.to_csv("ETF_list_con_ticker.csv", index=False)
print("✅ File aggiornato: ETF_list_con_ticker.csv")
