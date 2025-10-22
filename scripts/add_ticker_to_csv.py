import pandas as pd
import requests
from bs4 import BeautifulSoup
import re
import time
import os

HEADERS = {"User-Agent": "Mozilla/5.0"}

def safe_get(url):
    try:
        r = requests.get(url, headers=HEADERS, timeout=10)
        if r.status_code == 200:
            return r.text
    except Exception:
        pass
    return None

def search_borsa_italiana(isin):
    url = f"https://www.borsaitaliana.it/borsa/etf/scheda/{isin}.html"
    html = safe_get(url)
    if not html:
        return None
    soup = BeautifulSoup(html, "lxml")
    text = soup.get_text(" ", strip=True)
    m = re.search(r"\b[A-Z0-9]{3,6}\.MI\b", text)
    if m:
        return m.group(0)
    return None

def search_justetf(isin):
    url = f"https://www.justetf.com/en/etf-profile.html?isin={isin}"
    html = safe_get(url)
    if not html:
        return None
    soup = BeautifulSoup(html, "lxml")
    text = soup.get_text(" ", strip=True)
    t = soup.find(string=re.compile("Ticker"))
    if t:
        nxt = t.find_next()
        if nxt:
            return nxt.text.strip()
    return None

def search_xetra(isin):
    url = f"https://www.xetra.com/xetra-en/instruments/etf-finder/{isin}"
    html = safe_get(url)
    if not html:
        return None
    text = BeautifulSoup(html, "lxml").get_text(" ", strip=True)
    m = re.search(r"\b[A-Z0-9]{2,6}\.[A-Z]{2,3}\b", text)
    if m:
        return m.group(0)
    return None

def find_ticker(isin):
    for fn in (search_borsa_italiana, search_justetf, search_xetra):
        t = fn(isin)
        if t:
            return t
        time.sleep(1)
    return ""

def main():
    filename = "ETF_list prova v1.csv"
    if not os.path.exists(filename):
        raise FileNotFoundError(f"❌ File {filename} non trovato.")

    # rileva separatore automatico
    with open(filename, "r", encoding="utf-8") as f:
        first_line = f.readline()
        sep = ";" if ";" in first_line else ","

    df = pd.read_csv(filename, sep=sep)
    df.columns = [c.strip().lower() for c in df.columns]

    if "isin" not in df.columns:
        raise ValueError("❌ Nessuna colonna ISIN trovata nel file.")

    if "ticker" not in df.columns:
        df["Ticker"] = ""

    print(f"📊 {len(df)} ETF trovati nel file. Avvio ricerca ticker online...")

    for i, row in df.iterrows():
        isin = str(row["isin"]).strip()
        if not isin or not isinstance(isin, str):
            continue
        if df.at[i, "Ticker"]:
            continue  # già presente
        print(f"🔍 Ricerca per {isin} ...")
        ticker = find_ticker(isin)
        df.at[i, "Ticker"] = ticker or ""
        print(f" → {ticker if ticker else 'Nessun risultato'}")

    df.to_csv(filename, sep=sep, index=False)
    print(f"✅ Aggiornato {filename} con la colonna Ticker.")

if __name__ == "__main__":
    main()
