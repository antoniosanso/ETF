import requests
import pandas as pd
import time

BASE_URL = "https://www.justetf.com/api/etfs"

def fetch_page(page=1, page_size=100):
    params = {
        "page": page,
        "limit": page_size,
        "type": "ETF",
        "ucits": "true",
        "region": "Europe"
    }
    r = requests.get(BASE_URL, params=params, headers={"User-Agent": "Mozilla/5.0"})
    if r.status_code != 200:
        print(f"⚠️ HTTP {r.status_code} per pagina {page}")
        return []
    return r.json().get("data", [])

def fetch_all_etfs(max_pages=50):
    all_data = []
    for page in range(1, max_pages + 1):
        data = fetch_page(page)
        if not data:
            break
        all_data.extend(data)
        print(f"✅ Scaricata pagina {page} ({len(data)} ETF)")
        time.sleep(0.5)
    return all_data

def normalize(data):
    records = []
    for d in data:
        records.append({
            "provider": d.get("issuer", ""),
            "name": d.get("name", ""),
            "isin": d.get("isin", ""),
            "ticker_bi": d.get("symbol", ""),
            "venue": d.get("exchange", ""),
            "quote_ccy": d.get("currency", ""),
            "base_ccy": d.get("baseCurrency", ""),
            "eur_hedged": d.get("hedged", ""),
            "theme": d.get("category", ""),
            "source_url": f"https://www.justetf.com/en/etf-profile.html?isin={d.get('isin','')}"
        })
    df = pd.DataFrame(records)
    df.drop_duplicates(subset=["isin"], inplace=True)
    df.sort_values("provider", inplace=True)
    return df

def main():
    print("📡 Avvio download ETF UCITS da JustETF ...")
    data = fetch_all_etfs()
    if not data:
        print("❌ Nessun dato ricevuto.")
        return
    df = normalize(data)
    df.to_csv("ETF_list.csv", index=False)
    print(f"✅ Salvato ETF_list.csv con {len(df)} strumenti UCITS.")

if __name__ == "__main__":
    main()
