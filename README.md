# ETF History Pack v4 (Yahoo Finance)
- Fonte: Yahoo Finance via `yfinance`, affidabile su GitHub Actions.
- Periodo: dal 2010-01-01 ad oggi (giornaliero).
- Input: `ETF.csv` (name,ticker) + `yahoo_map.csv` (override simboli Yahoo, settore, valuta).
- Output: `datasets/investing_history.csv` con colonne: `name,ticker,date,close,sector,currency`.
- Heuristics: per i WisdomTree/Boost ETP quotati a Milano prova automaticamente il suffisso `.MI`; puoi forzare/aggiustare su `yahoo_map.csv`.
