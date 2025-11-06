# Investing.com History Fetch (from 2010-01-01)

Questo pacchetto contiene:
- `ETF.csv` — elenco ETF (name, ticker)
- `scripts/fetch_investing.py` — scraper Investing.com Italia che cerca PER TICKER e scarica i prezzi storici giornalieri dal 2010-01-01 ad oggi, salvando:
  `name, ticker, date, close, sector, currency`
- `.github/workflows/investing_history.yml` — workflow GitHub Actions per eseguire lo script e committare `datasets/investing_history.csv`

## Avvertenze
- Il sito può cambiare markup o applicare limitazioni; lo script include backoff e fallback ma potrebbe saltare alcuni ticker se bloccato.
- Usa responsabilmente nel rispetto dei Termini del sito.
