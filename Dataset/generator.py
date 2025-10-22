import yfinance as yf
import pandas as pd
import requests
from datetime import datetime, timedelta
import json
import logging
import os
import time
import re

# ===== Setup logger =====
LOG_FILENAME = "stock_gdelt.log"
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILENAME, mode='w', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger()

# ===== Stock data fetch =====
def fetch_stock_data(ticker, company_name, start_date, end_date):
    logger.info(f"Fetching stock data for {company_name} ({ticker})...")
    
    stock_data = yf.download(ticker, start=start_date, end=end_date, auto_adjust=False)
    
    if isinstance(stock_data.columns, pd.MultiIndex):
        stock_data.columns = stock_data.columns.get_level_values(0)
    
    stock_data.reset_index(inplace=True)
    stock_data["Date"] = pd.to_datetime(stock_data["Date"]).dt.date
    stock_data["Daily_Return"] = stock_data["Adj Close"].pct_change()
    stock_data["EMA_7"] = stock_data["Adj Close"].ewm(span=7, adjust=False).mean()
    stock_data["EMA_21"] = stock_data["Adj Close"].ewm(span=21, adjust=False).mean()
    stock_data["Ticker"] = ticker
    stock_data["Company"] = company_name
    
    logger.info(f"Successfully fetched {len(stock_data)} records.")
    return stock_data


# ===== GDELT news fetch =====
def fetch_gdelt_finance_news(ticker, company_name, start_date, end_date, company_keywords=None, top_n=5):
    if company_keywords is None:
        company_keywords = [ticker, company_name]

    impact_keywords = [
        "earnings", "profit", "loss", "revenue", "guidance", "dividend",
        "ipo", "lawsuit", "merger", "acquisition", "partnership",
        "regulation", "fine", "recall", "data breach",
        "analyst upgrade", "analyst downgrade", "stock buyback"
    ]
    
    finance_keywords = [
        "finance", "market", "shares", "investment", "stock", "economy",
        "ipo", "trading", "fund", "earnings", "revenue", "profit",
        "dividend", "valuation", "forecast", "guidance"
    ]

    def is_stock_relevant(article):
        title = (article.get("title") or "").lower()
        summary = (article.get("seendoc") or article.get("snippet") or "").lower()
        text = title + " " + summary
        if not any(kw.lower() in text for kw in company_keywords):
            return False
        return any(kw.lower() in text for kw in impact_keywords) or "stock" in text or "shares" in text

    def score_article(article):
        title = article.get("title", "").lower()
        return sum(title.count(k.lower()) for k in finance_keywords)

    logger.info(f"Fetching finance-related news for {company_name} ({ticker})...")
    base_url = "https://api.gdeltproject.org/api/v2/doc/doc"
    all_results = []

    current_date = datetime.strptime(start_date, "%Y-%m-%d")
    end_date_dt = datetime.strptime(end_date, "%Y-%m-%d")

    while current_date <= end_date_dt:
        next_date = current_date + timedelta(days=1)

        # ✅ Clean company name for safe querying
        cleaned_company_name = re.sub(r'[^A-Za-z0-9\s]', '', company_name).strip()
        cleaned_company_name = " ".join([w for w in cleaned_company_name.split() if len(w) >= 3])
        logger.debug(f"Querying GDELT for '{cleaned_company_name}' on {current_date.date()}")

        params = {
            "query": cleaned_company_name,
            "mode": "ArtList",
            "maxrecords": "250",
            "format": "JSON",
            "startdatetime": current_date.strftime("%Y%m%d%H%M%S"),
            "enddatetime": next_date.strftime("%Y%m%d%H%M%S")
        }

        success = False
        retries = 3
        while not success and retries > 0:
            try:
                response = requests.get(base_url, params=params, timeout=15)
                if "Please limit requests" in response.text:
                    logger.warning(f"Rate limited on {current_date.date()}, retrying in 10s...")
                    time.sleep(10)
                    retries -= 1
                    continue

                try:
                    data = response.json()
                except json.JSONDecodeError:
                    logger.error(f"Non-JSON response on {current_date.date()}: {response.text[:200]}")
                    break

                articles = data.get("articles", [])
                eng_articles = [a for a in articles if a.get("language", "").lower() in ["en", "english"]]
                relevant_articles = [a for a in eng_articles if is_stock_relevant(a)]
                if not relevant_articles:
                    relevant_articles = [
                        a for a in eng_articles
                        if any(kw.lower() in (a.get("title", "") + " " + (a.get("snippet") or "")).lower()
                               for kw in company_keywords)
                    ]
                top_articles = sorted(relevant_articles, key=score_article, reverse=True)[:top_n]
                if top_articles:
                    headlines = " | ".join(art.get("title", "") for art in top_articles)
                    all_results.append({
                        "Date": current_date.date(),
                        "Headlines": headlines,
                        "Ticker": ticker,
                        "Company": company_name
                    })
                success = True

            except requests.RequestException as e:
                logger.error(f"Network error fetching {current_date.date()}: {e}")
                time.sleep(10)

        # Wait 10 seconds before next day's request to avoid rate-limit
        time.sleep(10)
        current_date = next_date

    df = pd.DataFrame(all_results, columns=["Date", "Headlines", "Ticker", "Company"])
    logger.info(f"Retrieved {len(df)} days of news for {company_name}.")
    return df


# ===== Main =====
if __name__ == "__main__":
    all_years_data = []
    TICKER = "V"
    COMPANY_NAME = "VISA Inc."
    START_YEAR = 2021
    END_YEAR = 2024

    def clean_keywords(keywords):
        cleaned = []
        for kw in keywords:
            kw = kw.replace(".", "").replace("&", "and")
            cleaned_kw = re.sub(r'[^A-Za-z0-9\s]', '', kw).strip()
            if any(len(word) >= 3 for word in cleaned_kw.split()):
                cleaned.append(cleaned_kw)
        return cleaned

    company_keywords = [
        "Visa", "Visa Inc", "V", "Visa Card", "Visa Credit", "Visa Debit",
        "Visa Payment", "VisaNet", "Visa Checkout", "Visa Direct",
        "Visa Secure", "Visa Financial Services", "Visa Transactions",
        "Visa Network", "Visa Digital", "Visa Token Service", "Visa Tap to Pay",
        "Visa Contactless", "Visa Merchant", "Visa Account", "Visa Commercial",
        "Visa B2B", "Visa Analytics", "Visa Risk Management", "Visa Innovations",
        "Visa Everywhere Initiative", "Al Kelly", "Ryan McInerney",
        "Visa Global", "Visa Partnerships"
    ]

    company_keywords = clean_keywords(company_keywords)
    OUTPUT_FILENAME = f"{TICKER}_stock_gdelt_final.csv"

    for year in range(START_YEAR, END_YEAR + 1):
        start_date = f"{year}-01-01"
        end_date = f"{year}-12-31"

        logger.info(f"\n===== Fetching data for {year} =====")
        stock_data = fetch_stock_data(TICKER, COMPANY_NAME, start_date, end_date)
        news_df = fetch_gdelt_finance_news(TICKER, COMPANY_NAME, start_date, end_date, company_keywords)

        logger.info("Merging stock and news data...")
        combined_df = pd.merge(stock_data, news_df, on=["Date", "Ticker", "Company"], how="left")

        if year == START_YEAR:
            combined_df.to_csv(OUTPUT_FILENAME, index=False)
        else:
            combined_df.to_csv(OUTPUT_FILENAME, mode='a', header=False, index=False)

        logger.info(f"Year {year} saved to '{OUTPUT_FILENAME}'")
        all_years_data.append(combined_df)

# import yfinance as yf
# import pandas as pd
# import requests
# from datetime import datetime, timedelta
# import json
# import logging
# import os
# import time
# import re

# # ===== Setup logger =====
# LOG_FILENAME = "stock_gdelt.log"
# logging.basicConfig(
#     level=logging.INFO,
#     format="%(asctime)s [%(levelname)s] %(message)s",
#     handlers=[
#         logging.FileHandler(LOG_FILENAME, mode='w', encoding='utf-8'),
#         logging.StreamHandler()
#     ]
# )
# logger = logging.getLogger()

# # ===== Helper: Safe JSON loader =====
# def safe_json_load(response_text, date_str):
#     """
#     Safely parse possibly truncated or malformed JSON responses.
#     Repairs or trims partial responses (e.g. Benzinga / Sina cases).
#     """
#     text = response_text.strip()
#     if not text.startswith("{") and not text.startswith("["):
#         logger.error(f"Non-JSON response on {date_str}: {text[:200]}")
#         return None

#     try:
#         return json.loads(text)
#     except json.JSONDecodeError as e:
#         logger.warning(f"Malformed JSON on {date_str}: {e}. Attempting repair...")

#         # Try Unicode fix
#         try:
#             fixed_text = text.encode('utf-8').decode('unicode_escape')
#             return json.loads(fixed_text)
#         except Exception:
#             pass

#         # Try trimming incomplete endings
#         cleaned = text
#         if cleaned.endswith(","):
#             cleaned = cleaned[:-1]
#         if cleaned.count("{") > cleaned.count("}"):
#             cleaned += "}" * (cleaned.count("{") - cleaned.count("}"))
#         if cleaned.count("[") > cleaned.count("]"):
#             cleaned += "]" * (cleaned.count("[") - cleaned.count("]"))

#         try:
#             return json.loads(cleaned)
#         except Exception as e2:
#             logger.error(f"Unrecoverable JSON on {date_str}: {e2}. Snippet: {text[:400]}")
#             return None


# # ===== Stock data fetch =====
# def fetch_stock_data(ticker, company_name, start_date, end_date):
#     logger.info(f"Fetching stock data for {company_name} ({ticker})...")
#     stock_data = yf.download(ticker, start=start_date, end=end_date, auto_adjust=False)

#     if isinstance(stock_data.columns, pd.MultiIndex):
#         stock_data.columns = stock_data.columns.get_level_values(0)

#     stock_data.reset_index(inplace=True)
#     stock_data["Date"] = pd.to_datetime(stock_data["Date"]).dt.date
#     stock_data["Daily_Return"] = stock_data["Adj Close"].pct_change()
#     stock_data["EMA_7"] = stock_data["Adj Close"].ewm(span=7, adjust=False).mean()
#     stock_data["EMA_21"] = stock_data["Adj Close"].ewm(span=21, adjust=False).mean()
#     stock_data["Ticker"] = ticker
#     stock_data["Company"] = company_name

#     logger.info(f"Successfully fetched {len(stock_data)} records.")
#     return stock_data


# # ===== GDELT news fetch =====
# def fetch_gdelt_finance_news(ticker, company_name, start_date, end_date, company_keywords=None, top_n=5):
#     if company_keywords is None:
#         company_keywords = [ticker, company_name]

#     impact_keywords = [
#         "earnings", "profit", "loss", "revenue", "guidance", "dividend",
#         "ipo", "lawsuit", "merger", "acquisition", "partnership",
#         "regulation", "fine", "recall", "data breach",
#         "analyst upgrade", "analyst downgrade", "stock buyback"
#     ]

#     finance_keywords = [
#         "finance", "market", "shares", "investment", "stock", "economy",
#         "ipo", "trading", "fund", "earnings", "revenue", "profit",
#         "dividend", "valuation", "forecast", "guidance"
#     ]

#     def is_stock_relevant(article):
#         title = (article.get("title") or "").lower()
#         summary = (article.get("seendoc") or article.get("snippet") or "").lower()
#         text = title + " " + summary
#         if not any(kw.lower() in text for kw in company_keywords):
#             return False
#         return any(kw.lower() in text for kw in impact_keywords) or "stock" in text or "shares" in text

#     def score_article(article):
#         title = article.get("title", "").lower()
#         return sum(title.count(k.lower()) for k in finance_keywords)

#     logger.info(f"Fetching finance-related news for {company_name} ({ticker})...")
#     base_url = "https://api.gdeltproject.org/api/v2/doc/doc"
#     all_results = []

#     current_date = datetime.strptime(start_date, "%Y-%m-%d")
#     end_date_dt = datetime.strptime(end_date, "%Y-%m-%d")

#     while current_date <= end_date_dt:
#         next_date = current_date + timedelta(days=1)
#         cleaned_company_name = re.sub(r'[^A-Za-z0-9\s]', '', company_name).strip()
#         cleaned_company_name = " ".join([w for w in cleaned_company_name.split() if len(w) >= 3])

#         params = {
#             "query": cleaned_company_name,
#             "mode": "ArtList",
#             "maxrecords": "250",
#             "format": "JSON",
#             "startdatetime": current_date.strftime("%Y%m%d%H%M%S"),
#             "enddatetime": next_date.strftime("%Y%m%d%H%M%S")
#         }

#         retries = 3
#         while retries > 0:
#             try:
#                 response = requests.get(base_url, params=params, timeout=15)
#                 if "Please limit requests" in response.text:
#                     logger.warning(f"Rate limited on {current_date.date()}, retrying in 10s...")
#                     time.sleep(10)
#                     retries -= 1
#                     continue

#                 data = safe_json_load(response.text, current_date.date())
#                 if not data or "articles" not in data:
#                     break

#                 articles = data["articles"]
#                 eng_articles = [a for a in articles if a.get("language", "").lower() in ["en", "english"]]
#                 relevant_articles = [a for a in eng_articles if is_stock_relevant(a)]
#                 if not relevant_articles:
#                     relevant_articles = [
#                         a for a in eng_articles
#                         if any(kw.lower() in (a.get("title", "") + " " + (a.get("snippet") or "")).lower()
#                                for kw in company_keywords)
#                     ]
#                 top_articles = sorted(relevant_articles, key=score_article, reverse=True)[:top_n]

#                 if top_articles:
#                     headlines = " | ".join(art.get("title", "") for art in top_articles)
#                     all_results.append({
#                         "Date": current_date.date(),
#                         "Headlines": headlines,
#                         "Ticker": ticker,
#                         "Company": company_name
#                     })
#                 break

#             except requests.RequestException as e:
#                 logger.error(f"Network error fetching {current_date.date()}: {e}")
#                 time.sleep(10)
#                 retries -= 1

#         time.sleep(5)
#         current_date = next_date

#     df = pd.DataFrame(all_results, columns=["Date", "Headlines", "Ticker", "Company"])
#     logger.info(f"Retrieved {len(df)} days of news for {company_name}.")
#     return df


# # ===== Main =====
# if __name__ == "__main__":
#     all_years_data = []
#     TICKER = "JPM"
#     COMPANY_NAME = "JPMorgan Chase & Co."
#     START_YEAR = 2021
#     END_YEAR = 2024

#     def clean_keywords(keywords):
#         cleaned = []
#         for kw in keywords:
#             kw = kw.replace(".", "").replace("&", "and")
#             cleaned_kw = re.sub(r'[^A-Za-z0-9\s]', '', kw).strip()
#             if any(len(word) >= 3 for word in cleaned_kw.split()):
#                 cleaned.append(cleaned_kw)
#         return cleaned

#     company_keywords = [
#         "JPMorgan Chase", "JPMorgan", "JP Morgan Chase", "JP Morgan", "JPMC",
#         "JPM", "Chase Bank", "Jamie Dimon", "Investment Banking",
#         "Commercial Banking", "Retail Banking", "Corporate Banking",
#         "Asset Management", "Wealth Management", "Private Banking",
#         "Financial Services", "Risk Management", "Capital Markets",
#         "Digital Banking", "Fintech", "Blockchain", "Payments",
#         "Global Economy", "Federal Reserve", "Interest Rates"
#     ]

#     company_keywords = clean_keywords(company_keywords)
#     OUTPUT_FILENAME = f"{TICKER}_stock_gdelt_final.csv"

#     for year in range(START_YEAR, END_YEAR + 1):
#         start_date = f"{year}-01-01"
#         end_date = f"{year}-12-31"

#         logger.info(f"\n===== Fetching data for {year} =====")
#         stock_data = fetch_stock_data(TICKER, COMPANY_NAME, start_date, end_date)
#         news_df = fetch_gdelt_finance_news(TICKER, COMPANY_NAME, start_date, end_date, company_keywords)

#         logger.info("Merging stock and news data...")
#         combined_df = pd.merge(stock_data, news_df, on=["Date", "Ticker", "Company"], how="left")

#         if year == START_YEAR:
#             combined_df.to_csv(OUTPUT_FILENAME, index=False)
#         else:
#             combined_df.to_csv(OUTPUT_FILENAME, mode='a', header=False, index=False)

#         logger.info(f"Year {year} saved to '{OUTPUT_FILENAME}'")
#         all_years_data.append(combined_df)
