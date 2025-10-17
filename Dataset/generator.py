import yfinance as yf
import pandas as pd
import requests
from datetime import datetime, timedelta
import json
import logging
import os

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
        params = {
            "query": company_name,
            "mode": "ArtList",
            "maxrecords": "250",
            "format": "JSON",
            "startdatetime": current_date.strftime("%Y%m%d%H%M%S"),
            "enddatetime": next_date.strftime("%Y%m%d%H%M%S")
        }

        try:
            response = requests.get(base_url, params=params, timeout=15)
            fixed_text = response.text.encode('utf-8').decode('unicode_escape')
            data = json.loads(fixed_text)
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
        except Exception as e:
            logger.error(f"Error fetching {current_date.date()}: {e}")

        current_date = next_date

    df = pd.DataFrame(all_results, columns=["Date", "Headlines", "Ticker", "Company"])
    logger.info(f"Retrieved {len(df)} days of news for {company_name}.")
    return df

# ===== Main =====
if __name__ == "__main__":
    all_years_data = []
    TICKER = "AMZN"
    COMPANY_NAME = "Amazon.com Inc."
    START_YEAR = 2021
    END_YEAR = 2024 
    company_keywords = [
        "AMZN", "Amazon", "Amazon.com", "Amazon.com Inc", "Jeff Bezos", "Andy Jassy",
        "Prime", "Prime Video", "Amazon Prime", "AWS", "Amazon Web Services",
        "Kindle", "Echo", "Alexa", "Fulfillment", "Marketplace", "Whole Foods",
        "retail", "e-commerce", "logistics", "delivery", "seller"
    ]
    OUTPUT_FILENAME = f"{TICKER}_stock_gdelt_final.csv"

    for year in range(START_YEAR, END_YEAR + 1):
        start_date = f"{year}-01-01"
        end_date=f"{year}-12-31"

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