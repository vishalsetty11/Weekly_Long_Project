import datetime
import os
from config import DB_PATH
from Downloader import download_to_cloud
from ETL.bronze import load_bronze
from ETL.silver import load_silver
from ETL.gold import load_gold

def get_previous_business_day(today):
    """Calculates the most recent business day for NSE data."""
    day_of_week = today.weekday()
    if day_of_week == 0:    # Monday -> Friday
        return today - datetime.timedelta(days=3)
    elif day_of_week == 6:  # Sunday -> Friday
        return today - datetime.timedelta(days=2)
    else:                   # Tuesday-Saturday -> Previous day
        return today - datetime.timedelta(days=1)

def run_nse_pipeline():
    """
    Orchestrates the 100% Cloud-Native Pipeline.
    Calculates latest business day for logging, then performs 1-year sync.
    """
    # PRECISE ADDITION: Calculate current target date for logging
    target_date = get_previous_business_day(datetime.date.today())
    
    print(f"\n{'='*60}")
    print(f"🚀 STARTING CLOUD-NATIVE PIPELINE")
    print(f"📍 TARGET DB : {DB_PATH.split('?')[0]}") 
    print(f"📅 LATEST TARGET : {target_date.strftime('%Y-%m-%d')}")
    print(f"{'='*60}\n")

    # 1. DOWNLOAD STAGE: 
    # Logic: Scans 365 days back, skipping weekends/holidays and already downloaded files.
    success = download_to_cloud(DB_PATH, days_back=365)

    if success:
        # 2. BRONZE STAGE: Parse CSV strings from 'ingestedCSVData'
        load_bronze(DB_PATH)

        # 3. SILVER STAGE: Cleaning, Filtering (EQ Only), Weekly & 6-Month Vol
        load_silver(DB_PATH)

        # 4. GOLD STAGE: Signal Generation
        load_gold(DB_PATH)
        
        print(f"\n✅ Pipeline Complete. Data live in MotherDuck.")
    else:
        print("\n❌ Pipeline Halted: Critical error in Downloader.")

if __name__ == "__main__":
    run_nse_pipeline()