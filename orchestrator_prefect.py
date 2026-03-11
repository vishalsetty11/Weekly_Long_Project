import datetime
from prefect import flow, task
from config import DB_PATH
from Downloader import download_to_cloud
from ETL.bronze import load_bronze
from ETL.silver import load_silver
from ETL.gold import load_gold

@task(retries=3, retry_delay_seconds=300)
def download_task(target_date):
    """Step 1: Download from NSE to MotherDuck Memory Staging"""
    return download_to_cloud(target_date, DB_PATH)

@task
def bronze_task():
    """Step 2: Parse raw strings to Bronze Schema"""
    load_bronze(DB_PATH)

@task
def silver_task():
    """Step 3: Clean and Cast to Silver Schema"""
    load_silver(DB_PATH)

@task
def gold_task():
    """Step 4: Generate Buy Signals in Gold Schema"""
    load_gold(DB_PATH)

@flow(name="NSE-Medallion-Cloud-Pipeline", log_prints=True)
def nse_pipeline_flow():
    # Target Date: Usually T-1 (Yesterday)
    target_date = datetime.date.today() - datetime.timedelta(days=1)
    
    print(f"🚀 Initializing Cloud Flow for Date: {target_date}")
    
    # Execution Logic
    filename = download_task(target_date)
    
    if filename:
        bronze_task()
        silver_task()
        gold_task()
        print("✅ Pipeline Success")
    else:
        print("⚠️ Archive not yet available at NSE.")

if __name__ == "__main__":
    # LOCAL RUN: 
    # nse_pipeline_flow()

    # CLOUD DEPLOYMENT: 
    # This creates a 'Managed' deployment on Prefect Cloud
    # Run: python orchestrator_prefect.py 
    nse_pipeline_flow.serve(
        name="daily-nse-sync",
        cron="30 13 * * *"  # Runs daily at 1:30 PM UTC
    )