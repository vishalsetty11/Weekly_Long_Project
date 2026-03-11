import datetime
import os
from prefect import flow, task
import re
import pandas as pd
import duckdb

# PRECISE FIX: Fallback to MotherDuck if config.py is missing/ignored
DB_PATH = os.getenv("DB_PATH", "md:nse_market")

@task(retries=3, retry_delay_seconds=300)
def download_task():
    """Step 1: Download from NSE to MotherDuck Staging"""
    # PRECISE FIX: Import inside task to avoid ModuleNotFoundError on flow load
    from Downloader import download_to_cloud
    return download_to_cloud(DB_PATH)

@task
def bronze_task():
    """Step 2: Parse raw strings to Bronze Schema"""
    # PRECISE FIX: Import inside task
    from ETL.bronze import load_bronze
    load_bronze(DB_PATH)

@task
def silver_task():
    """Step 3: Clean and Cast to Silver Schema"""
    # PRECISE FIX: Import inside task
    from ETL.silver import load_silver
    load_silver(DB_PATH)

@task
def gold_task():
    """Step 4: Generate Buy Signals in Gold Schema"""
    # PRECISE FIX: Import inside task
    from ETL.gold import load_gold
    load_gold(DB_PATH)

@flow(name="NSE-Medallion-Cloud-Pipeline", log_prints=True)
def run_nse_pipeline():
    print(f"🚀 Initializing Cloud Flow...")
    if download_task():
        bronze_task()
        silver_task()
        gold_task()
        print("✅ Pipeline Success")
    else:
        print("⚠️ Archive not yet available at NSE.")

if __name__ == "__main__":
    # Deployment configuration for Prefect Cloud Managed Worker
    run_nse_pipeline.from_source(
        source="https://github.com/vishalsetty11/NSE_Bhavcopy_Project.git",
        entrypoint="prefectDeploy.py:run_nse_pipeline"
    ).deploy(
        name="daily-medallion-sync",
        work_pool_name="NSE_Bhavcopy_Project", 
        cron="30 13 * * 1-5",              
        job_variables={
            "pip_install": ["requests", "duckdb", "pandas", "motherduck"], 
            "env": {
                "MOTHERDUCK_TOKEN": "YOUR_ACTUAL_TOKEN_HERE",
                "DB_PATH": "md:nse_market"
            }
        }
    )