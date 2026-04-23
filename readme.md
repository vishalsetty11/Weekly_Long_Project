# Weekly Long

A cloud-native, end-to-end ELT pipeline that ingests daily National Stock Exchange (NSE) data into a MotherDuck data lake. It uses the Medallion Architecture to transform raw data into actionable "WeeklyLong" trading signals.

## 🚀 Key Features

* **100% Cloud-Native:** Downloads data directly to memory and dumps it into MotherDuck—zero local disk footprint.
* **Medallion Architecture:** Structured layers for data integrity (Bronze 🥉, Silver 🥈, Gold 🥇).
* **Automated Pipeline:** Scheduled via Render CRON Jobs to run daily after market close.
* **Interactive Dashboard:** Flask-based UI with signal monitoring.

## 🛠️ Tech Stack

* **Database:** MotherDuck (Cloud DuckDB)
* **Engine:** DuckDB
* **Backend:** Python 3.11+
* **Dashboard:** Flask, Tailwind CSS, JavaScript
* **Automation:** Render CRON Jobs
