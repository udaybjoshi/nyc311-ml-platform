# Databricks notebook source
# MAGIC %md
# MAGIC # 01 - API to Volume Ingestion
# MAGIC 
# MAGIC **Purpose**: Fetch data from Chicago 311 API and save as JSON files in Volume
# MAGIC 
# MAGIC **Pattern**: API → JSON File → Volume (Landing Zone)
# MAGIC 
# MAGIC **Schedule**: Daily (fetches previous day's data)
# MAGIC 
# MAGIC **Source**: https://data.cityofchicago.org/resource/v6vf-nfxy.json

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

import requests
import json
from datetime import datetime, timedelta
from pyspark.sql import functions as F

# API Configuration
API_URL = "https://data.cityofchicago.org/resource/v6vf-nfxy.json"
API_LIMIT = 50000  # Max records per request

# Volume paths
CATALOG = "workspace"
LANDING_PATH = f"/Volumes/{CATALOG}/raw/chi311_landing"
INITIAL_PATH = f"{LANDING_PATH}/initial"
INCREMENTAL_PATH = f"{LANDING_PATH}/incremental"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Widget Parameters
# MAGIC 
# MAGIC Run modes:
# MAGIC - `initial`: Full historical load (last 2 years)
# MAGIC - `incremental`: Daily incremental (yesterday's data)

# COMMAND ----------

dbutils.widgets.dropdown("load_type", "incremental", ["initial", "incremental"], "Load Type")
dbutils.widgets.text("days_back", "1", "Days Back (for incremental)")

load_type = dbutils.widgets.get("load_type")
days_back = int(dbutils.widgets.get("days_back"))

print(f"Load Type: {load_type}")
print(f"Days Back: {days_back}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Helper Functions

# COMMAND ----------

def fetch_from_api(where_clause: str = None, limit: int = API_LIMIT, offset: int = 0) -> list:
    """Fetch records from Chicago 311 API with pagination support"""
    
    params = {
        "$limit": limit,
        "$offset": offset,
        "$order": "created_date DESC"
    }
    
    if where_clause:
        params["$where"] = where_clause
    
    response = requests.get(API_URL, params=params)
    response.raise_for_status()
    
    return response.json()


def fetch_all_records(where_clause: str = None) -> list:
    """Fetch all records with pagination"""
    
    all_records = []
    offset = 0
    
    while True:
        print(f"Fetching records {offset} to {offset + API_LIMIT}...")
        records = fetch_from_api(where_clause=where_clause, offset=offset)
        
        if not records:
            break
            
        all_records.extend(records)
        offset += API_LIMIT
        
        # Safety limit (10 batches = 500K records max)
        if offset >= API_LIMIT * 10:
            print("Reached safety limit, stopping pagination")
            break
    
    return all_records


def save_to_volume(records: list, path: str, filename: str) -> str:
    """Save records as JSON file to Volume"""
    
    full_path = f"{path}/{filename}"
    
    # Convert to JSON string
    json_content = json.dumps(records, indent=2)
    
    # Write to volume using dbutils
    dbutils.fs.put(full_path, json_content, overwrite=True)
    
    return full_path

# COMMAND ----------

# MAGIC %md
# MAGIC ## Initial Load (Historical)
# MAGIC 
# MAGIC Fetches last 2 years of data for initial setup

# COMMAND ----------

if load_type == "initial":
    # Calculate date range (last 2 years)
    end_date = datetime.now()
    start_date = end_date - timedelta(days=730)  # ~2 years
    
    where_clause = f"created_date >= '{start_date.strftime('%Y-%m-%d')}'"
    
    print(f"Initial Load: {start_date.date()} to {end_date.date()}")
    print(f"Where clause: {where_clause}")
    
    # Fetch all records
    records = fetch_all_records(where_clause=where_clause)
    print(f"Total records fetched: {len(records)}")
    
    # Save to initial directory
    filename = f"chi311_initial_{end_date.strftime('%Y%m%d')}.json"
    saved_path = save_to_volume(records, INITIAL_PATH, filename)
    print(f"Saved to: {saved_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Incremental Load (Daily)
# MAGIC 
# MAGIC Fetches records from the last N days (default: yesterday)

# COMMAND ----------

if load_type == "incremental":
    # Calculate date range
    end_date = datetime.now()
    start_date = end_date - timedelta(days=days_back)
    
    # Use last_modified_date for true incremental (catches updates too)
    where_clause = f"last_modified_date >= '{start_date.strftime('%Y-%m-%dT00:00:00')}'"
    
    print(f"Incremental Load: {start_date.date()} to {end_date.date()}")
    print(f"Where clause: {where_clause}")
    
    # Fetch records
    records = fetch_from_api(where_clause=where_clause)
    print(f"Records fetched: {len(records)}")
    
    if records:
        # Save to incremental directory with timestamp
        filename = f"chi311_incremental_{end_date.strftime('%Y%m%d_%H%M%S')}.json"
        saved_path = save_to_volume(records, INCREMENTAL_PATH, filename)
        print(f"Saved to: {saved_path}")
    else:
        print("No new records to save")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify Files in Volume

# COMMAND ----------

print("Files in landing volume:")
print("-" * 60)

# List initial files
print("\nInitial directory:")
try:
    for f in dbutils.fs.ls(INITIAL_PATH):
        print(f"  {f.name} ({f.size:,} bytes)")
except:
    print("  (empty)")

# List incremental files
print("\nIncremental directory:")
try:
    for f in dbutils.fs.ls(INCREMENTAL_PATH):
        print(f"  {f.name} ({f.size:,} bytes)")
except:
    print("  (empty)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sample Data Preview

# COMMAND ----------

# Read one file to preview
if load_type == "initial":
    sample_path = INITIAL_PATH
else:
    sample_path = INCREMENTAL_PATH

try:
    files = dbutils.fs.ls(sample_path)
    if files:
        latest_file = sorted(files, key=lambda x: x.name)[-1]
        print(f"Previewing: {latest_file.path}")
        
        df = spark.read.json(latest_file.path)
        print(f"Records: {df.count()}")
        print(f"Columns: {len(df.columns)}")
        display(df.limit(5))
except Exception as e:
    print(f"No files to preview: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC 
# MAGIC | Metric | Value |
# MAGIC |--------|-------|
# MAGIC | Load Type | {load_type} |
# MAGIC | Records Fetched | {len(records) if 'records' in dir() else 'N/A'} |
# MAGIC | Saved To | {saved_path if 'saved_path' in dir() else 'N/A'} |
# MAGIC 
# MAGIC **Next Step**: Run `02_bronze_autoloader.py` to ingest files into Bronze Delta table