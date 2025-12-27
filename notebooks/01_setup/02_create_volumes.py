# Databricks notebook source
# MAGIC %md
# MAGIC # 02 - Create Volumes
# MAGIC 
# MAGIC **Purpose**: Set up Unity Catalog Volumes for file storage
# MAGIC 
# MAGIC **What are Volumes?**
# MAGIC 
# MAGIC Volumes are the governance layer for files in Unity Catalog. Unlike DBFS, Volumes provide:
# MAGIC - **Access control**: Fine-grained permissions via Unity Catalog
# MAGIC - **Discovery**: Files are visible in the catalog explorer
# MAGIC - **SQL queryability**: Can query files directly with SQL
# MAGIC - **Governance**: Full lineage and audit trail
# MAGIC 
# MAGIC **Run**: Once during initial setup (after 01_create_catalog_schemas.py)
# MAGIC 
# MAGIC **Prerequisites**:
# MAGIC - Run `01_create_catalog_schemas.py` first
# MAGIC - Unity Catalog schemas must exist

# COMMAND ----------

# MAGIC %md
# MAGIC ## Volume Structure
# MAGIC 
# MAGIC ```
# MAGIC /Volumes/workspace/raw/chi311_landing/
# MAGIC ├── initial/              # First bulk load (historical data)
# MAGIC │   └── chi311_20241201.json
# MAGIC ├── incremental/          # Daily incremental files
# MAGIC │   ├── chi311_20241202_060000.json
# MAGIC │   └── chi311_20241203_060000.json
# MAGIC └── archive/              # Processed files (optional)
# MAGIC 
# MAGIC /Volumes/workspace/bronze/chi311_checkpoint/
# MAGIC ├── autoloader/           # Autoloader streaming checkpoint
# MAGIC └── schema/               # Inferred schema location
# MAGIC 
# MAGIC /Volumes/workspace/ml/chi311_models/
# MAGIC └── prophet/              # Exported Prophet models
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Widget Parameters

# COMMAND ----------

# Create widgets
dbutils.widgets.text("catalog_name", "workspace", "Catalog Name")
dbutils.widgets.dropdown("reset_volumes", "false", ["true", "false"], "Reset Volumes (DROP ALL)")

# Get values
CATALOG_NAME = dbutils.widgets.get("catalog_name")
RESET_VOLUMES = dbutils.widgets.get("reset_volumes") == "true"

print(f"Catalog Name: {CATALOG_NAME}")
print(f"Reset Volumes: {RESET_VOLUMES}")

if RESET_VOLUMES:
    print("\n⚠️ WARNING: Reset mode enabled - existing volumes will be dropped!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

# Volume definitions: {schema: {volume_name: {description, directories}}}
VOLUMES = {
    "raw": {
        "chi311_landing": {
            "description": "Landing zone for raw JSON files from Chicago 311 API",
            "directories": [
                "initial",      # Bulk historical load
                "incremental",  # Daily incremental files
                "archive"       # Processed files (optional)
            ]
        }
    },
    "bronze": {
        "chi311_checkpoint": {
            "description": "Checkpoint location for Autoloader streaming state",
            "directories": [
                "autoloader",   # Streaming checkpoint
                "schema"        # Schema inference location
            ]
        }
    },
    "ml": {
        "chi311_models": {
            "description": "Storage for exported ML models and artifacts",
            "directories": [
                "prophet",      # Prophet model exports
                "predictions",  # Batch prediction outputs
                "features"      # Feature store exports
            ]
        }
    }
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Helper Functions

# COMMAND ----------

def volume_exists(catalog: str, schema: str, volume: str) -> bool:
    """Check if a volume exists"""
    try:
        spark.sql(f"DESCRIBE VOLUME {catalog}.{schema}.{volume}")
        return True
    except Exception as e:
        if "VOLUME_NOT_FOUND" in str(e) or "does not exist" in str(e):
            return False
        raise e


def create_volume(catalog: str, schema: str, volume: str, description: str) -> bool:
    """Create a managed volume"""
    full_name = f"{catalog}.{schema}.{volume}"
    try:
        spark.sql(f"""
            CREATE VOLUME IF NOT EXISTS {full_name}
            COMMENT '{description}'
        """)
        print(f"Created volume: {full_name}")
        return True
    except Exception as e:
        print(f"Failed to create volume {full_name}: {e}")
        return False


def drop_volume(catalog: str, schema: str, volume: str) -> bool:
    """Drop a volume"""
    full_name = f"{catalog}.{schema}.{volume}"
    try:
        spark.sql(f"DROP VOLUME IF EXISTS {full_name}")
        print(f"Dropped volume: {full_name}")
        return True
    except Exception as e:
        print(f"Failed to drop volume {full_name}: {e}")
        return False


def create_directory(path: str) -> bool:
    """Create a directory in a volume"""
    try:
        dbutils.fs.mkdirs(path)
        print(f"Created directory: {path}")
        return True
    except Exception as e:
        print(f"Failed to create directory {path}: {e}")
        return False


def get_volume_path(catalog: str, schema: str, volume: str) -> str:
    """Get the file system path for a volume"""
    return f"/Volumes/{catalog}/{schema}/{volume}"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Set Catalog Context

# COMMAND ----------

# Set catalog
spark.sql(f"USE CATALOG {CATALOG_NAME}")
print(f"Using catalog: {CATALOG_NAME}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Reset Volumes (Optional)

# COMMAND ----------

if RESET_VOLUMES:
    print("Resetting volumes...")
    print("-" * 50)
    
    for schema_name, volumes in VOLUMES.items():
        for volume_name in volumes.keys():
            if volume_exists(CATALOG_NAME, schema_name, volume_name):
                drop_volume(CATALOG_NAME, schema_name, volume_name)
            else:
                print(f"Volume '{volume_name}' doesn't exist, skipping")
    
    print("\n Reset complete")
else:
    print("Reset mode disabled - existing volumes will be preserved")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Create Volumes

# COMMAND ----------

print(f"\nCreating volumes in catalog '{CATALOG_NAME}':")
print("=" * 60)

created_volumes = 0
existing_volumes = 0
failed_volumes = 0

for schema_name, volumes in VOLUMES.items():
    print(f"\n Schema: {schema_name}")
    print("-" * 40)
    
    for volume_name, config in volumes.items():
        if volume_exists(CATALOG_NAME, schema_name, volume_name):
            print(f"⏭️ Volume '{volume_name}' already exists")
            existing_volumes += 1
        else:
            if create_volume(CATALOG_NAME, schema_name, volume_name, config["description"]):
                created_volumes += 1
            else:
                failed_volumes += 1

print(f"\n{'='*60}")
print(f"Volumes: Created={created_volumes}, Existing={existing_volumes}, Failed={failed_volumes}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Create Directory Structure

# COMMAND ----------

print("\nCreating directory structure:")
print("=" * 60)

for schema_name, volumes in VOLUMES.items():
    for volume_name, config in volumes.items():
        volume_path = get_volume_path(CATALOG_NAME, schema_name, volume_name)
        print(f"\n📂 {volume_path}/")
        
        for directory in config.get("directories", []):
            dir_path = f"{volume_path}/{directory}"
            create_directory(dir_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Verify Setup

# COMMAND ----------

print("\n Volumes in catalog:")
print("=" * 60)

for schema_name in VOLUMES.keys():
    print(f"\nSchema: {schema_name}")
    try:
        volumes_df = spark.sql(f"SHOW VOLUMES IN {CATALOG_NAME}.{schema_name}")
        display(volumes_df)
    except Exception as e:
        print(f"  Error listing volumes: {e}")

# COMMAND ----------

# List directory contents
print("\n Directory Structure:")
print("=" * 60)

for schema_name, volumes in VOLUMES.items():
    for volume_name, config in volumes.items():
        volume_path = get_volume_path(CATALOG_NAME, schema_name, volume_name)
        print(f"\n{volume_path}/")
        
        try:
            files = dbutils.fs.ls(volume_path)
            for f in files:
                print(f"  ├── {f.name}")
        except Exception as e:
            print(f"  (empty or error: {e})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Output: Volume Paths for Other Notebooks

# COMMAND ----------

# Volume paths configuration
volume_paths = {
    # Landing zone for raw files
    "landing": {
        "base": f"/Volumes/{CATALOG_NAME}/raw/chi311_landing",
        "initial": f"/Volumes/{CATALOG_NAME}/raw/chi311_landing/initial",
        "incremental": f"/Volumes/{CATALOG_NAME}/raw/chi311_landing/incremental",
        "archive": f"/Volumes/{CATALOG_NAME}/raw/chi311_landing/archive"
    },
    # Checkpoint locations
    "checkpoint": {
        "base": f"/Volumes/{CATALOG_NAME}/bronze/chi311_checkpoint",
        "autoloader": f"/Volumes/{CATALOG_NAME}/bronze/chi311_checkpoint/autoloader",
        "schema": f"/Volumes/{CATALOG_NAME}/bronze/chi311_checkpoint/schema"
    },
    # ML artifacts
    "ml": {
        "base": f"/Volumes/{CATALOG_NAME}/ml/chi311_models",
        "prophet": f"/Volumes/{CATALOG_NAME}/ml/chi311_models/prophet",
        "predictions": f"/Volumes/{CATALOG_NAME}/ml/chi311_models/predictions",
        "features": f"/Volumes/{CATALOG_NAME}/ml/chi311_models/features"
    }
}

print("Volume Paths for Other Notebooks:")
print("=" * 60)
print("\n# Copy these paths to your notebooks:\n")

print("# Landing zone")
print(f'LANDING_PATH = "{volume_paths["landing"]["base"]}"')
print(f'INITIAL_PATH = "{volume_paths["landing"]["initial"]}"')
print(f'INCREMENTAL_PATH = "{volume_paths["landing"]["incremental"]}"')

print("\n# Checkpoints")
print(f'CHECKPOINT_PATH = "{volume_paths["checkpoint"]["autoloader"]}"')
print(f'SCHEMA_PATH = "{volume_paths["checkpoint"]["schema"]}"')

print("\n# ML artifacts")
print(f'MODELS_PATH = "{volume_paths["ml"]["prophet"]}"')
print(f'PREDICTIONS_PATH = "{volume_paths["ml"]["predictions"]}"')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Quick Test: Write a Sample File

# COMMAND ----------

# Test writing a file to the landing zone
test_content = '{"test": "hello", "message": "Volume is working!"}'
test_path = f"{volume_paths['landing']['base']}/test_file.json"

try:
    dbutils.fs.put(test_path, test_content, overwrite=True)
    print(f"Successfully wrote test file: {test_path}")
    
    # Read it back
    content = dbutils.fs.head(test_path)
    print(f"Read content: {content}")
    
    # Clean up
    dbutils.fs.rm(test_path)
    print(f"Cleaned up test file")
    
except Exception as e:
    print(f"Test failed: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC 
# MAGIC Created Unity Catalog Volumes for Chicago 311 project:
# MAGIC 
# MAGIC | Volume | Path | Purpose |
# MAGIC |--------|------|---------|
# MAGIC | `chi311_landing` | `/Volumes/workspace/raw/chi311_landing` | Raw JSON files from API |
# MAGIC | `chi311_checkpoint` | `/Volumes/workspace/bronze/chi311_checkpoint` | Autoloader state |
# MAGIC | `chi311_models` | `/Volumes/workspace/ml/chi311_models` | ML model artifacts |
# MAGIC 
# MAGIC **Directory Structure**:
# MAGIC - `landing/initial/` - Historical bulk load
# MAGIC - `landing/incremental/` - Daily incremental files
# MAGIC - `checkpoint/autoloader/` - Streaming checkpoint
# MAGIC - `models/prophet/` - Prophet model exports
# MAGIC 
# MAGIC **Next Step**: Run `02_ingestion/01_api_to_volume.py` to fetch data from API

# COMMAND ----------

# Return success status
dbutils.notebook.exit("SUCCESS")