# Databricks notebook source
# MAGIC %md
# MAGIC # 02 - Bronze: Raw Ingestion
# MAGIC
# MAGIC Ingests raw sensor data from the landing volume into Bronze Delta tables.
# MAGIC
# MAGIC Uses Auto Loader (cloudFiles) for incremental file processing with:
# MAGIC - Schema enforcement and rescued data column
# MAGIC - Ingestion metadata (timestamp, source file, batch ID)
# MAGIC - Append-only writes, partitioned by ingestion_date
# MAGIC
# MAGIC Handles all 4 data sources:
# MAGIC 1. Synthetic equipment sensor JSON
# MAGIC 2. Kaggle flotation plant CSV (manual upload)
# MAGIC 3. AI4I 2020 maintenance labels CSV (manual upload)
# MAGIC 4. NASA IMS bearing vibration (manual upload)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Configuration

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import *

CATALOG_NAME = "mining_telemetry"
BRONZE_SCHEMA = "bronze"
VOLUME_PATH = f"/Volumes/{CATALOG_NAME}/{BRONZE_SCHEMA}/raw_landing"
SENSOR_DIR = f"{VOLUME_PATH}/sensor_readings"
CHECKPOINT_BASE = f"/Volumes/{CATALOG_NAME}/{BRONZE_SCHEMA}/checkpoints"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Ingest Synthetic Sensor Readings (Auto Loader)
# MAGIC
# MAGIC Auto Loader processes JSON files incrementally from the volume.
# MAGIC On Community Edition without Workflows, we use trigger-once
# MAGIC to process all available files in a single batch.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2a. Define sensor reading schema
# MAGIC
# MAGIC Explicit schema prevents Auto Loader from inferring types incorrectly.
# MAGIC The _rescued_data column catches any fields that don't match.

# COMMAND ----------

sensor_schema = StructType([
    StructField("equipment_id", StringType()),
    StructField("equipment_type", StringType()),
    StructField("timestamp", StringType()),
    # Haul truck
    StructField("engine_temp_c", DoubleType()),
    StructField("oil_pressure_bar", DoubleType()),
    StructField("tire_pressure_psi", DoubleType()),
    StructField("fuel_rate_lph", DoubleType()),
    StructField("payload_tonnes", DoubleType()),
    StructField("speed_kmh", DoubleType()),
    StructField("gps_latitude", DoubleType()),
    StructField("gps_longitude", DoubleType()),
    # Excavator
    StructField("hydraulic_pressure_bar", DoubleType()),
    StructField("boom_angle_deg", DoubleType()),
    StructField("swing_torque_knm", DoubleType()),
    StructField("bucket_fill_factor", DoubleType()),
    StructField("hydraulic_oil_temp_c", DoubleType()),
    StructField("cycle_time_sec", DoubleType()),
    # Conveyor
    StructField("belt_speed_mps", DoubleType()),
    StructField("motor_current_amps", DoubleType()),
    StructField("vibration_x_mms", DoubleType()),
    StructField("vibration_y_mms", DoubleType()),
    StructField("vibration_z_mms", DoubleType()),
    StructField("belt_alignment_mm", DoubleType()),
    StructField("motor_temp_c", DoubleType()),
    StructField("tonnage_tph", DoubleType()),
    # Crusher
    StructField("feed_rate_tph", DoubleType()),
    StructField("power_draw_kw", DoubleType()),
    StructField("css_mm", DoubleType()),
    StructField("crusher_vibration_mms", DoubleType()),
    StructField("crusher_oil_temp_c", DoubleType()),
    # Common
    StructField("operating_status", StringType()),
    StructField("engine_hours", DoubleType()),
])

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2b. Run Auto Loader ingestion

# COMMAND ----------

df_raw_sensors = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", f"{CHECKPOINT_BASE}/sensor_schema")
    .option("cloudFiles.inferColumnTypes", "false")
    .option("cloudFiles.rescuedDataColumn", "_rescued_data")
    .schema(sensor_schema)
    .load(SENSOR_DIR)
    # Add ingestion metadata
    .withColumn("ingestion_timestamp", F.current_timestamp())
    .withColumn("source_file", F.col("_metadata.file_path"))
    .withColumn("ingestion_date", F.current_date())
)

# Write to Bronze table (trigger once for batch processing)
(
    df_raw_sensors.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", f"{CHECKPOINT_BASE}/bronze_sensors")
    .option("mergeSchema", "true")
    .trigger(availableNow=True)
    .toTable(f"{CATALOG_NAME}.{BRONZE_SCHEMA}.raw_sensor_readings")
)

print("Sensor readings ingestion started (trigger: availableNow)")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2c. Verify sensor ingestion

# COMMAND ----------

df_verify = spark.table(f"{CATALOG_NAME}.{BRONZE_SCHEMA}.raw_sensor_readings")

print(f"Total records ingested: {df_verify.count():,}")
print(f"\nRecords by equipment type:")
df_verify.groupBy("equipment_type").count().orderBy("equipment_type").show()

print(f"Records with rescued data (schema mismatches):")
df_verify.filter(F.col("_rescued_data").isNotNull()).count()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Ingest Kaggle Flotation Plant Data
# MAGIC
# MAGIC The Kaggle Mining Process CSV uses semicolons as delimiters
# MAGIC and commas as decimal separators (Brazilian format).
# MAGIC
# MAGIC Upload the CSV to: /Volumes/mining_telemetry/bronze/raw_landing/flotation/
# MAGIC
# MAGIC File: MiningProcess_Flotation_Plant_Database.csv

# COMMAND ----------

FLOTATION_DIR = f"{VOLUME_PATH}/flotation"

# Check if flotation data exists
try:
    flotation_files = dbutils.fs.ls(FLOTATION_DIR)
    print(f"Found {len(flotation_files)} flotation file(s)")
    HAS_FLOTATION = True
except Exception:
    print("No flotation data found. Upload the Kaggle CSV to:")
    print(f"  {FLOTATION_DIR}/MiningProcess_Flotation_Plant_Database.csv")
    print("Skipping flotation ingestion.")
    HAS_FLOTATION = False

# COMMAND ----------

if HAS_FLOTATION:
    # Kaggle dataset uses commas as decimal separators (Brazilian format)
    # Read as strings, convert decimals, then cast to DOUBLE
    df_flotation_raw = (
        spark.read
        .option("header", "true")
        .option("delimiter", ",")
        .option("inferSchema", "false")
        .csv(f"{FLOTATION_DIR}/*.csv")
    )

    # Cast date to string, numeric columns: replace comma decimals with dots
    df_flotation_raw = df_flotation_raw.withColumn("date", F.col("date").cast("string"))
    numeric_cols = [c for c in df_flotation_raw.columns if c != "date"]
    for col_name in numeric_cols:
        df_flotation_raw = df_flotation_raw.withColumn(
            col_name,
            F.regexp_replace(F.col(col_name), ",", ".").cast("double")
        )

    # Rename columns to match our Bronze schema
    column_mapping = {
        "% Iron Feed": "iron_feed_pct",
        "% Silica Feed": "silica_feed_pct",
        "Starch Flow": "starch_flow",
        "Amina Flow": "amina_flow",
        "Ore Pulp Flow": "ore_pulp_flow",
        "Ore Pulp pH": "ore_pulp_ph",
        "Ore Pulp Density": "ore_pulp_density",
        "Flotation Column 01 Air Flow": "flotation_col_01_air_flow",
        "Flotation Column 02 Air Flow": "flotation_col_02_air_flow",
        "Flotation Column 03 Air Flow": "flotation_col_03_air_flow",
        "Flotation Column 04 Air Flow": "flotation_col_04_air_flow",
        "Flotation Column 05 Air Flow": "flotation_col_05_air_flow",
        "Flotation Column 06 Air Flow": "flotation_col_06_air_flow",
        "Flotation Column 07 Air Flow": "flotation_col_07_air_flow",
        "Flotation Column 01 Level": "flotation_col_01_level",
        "Flotation Column 02 Level": "flotation_col_02_level",
        "Flotation Column 03 Level": "flotation_col_03_level",
        "Flotation Column 04 Level": "flotation_col_04_level",
        "Flotation Column 05 Level": "flotation_col_05_level",
        "Flotation Column 06 Level": "flotation_col_06_level",
        "Flotation Column 07 Level": "flotation_col_07_level",
        "% Iron Concentrate": "iron_concentrate_pct",
        "% Silica Concentrate": "silica_concentrate_pct",
    }

    for old_name, new_name in column_mapping.items():
        if old_name in df_flotation_raw.columns:
            df_flotation_raw = df_flotation_raw.withColumnRenamed(old_name, new_name)

    df_flotation_bronze = (
        df_flotation_raw
        .withColumn("ingestion_timestamp", F.current_timestamp())
        .withColumn("source_file", F.lit("MiningProcess_Flotation_Plant_Database.csv"))
        .withColumn("ingestion_date", F.current_date())
        .withColumn("_rescued_data", F.lit(None).cast(StringType()))
    )

    (
        df_flotation_bronze.write
        .format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .saveAsTable(f"{CATALOG_NAME}.{BRONZE_SCHEMA}.raw_flotation_readings")
    )

    print(f"Flotation records ingested: {df_flotation_bronze.count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Ingest AI4I 2020 Predictive Maintenance Data
# MAGIC
# MAGIC Upload the CSV to: /Volumes/mining_telemetry/bronze/raw_landing/maintenance/
# MAGIC
# MAGIC File: predictive_maintenance.csv (from Kaggle)

# COMMAND ----------

MAINTENANCE_DIR = f"{VOLUME_PATH}/maintenance"

try:
    maint_files = dbutils.fs.ls(MAINTENANCE_DIR)
    print(f"Found {len(maint_files)} maintenance file(s)")
    HAS_MAINTENANCE = True
except Exception:
    print("No maintenance label data found. Upload the AI4I CSV to:")
    print(f"  {MAINTENANCE_DIR}/predictive_maintenance.csv")
    print("Skipping maintenance labels ingestion.")
    HAS_MAINTENANCE = False

# COMMAND ----------

if HAS_MAINTENANCE:
    df_maint_raw = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "false")
        .csv(f"{MAINTENANCE_DIR}/*.csv")
    )

    # Rename columns to match Bronze schema
    maint_mapping = {
        "UDI": "uid",
        "Product ID": "product_id",
        "Type": "type",
        "Air temperature [K]": "air_temperature_k",
        "Process temperature [K]": "process_temperature_k",
        "Rotational speed [rpm]": "rotational_speed_rpm",
        "Torque [Nm]": "torque_nm",
        "Tool wear [min]": "tool_wear_min",
        "Machine failure": "machine_failure",
        "TWF": "twf",
        "HDF": "hdf",
        "PWF": "pwf",
        "OSF": "osf",
        "RNF": "rnf",
    }

    for old_name, new_name in maint_mapping.items():
        if old_name in df_maint_raw.columns:
            df_maint_raw = df_maint_raw.withColumnRenamed(old_name, new_name)

    # Explicit casts to match Bronze table schema
    double_cols = ["air_temperature_k", "process_temperature_k",
                   "rotational_speed_rpm", "torque_nm", "tool_wear_min"]
    int_cols = ["machine_failure", "twf", "hdf", "pwf", "osf", "rnf"]

    for c in double_cols:
        if c in df_maint_raw.columns:
            df_maint_raw = df_maint_raw.withColumn(c, F.col(c).cast("double"))
    for c in int_cols:
        if c in df_maint_raw.columns:
            df_maint_raw = df_maint_raw.withColumn(c, F.col(c).cast("int"))

    df_maint_bronze = (
        df_maint_raw
        .withColumn("ingestion_timestamp", F.current_timestamp())
        .withColumn("source_file", F.lit("predictive_maintenance.csv"))
        .withColumn("ingestion_date", F.current_date())
        .withColumn("_rescued_data", F.lit(None).cast(StringType()))
    )

    (
        df_maint_bronze.write
        .format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .saveAsTable(f"{CATALOG_NAME}.{BRONZE_SCHEMA}.raw_maintenance_labels")
    )

    print(f"Maintenance label records ingested: {df_maint_bronze.count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Ingest NASA IMS Bearing Vibration Data
# MAGIC
# MAGIC The NASA dataset is structured as directories per experiment,
# MAGIC with each file containing vibration samples for 4 bearings.
# MAGIC
# MAGIC Upload to: /Volumes/mining_telemetry/bronze/raw_landing/bearing/
# MAGIC
# MAGIC Expected structure:
# MAGIC   bearing/1st_test/2003.10.22.12.06.24 (tab-separated, 4 columns)

# COMMAND ----------

BEARING_DIR = f"{VOLUME_PATH}/bearing"

try:
    bearing_dirs = dbutils.fs.ls(BEARING_DIR)
    print(f"Found {len(bearing_dirs)} bearing experiment directory(s)")
    HAS_BEARING = True
except Exception:
    print("No bearing data found. Upload the NASA IMS dataset to:")
    print(f"  {BEARING_DIR}/1st_test/")
    print(f"  {BEARING_DIR}/2nd_test/")
    print(f"  {BEARING_DIR}/3rd_test/")
    print("Skipping bearing vibration ingestion.")
    HAS_BEARING = False

# COMMAND ----------

if HAS_BEARING:
    from pyspark.sql import Row

    bearing_records = []
    experiment_map = {"1st_test": 1, "2nd_test": 2, "3rd_test": 3}

    for exp_dir in bearing_dirs:
        exp_name = exp_dir.name.rstrip("/")
        exp_id = experiment_map.get(exp_name, 0)
        if exp_id == 0:
            continue

        try:
            files = dbutils.fs.ls(exp_dir.path)
        except Exception:
            continue

        for idx, f in enumerate(sorted(files, key=lambda x: x.name)):
            if f.isDir():
                continue
            try:
                # Each file has 4 columns (one per bearing), tab-separated
                df_file = (
                    spark.read
                    .option("delimiter", "\t")
                    .option("header", "false")
                    .csv(f.path)
                )
                cols = df_file.columns
                for bearing_idx, col in enumerate(cols[:4]):
                    values = [row[col] for row in df_file.select(col).collect()]
                    float_values = []
                    for v in values:
                        try:
                            float_values.append(float(v))
                        except (ValueError, TypeError):
                            continue

                    bearing_records.append(Row(
                        experiment_id=exp_id,
                        bearing_id=bearing_idx + 1,
                        measurement_index=idx,
                        timestamp=f.name,
                        vibration_signal=float_values[:2048],  # cap at 2048 samples
                        sample_rate_hz=20480,
                    ))
            except Exception as e:
                print(f"  Skipped {f.name}: {str(e)[:80]}")
                continue

        print(f"  Processed experiment {exp_id}: {exp_name}")

    if bearing_records:
        df_bearing = (
            spark.createDataFrame(bearing_records)
            .withColumn("ingestion_timestamp", F.current_timestamp())
            .withColumn("source_file", F.lit("nasa_ims_bearing"))
            .withColumn("ingestion_date", F.current_date())
            .withColumn("_rescued_data", F.lit(None).cast(StringType()))
        )

        (
            df_bearing.write
            .format("delta")
            .mode("append")
            .option("mergeSchema", "true")
            .saveAsTable(f"{CATALOG_NAME}.{BRONZE_SCHEMA}.raw_bearing_vibration")
        )

        print(f"Bearing vibration records ingested: {df_bearing.count():,}")
    else:
        print("No bearing records parsed. Check file format.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Bronze Layer Summary

# COMMAND ----------

print("=" * 60)
print("BRONZE LAYER INGESTION SUMMARY")
print("=" * 60)

bronze_tables = [
    "raw_sensor_readings",
    "raw_flotation_readings",
    "raw_maintenance_labels",
    "raw_bearing_vibration",
]

for table in bronze_tables:
    try:
        count = spark.table(f"{CATALOG_NAME}.{BRONZE_SCHEMA}.{table}").count()
        print(f"  {table}: {count:,} records")
    except Exception:
        print(f"  {table}: empty (data not uploaded yet)")

print(f"\nCheckpoint location: {CHECKPOINT_BASE}")
print("\nProceed to notebook 03_Silver_Clean_Enrich.")