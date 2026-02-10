# Databricks notebook source
# MAGIC %md
# MAGIC # 03 - Silver: Clean, Enrich & Detect Anomalies
# MAGIC
# MAGIC Transforms Bronze raw data into validated, enriched Silver tables.
# MAGIC
# MAGIC Processing steps:
# MAGIC 1. Parse and validate timestamps
# MAGIC 2. Apply physics-based range validation
# MAGIC 3. Route bad records to quarantine table
# MAGIC 4. Deduplicate using Delta MERGE
# MAGIC 5. Enrich with equipment registry metadata
# MAGIC 6. Run anomaly detection (Z-score, threshold)
# MAGIC 7. Write anomaly events to separate table

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Configuration

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql.types import *

CATALOG_NAME = "mining_telemetry"
BRONZE = f"{CATALOG_NAME}.bronze"
SILVER = f"{CATALOG_NAME}.silver"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Create Silver Tables

# COMMAND ----------

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {SILVER}.cleaned_sensor_readings (
        equipment_id STRING NOT NULL,
        equipment_type STRING NOT NULL,
        reading_timestamp TIMESTAMP NOT NULL,
        reading_date DATE,
        -- Haul truck
        engine_temp_c DOUBLE,
        oil_pressure_bar DOUBLE,
        tire_pressure_psi DOUBLE,
        fuel_rate_lph DOUBLE,
        payload_tonnes DOUBLE,
        speed_kmh DOUBLE,
        gps_latitude DOUBLE,
        gps_longitude DOUBLE,
        -- Excavator
        hydraulic_pressure_bar DOUBLE,
        boom_angle_deg DOUBLE,
        swing_torque_knm DOUBLE,
        bucket_fill_factor DOUBLE,
        hydraulic_oil_temp_c DOUBLE,
        cycle_time_sec DOUBLE,
        -- Conveyor
        belt_speed_mps DOUBLE,
        motor_current_amps DOUBLE,
        vibration_x_mms DOUBLE,
        vibration_y_mms DOUBLE,
        vibration_z_mms DOUBLE,
        belt_alignment_mm DOUBLE,
        motor_temp_c DOUBLE,
        tonnage_tph DOUBLE,
        -- Crusher
        feed_rate_tph DOUBLE,
        power_draw_kw DOUBLE,
        css_mm DOUBLE,
        crusher_vibration_mms DOUBLE,
        crusher_oil_temp_c DOUBLE,
        -- Common
        operating_status STRING,
        engine_hours DOUBLE,
        -- Enrichment
        model STRING,
        manufacturer STRING,
        site_location STRING,
        max_engine_temp_c DOUBLE,
        max_oil_pressure_bar DOUBLE,
        max_payload_tonnes DOUBLE,
        -- Quality flags
        is_anomaly BOOLEAN,
        anomaly_reasons ARRAY<STRING>,
        quality_score DOUBLE COMMENT '0.0 to 1.0, 1.0 = perfect',
        -- Metadata
        bronze_ingestion_timestamp TIMESTAMP,
        silver_processed_timestamp TIMESTAMP
    )
    USING DELTA
    PARTITIONED BY (reading_date)
    COMMENT 'Cleaned and enriched sensor readings from all equipment types'
    TBLPROPERTIES (
        'delta.enableChangeDataFeed' = 'true',
        'quality' = 'silver'
    )
""")

# COMMAND ----------

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {SILVER}.quarantine_records (
        equipment_id STRING,
        equipment_type STRING,
        raw_timestamp STRING,
        quarantine_reason STRING,
        raw_record STRING,
        quarantined_at TIMESTAMP
    )
    USING DELTA
    COMMENT 'Records that failed Silver validation rules'
    TBLPROPERTIES ('quality' = 'quarantine')
""")

# COMMAND ----------

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {SILVER}.anomaly_events (
        event_id STRING NOT NULL,
        equipment_id STRING NOT NULL,
        equipment_type STRING NOT NULL,
        event_timestamp TIMESTAMP NOT NULL,
        anomaly_type STRING COMMENT 'threshold_breach, z_score, rate_of_change',
        severity STRING COMMENT 'critical, warning, watch',
        sensor_name STRING,
        sensor_value DOUBLE,
        threshold_value DOUBLE,
        z_score DOUBLE,
        description STRING,
        detected_at TIMESTAMP
    )
    USING DELTA
    COMMENT 'Anomaly events detected during Silver processing'
    TBLPROPERTIES (
        'delta.enableChangeDataFeed' = 'true',
        'quality' = 'silver'
    )
""")

print("Silver tables created")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Read Bronze Sensor Data

# COMMAND ----------

df_bronze = spark.table(f"{BRONZE}.raw_sensor_readings")
record_count = df_bronze.count()
print(f"Bronze sensor records to process: {record_count:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Timestamp Parsing and Validation
# MAGIC
# MAGIC Parse the string timestamp. Records with unparseable timestamps
# MAGIC get routed to quarantine.

# COMMAND ----------

df_parsed = (
    df_bronze
    .withColumn("parsed_ts", F.try_to_timestamp("timestamp", F.lit("yyyy-MM-dd'T'HH:mm:ss"))
)
    )

# Quarantine: records where timestamp could not be parsed
df_bad_ts = (
    df_parsed
    .filter(F.col("parsed_ts").isNull())
    .select(
        F.col("equipment_id"),
        F.col("equipment_type"),
        F.col("timestamp").alias("raw_timestamp"),
        F.lit("invalid_timestamp").alias("quarantine_reason"),
        F.to_json(F.struct("*")).alias("raw_record"),
        F.current_timestamp().alias("quarantined_at"),
    )
)

quarantine_count = df_bad_ts.count()
print(f"Records quarantined (bad timestamp): {quarantine_count:,}")

if quarantine_count > 0:
    df_bad_ts.write.format("delta").mode("append").saveAsTable(f"{SILVER}.quarantine_records")

# Keep only parseable records
df_valid = df_parsed.filter(F.col("parsed_ts").isNotNull())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Physics-Based Range Validation
# MAGIC
# MAGIC Flag or quarantine records that fall outside physically possible ranges.
# MAGIC Different rules per equipment type.

# COMMAND ----------

# Define validation bounds per sensor
VALIDATION_RULES = {
    # sensor_column: (min, max, quarantine_if_exceeded)
    "engine_temp_c": (-40, 150, True),
    "oil_pressure_bar": (0, 10, True),
    "tire_pressure_psi": (50, 140, False),
    "fuel_rate_lph": (0, 700, False),
    "payload_tonnes": (0, 450, False),
    "speed_kmh": (0, 80, False),
    "hydraulic_pressure_bar": (0, 420, True),
    "hydraulic_oil_temp_c": (-20, 120, True),
    "belt_speed_mps": (0, 8, False),
    "motor_current_amps": (0, 400, False),
    "vibration_x_mms": (0, 50, True),
    "vibration_y_mms": (0, 50, True),
    "vibration_z_mms": (0, 50, True),
    "motor_temp_c": (-20, 120, True),
    "feed_rate_tph": (0, 8000, False),
    "power_draw_kw": (0, 1500, False),
    "css_mm": (50, 250, False),
    "crusher_vibration_mms": (0, 50, True),
    "crusher_oil_temp_c": (-20, 120, True),
}

# Build quarantine condition: any sensor exceeding hard limits
quarantine_conditions = []
anomaly_flags = []

for col_name, (min_val, max_val, is_hard_limit) in VALIDATION_RULES.items():
    out_of_range = (
        F.col(col_name).isNotNull() &
        ((F.col(col_name) < min_val) | (F.col(col_name) > max_val))
    )
    if is_hard_limit:
        quarantine_conditions.append(out_of_range)
    anomaly_flags.append(
        F.when(out_of_range, F.lit(f"{col_name}_out_of_range"))
    )

# Combine quarantine conditions with OR
quarantine_expr = quarantine_conditions[0]
for cond in quarantine_conditions[1:]:
    quarantine_expr = quarantine_expr | cond

# COMMAND ----------

# Route hard-limit violations to quarantine
df_quarantine_range = (
    df_valid
    .filter(quarantine_expr)
    .select(
        F.col("equipment_id"),
        F.col("equipment_type"),
        F.col("timestamp").alias("raw_timestamp"),
        F.lit("out_of_range_hard_limit").alias("quarantine_reason"),
        F.to_json(F.struct("*")).alias("raw_record"),
        F.current_timestamp().alias("quarantined_at"),
    )
)

range_quarantine_count = df_quarantine_range.count()
print(f"Records quarantined (out of range): {range_quarantine_count:,}")

if range_quarantine_count > 0:
    df_quarantine_range.write.format("delta").mode("append").saveAsTable(f"{SILVER}.quarantine_records")

# Keep only records passing hard limits
df_clean = df_valid.filter(~quarantine_expr | ~quarantine_expr.isNotNull())

# More precise: remove exact quarantined records
df_clean = df_valid.subtract(df_valid.filter(quarantine_expr))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Add Quality Flags and Anomaly Reasons
# MAGIC
# MAGIC Soft-limit violations get flagged but not quarantined.
# MAGIC Build an array of anomaly reasons per record.

# COMMAND ----------

# Build anomaly_reasons array from all validation flags
anomaly_reason_cols = []
for col_name, (min_val, max_val, _) in VALIDATION_RULES.items():
    anomaly_reason_cols.append(
        F.when(
            F.col(col_name).isNotNull() &
            ((F.col(col_name) < min_val) | (F.col(col_name) > max_val)),
            F.lit(f"{col_name}_out_of_range")
        )
    )

df_flagged = (
    df_clean
    .withColumn(
        "anomaly_reasons",
        F.array_compact(F.array(*anomaly_reason_cols))
    )
    .withColumn(
        "is_anomaly",
        F.size("anomaly_reasons") > 0
    )
    .withColumn(
        "quality_score",
        F.when(F.size("anomaly_reasons") == 0, 1.0)
         .when(F.size("anomaly_reasons") == 1, 0.7)
         .when(F.size("anomaly_reasons") == 2, 0.4)
         .otherwise(0.1)
    )
)

anomaly_count = df_flagged.filter(F.col("is_anomaly")).count()
print(f"Records flagged as anomalous: {anomaly_count:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Deduplicate with Delta MERGE
# MAGIC
# MAGIC Sensor retransmissions can create exact duplicates.
# MAGIC We deduplicate on (equipment_id, timestamp) keeping the
# MAGIC record with the highest quality score.

# COMMAND ----------

# Deduplicate within the batch first
window_dedup = Window.partitionBy("equipment_id", "parsed_ts").orderBy(F.desc("quality_score"))

df_deduped = (
    df_flagged
    .withColumn("row_num", F.row_number().over(window_dedup))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

original_count = df_flagged.count()
deduped_count = df_deduped.count()
print(f"Before dedup: {original_count:,}")
print(f"After dedup:  {deduped_count:,}")
print(f"Duplicates removed: {original_count - deduped_count:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Enrich with Equipment Registry

# COMMAND ----------

df_registry = spark.table(f"{SILVER}.equipment_registry").select(
    "equipment_id",
    F.col("model"),
    F.col("manufacturer"),
    F.col("site_location"),
    F.col("max_engine_temp_c").alias("reg_max_engine_temp_c"),
    F.col("max_oil_pressure_bar").alias("reg_max_oil_pressure_bar"),
    F.col("max_payload_tonnes").alias("reg_max_payload_tonnes"),
)

df_enriched = (
    df_deduped
    .join(df_registry, on="equipment_id", how="left")
    .withColumn("reading_timestamp", F.col("parsed_ts"))
    .withColumn("reading_date", F.to_date("parsed_ts"))
    .withColumn("bronze_ingestion_timestamp", F.col("ingestion_timestamp"))
    .withColumn("silver_processed_timestamp", F.current_timestamp())
    .withColumn("max_engine_temp_c", F.col("reg_max_engine_temp_c"))
    .withColumn("max_oil_pressure_bar", F.col("reg_max_oil_pressure_bar"))
    .withColumn("max_payload_tonnes", F.col("reg_max_payload_tonnes"))
    .drop(
        "parsed_ts", "timestamp", "ingestion_timestamp", "source_file",
        "ingestion_date", "_rescued_data",
        "reg_max_engine_temp_c", "reg_max_oil_pressure_bar", "reg_max_payload_tonnes"
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Write to Silver Table (MERGE for idempotency)

# COMMAND ----------

# For initial load, use overwrite. For incremental, use MERGE.
# Check if table has data to decide approach.

existing_count = spark.table(f"{SILVER}.cleaned_sensor_readings").count()

if existing_count == 0:
    print("Initial load: writing with overwrite mode")
    (
        df_enriched.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(f"{SILVER}.cleaned_sensor_readings")
    )
else:
    print("Incremental load: using MERGE for deduplication")
    df_enriched.createOrReplaceTempView("silver_updates")

    spark.sql(f"""
        MERGE INTO {SILVER}.cleaned_sensor_readings AS target
        USING silver_updates AS source
        ON target.equipment_id = source.equipment_id
           AND target.reading_timestamp = source.reading_timestamp
        WHEN MATCHED AND source.quality_score > target.quality_score THEN
            UPDATE SET *
        WHEN NOT MATCHED THEN
            INSERT *
    """)

final_count = spark.table(f"{SILVER}.cleaned_sensor_readings").count()
print(f"Silver cleaned_sensor_readings: {final_count:,} records")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Anomaly Detection: Threshold-Based
# MAGIC
# MAGIC Generate anomaly events for readings that breach
# MAGIC equipment-specific thresholds from the registry.

# COMMAND ----------

df_silver = spark.table(f"{SILVER}.cleaned_sensor_readings")

# Haul truck anomalies
df_ht_anomalies = (
    df_silver
    .filter(F.col("equipment_type") == "haul_truck")
    .filter(F.col("operating_status") != "idle")
    .select(
        F.expr("uuid()").alias("event_id"),
        "equipment_id", "equipment_type", F.col("reading_timestamp").alias("event_timestamp"),
        # Engine overtemp
        F.when(
            F.col("engine_temp_c") > F.col("max_engine_temp_c") * 0.9,
            F.lit("threshold_breach")
        ).alias("anomaly_type"),
        F.when(
            F.col("engine_temp_c") > F.col("max_engine_temp_c"), F.lit("critical")
        ).when(
            F.col("engine_temp_c") > F.col("max_engine_temp_c") * 0.9, F.lit("warning")
        ).alias("severity"),
        F.lit("engine_temp_c").alias("sensor_name"),
        F.col("engine_temp_c").alias("sensor_value"),
        F.col("max_engine_temp_c").alias("threshold_value"),
        F.lit(None).cast(DoubleType()).alias("z_score"),
        F.concat(
            F.lit("Engine temp "),
            F.round("engine_temp_c", 1),
            F.lit("C exceeds "),
            F.round(F.col("max_engine_temp_c") * 0.9, 1),
            F.lit("C threshold")
        ).alias("description"),
        F.current_timestamp().alias("detected_at"),
    )
    .filter(F.col("anomaly_type").isNotNull())
)

# Conveyor vibration anomalies
df_cv_anomalies = (
    df_silver
    .filter(F.col("equipment_type") == "conveyor")
    .filter(F.col("operating_status") == "running")
    .withColumn("vibration_rms", F.sqrt(
        F.pow("vibration_x_mms", 2) +
        F.pow("vibration_y_mms", 2) +
        F.pow("vibration_z_mms", 2)
    ))
    .filter(F.col("vibration_rms") > 7.0)
    .select(
        F.expr("uuid()").alias("event_id"),
        "equipment_id", "equipment_type", F.col("reading_timestamp").alias("event_timestamp"),
        F.lit("threshold_breach").alias("anomaly_type"),
        F.when(F.col("vibration_rms") > 15, F.lit("critical"))
         .when(F.col("vibration_rms") > 10, F.lit("warning"))
         .otherwise(F.lit("watch")).alias("severity"),
        F.lit("vibration_rms").alias("sensor_name"),
        F.col("vibration_rms").alias("sensor_value"),
        F.lit(7.0).alias("threshold_value"),
        F.lit(None).cast(DoubleType()).alias("z_score"),
        F.concat(
            F.lit("Conveyor vibration RMS "),
            F.round("vibration_rms", 2),
            F.lit(" mm/s exceeds 7.0 mm/s")
        ).alias("description"),
        F.current_timestamp().alias("detected_at"),
    )
)

# Crusher vibration anomalies
df_cr_anomalies = (
    df_silver
    .filter(F.col("equipment_type") == "crusher")
    .filter(F.col("operating_status") == "crushing")
    .filter(F.col("crusher_vibration_mms") > 8.0)
    .select(
        F.expr("uuid()").alias("event_id"),
        "equipment_id", "equipment_type", F.col("reading_timestamp").alias("event_timestamp"),
        F.lit("threshold_breach").alias("anomaly_type"),
        F.when(F.col("crusher_vibration_mms") > 15, F.lit("critical"))
         .when(F.col("crusher_vibration_mms") > 10, F.lit("warning"))
         .otherwise(F.lit("watch")).alias("severity"),
        F.lit("crusher_vibration_mms").alias("sensor_name"),
        F.col("crusher_vibration_mms").alias("sensor_value"),
        F.lit(8.0).alias("threshold_value"),
        F.lit(None).cast(DoubleType()).alias("z_score"),
        F.concat(
            F.lit("Crusher vibration "),
            F.round("crusher_vibration_mms", 2),
            F.lit(" mm/s exceeds 8.0 mm/s")
        ).alias("description"),
        F.current_timestamp().alias("detected_at"),
    )
)

# Excavator hydraulic oil temp anomalies
df_ex_anomalies = (
    df_silver
    .filter(F.col("equipment_type") == "excavator")
    .filter(F.col("operating_status") == "digging")
    .filter(F.col("hydraulic_oil_temp_c") > 80)
    .select(
        F.expr("uuid()").alias("event_id"),
        "equipment_id", "equipment_type", F.col("reading_timestamp").alias("event_timestamp"),
        F.lit("threshold_breach").alias("anomaly_type"),
        F.when(F.col("hydraulic_oil_temp_c") > 95, F.lit("critical"))
         .when(F.col("hydraulic_oil_temp_c") > 85, F.lit("warning"))
         .otherwise(F.lit("watch")).alias("severity"),
        F.lit("hydraulic_oil_temp_c").alias("sensor_name"),
        F.col("hydraulic_oil_temp_c").alias("sensor_value"),
        F.lit(80.0).alias("threshold_value"),
        F.lit(None).cast(DoubleType()).alias("z_score"),
        F.concat(
            F.lit("Hydraulic oil temp "),
            F.round("hydraulic_oil_temp_c", 1),
            F.lit("C exceeds 80C threshold")
        ).alias("description"),
        F.current_timestamp().alias("detected_at"),
    )
)

# Union all anomalies and write
df_all_anomalies = (
    df_ht_anomalies
    .unionByName(df_cv_anomalies)
    .unionByName(df_cr_anomalies)
    .unionByName(df_ex_anomalies)
)

(
    df_all_anomalies.write
    .format("delta")
    .mode("overwrite")
    .option("mergeSchema", "true")
    .saveAsTable(f"{SILVER}.anomaly_events")
)

anomaly_total = df_all_anomalies.count()
print(f"Anomaly events detected: {anomaly_total:,}")
print("\nAnomaly breakdown by severity:")
df_all_anomalies.groupBy("severity").count().show()
print("Anomaly breakdown by equipment:")
df_all_anomalies.groupBy("equipment_id", "anomaly_type").count().orderBy("equipment_id").show(20)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 11. Add Delta Table Constraints

# COMMAND ----------

# These constraints prevent future writes from violating critical rules
try:
    spark.sql(f"""
        ALTER TABLE {SILVER}.cleaned_sensor_readings
        ADD CONSTRAINT valid_equipment_id EXPECT (equipment_id IS NOT NULL)
    """)
    spark.sql(f"""
        ALTER TABLE {SILVER}.cleaned_sensor_readings
        ADD CONSTRAINT valid_timestamp EXPECT (reading_timestamp IS NOT NULL)
    """)
    spark.sql(f"""
        ALTER TABLE {SILVER}.cleaned_sensor_readings
        ADD CONSTRAINT valid_quality EXPECT (quality_score >= 0.0 AND quality_score <= 1.0)
    """)
    print("Delta constraints added")
except Exception as e:
    print(f"Constraints may already exist: {str(e)[:100]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 12. Silver Layer Summary

# COMMAND ----------

print("=" * 60)
print("SILVER LAYER PROCESSING SUMMARY")
print("=" * 60)

silver_tables = [
    "cleaned_sensor_readings",
    "equipment_registry",
    "anomaly_events",
    "quarantine_records",
]

for table in silver_tables:
    try:
        count = spark.table(f"{SILVER}.{table}").count()
        print(f"  {table}: {count:,} records")
    except Exception:
        print(f"  {table}: empty")

# Quality score distribution
print("\nQuality score distribution:")
(
    spark.table(f"{SILVER}.cleaned_sensor_readings")
    .groupBy(
        F.when(F.col("quality_score") == 1.0, "perfect")
         .when(F.col("quality_score") >= 0.7, "good")
         .when(F.col("quality_score") >= 0.4, "fair")
         .otherwise("poor").alias("quality_band")
    )
    .count()
    .orderBy("quality_band")
    .show()
)

print("\nProceed to notebook 04_Gold_KPIs_And_Scoring.")