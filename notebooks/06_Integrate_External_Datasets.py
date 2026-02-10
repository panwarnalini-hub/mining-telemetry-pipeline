# Databricks notebook source
# MAGIC %md
# MAGIC # 06 - Integrate External Datasets (Kaggle + NASA)
# MAGIC 
# MAGIC Processes and integrates the three external datasets into the pipeline:
# MAGIC 1. Kaggle Mining Process (flotation plant) - Silver + Gold
# MAGIC 2. AI4I 2020 Predictive Maintenance - Gold (failure calibration)
# MAGIC 3. NASA IMS Bearing Vibration - Silver (bearing features)
# MAGIC
# MAGIC **Prerequisites:**
# MAGIC - Notebooks 00 through 04 have been run
# MAGIC - Upload datasets to the following volume paths:
# MAGIC   - `/Volumes/mining_telemetry/bronze/raw_landing/flotation/`
# MAGIC   - `/Volumes/mining_telemetry/bronze/raw_landing/maintenance/`
# MAGIC   - `/Volumes/mining_telemetry/bronze/raw_landing/bearing/`

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
GOLD = f"{CATALOG_NAME}.gold"
VOLUME_PATH = f"/Volumes/{CATALOG_NAME}/bronze/raw_landing"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Flotation Plant Data (Kaggle Mining Process)
# MAGIC
# MAGIC This dataset has 737K rows of real flotation plant sensor data.
# MAGIC It feeds into:
# MAGIC - Silver: cleaned_flotation_readings
# MAGIC - Gold: flotation_quality_scores

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2a. Create Silver Flotation Table

# COMMAND ----------

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {SILVER}.cleaned_flotation_readings (
        reading_timestamp TIMESTAMP NOT NULL,
        reading_date DATE,
        iron_feed_pct DOUBLE,
        silica_feed_pct DOUBLE,
        starch_flow DOUBLE,
        amina_flow DOUBLE,
        ore_pulp_flow DOUBLE,
        ore_pulp_ph DOUBLE,
        ore_pulp_density DOUBLE,
        flotation_col_01_air_flow DOUBLE,
        flotation_col_02_air_flow DOUBLE,
        flotation_col_03_air_flow DOUBLE,
        flotation_col_04_air_flow DOUBLE,
        flotation_col_05_air_flow DOUBLE,
        flotation_col_06_air_flow DOUBLE,
        flotation_col_07_air_flow DOUBLE,
        flotation_col_01_level DOUBLE,
        flotation_col_02_level DOUBLE,
        flotation_col_03_level DOUBLE,
        flotation_col_04_level DOUBLE,
        flotation_col_05_level DOUBLE,
        flotation_col_06_level DOUBLE,
        flotation_col_07_level DOUBLE,
        iron_concentrate_pct DOUBLE,
        silica_concentrate_pct DOUBLE,
        -- Computed fields
        avg_air_flow DOUBLE,
        avg_column_level DOUBLE,
        silica_above_threshold BOOLEAN COMMENT 'True if silica concentrate > 3%',
        quality_score DOUBLE COMMENT '0.0 to 1.0',
        -- Metadata
        silver_processed_timestamp TIMESTAMP
    )
    USING DELTA
    PARTITIONED BY (reading_date)
    COMMENT 'Cleaned flotation plant readings with computed quality metrics'
    TBLPROPERTIES (
        'delta.enableChangeDataFeed' = 'true',
        'quality' = 'silver'
    )
""")

print("Silver flotation table created")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2b. Process Bronze Flotation Data into Silver

# COMMAND ----------

df_flotation_bronze = spark.table(f"{BRONZE}.raw_flotation_readings")
flotation_count = df_flotation_bronze.count()
print(f"Bronze flotation records: {flotation_count:,}")

if flotation_count > 0:
    # The date column format from the Kaggle dataset: "2017-03-10 01:00:00"
    # Try multiple parse formats
    df_flotation = (
        df_flotation_bronze
        .withColumn(
            "reading_timestamp",
            F.coalesce(
                F.try_to_timestamp("date", F.lit("yyyy-MM-dd HH:mm:ss")),
                F.try_to_timestamp("date", F.lit("dd/MM/yyyy HH:mm")),
                F.try_to_timestamp("date", F.lit("MM/dd/yyyy HH:mm")),
            )
        )
        .filter(F.col("reading_timestamp").isNotNull())
        .withColumn("reading_date", F.to_date("reading_timestamp"))
    )

    # Compute aggregate air flow and column level
    air_flow_cols = [f"flotation_col_0{i}_air_flow" for i in range(1, 8)]
    level_cols = [f"flotation_col_0{i}_level" for i in range(1, 8)]

    df_enriched = (
        df_flotation
        .withColumn("avg_air_flow",
            F.round(
                sum(F.coalesce(F.col(c), F.lit(0)) for c in air_flow_cols) / F.lit(7), 4
            )
        )
        .withColumn("avg_column_level",
            F.round(
                sum(F.coalesce(F.col(c), F.lit(0)) for c in level_cols) / F.lit(7), 4
            )
        )
        .withColumn("silica_above_threshold",
            F.col("silica_concentrate_pct") > 3.0
        )
        .withColumn("quality_score",
            F.when(F.col("silica_concentrate_pct").isNull(), 0.5)
             .when(F.col("silica_concentrate_pct") <= 2.0, 1.0)
             .when(F.col("silica_concentrate_pct") <= 3.0, 0.8)
             .when(F.col("silica_concentrate_pct") <= 4.0, 0.5)
             .otherwise(0.2)
        )
        .withColumn("silver_processed_timestamp", F.current_timestamp())
        .select(
            "reading_timestamp", "reading_date",
            "iron_feed_pct", "silica_feed_pct",
            "starch_flow", "amina_flow",
            "ore_pulp_flow", "ore_pulp_ph", "ore_pulp_density",
            *air_flow_cols, *level_cols,
            "iron_concentrate_pct", "silica_concentrate_pct",
            "avg_air_flow", "avg_column_level",
            "silica_above_threshold", "quality_score",
            "silver_processed_timestamp",
        )
    )

    (
        df_enriched.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(f"{SILVER}.cleaned_flotation_readings")
    )

    silver_flot_count = df_enriched.count()
    print(f"Silver flotation records: {silver_flot_count:,}")
    print(f"Records with silica > 3%: {df_enriched.filter(F.col('silica_above_threshold')).count():,}")
else:
    print("No flotation data in Bronze. Upload the Kaggle CSV and run notebook 02 first.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2c. Build Gold Flotation Quality Scores

# COMMAND ----------

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {GOLD}.flotation_quality_scores (
        score_date DATE NOT NULL,
        score_hour INT,
        avg_iron_feed_pct DOUBLE,
        avg_silica_feed_pct DOUBLE,
        avg_iron_concentrate_pct DOUBLE,
        avg_silica_concentrate_pct DOUBLE,
        min_silica_concentrate_pct DOUBLE,
        max_silica_concentrate_pct DOUBLE,
        silica_breach_count INT COMMENT 'Readings where silica > 3%',
        total_readings INT,
        silica_breach_rate_pct DOUBLE,
        avg_air_flow DOUBLE,
        avg_column_level DOUBLE,
        avg_ore_pulp_ph DOUBLE,
        process_quality_score DOUBLE COMMENT 'Hourly quality 0 to 100',
        calculated_at TIMESTAMP
    )
    USING DELTA
    COMMENT 'Hourly flotation process quality aggregates'
    TBLPROPERTIES ('quality' = 'gold')
""")

# COMMAND ----------

df_silver_flot = spark.table(f"{SILVER}.cleaned_flotation_readings")

if df_silver_flot.count() > 0:
    df_quality = (
        df_silver_flot
        .withColumn("score_hour", F.hour("reading_timestamp"))
        .groupBy("reading_date", "score_hour")
        .agg(
            F.round(F.avg("iron_feed_pct"), 4).alias("avg_iron_feed_pct"),
            F.round(F.avg("silica_feed_pct"), 4).alias("avg_silica_feed_pct"),
            F.round(F.avg("iron_concentrate_pct"), 4).alias("avg_iron_concentrate_pct"),
            F.round(F.avg("silica_concentrate_pct"), 4).alias("avg_silica_concentrate_pct"),
            F.round(F.min("silica_concentrate_pct"), 4).alias("min_silica_concentrate_pct"),
            F.round(F.max("silica_concentrate_pct"), 4).alias("max_silica_concentrate_pct"),
            F.sum(F.when(F.col("silica_above_threshold"), 1).otherwise(0)).alias("silica_breach_count"),
            F.count("*").alias("total_readings"),
            F.round(F.avg("avg_air_flow"), 4).alias("avg_air_flow"),
            F.round(F.avg("avg_column_level"), 4).alias("avg_column_level"),
            F.round(F.avg("ore_pulp_ph"), 4).alias("avg_ore_pulp_ph"),
        )
        .withColumn(
            "silica_breach_rate_pct",
            F.round(F.col("silica_breach_count") / F.col("total_readings") * 100, 2)
        )
        .withColumn(
            "process_quality_score",
            F.round(
                F.lit(100)
                - (F.col("silica_breach_rate_pct") * 0.5)
                - F.greatest(F.lit(0), (F.col("avg_silica_concentrate_pct") - 2.0) * 10),
                2
            )
        )
        .withColumn("score_date", F.col("reading_date"))
        .withColumn("calculated_at", F.current_timestamp())
        .select(
            "score_date", "score_hour",
            "avg_iron_feed_pct", "avg_silica_feed_pct",
            "avg_iron_concentrate_pct", "avg_silica_concentrate_pct",
            "min_silica_concentrate_pct", "max_silica_concentrate_pct",
            "silica_breach_count", "total_readings", "silica_breach_rate_pct",
            "avg_air_flow", "avg_column_level", "avg_ore_pulp_ph",
            "process_quality_score", "calculated_at",
        )
    )

    (
        df_quality.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(f"{GOLD}.flotation_quality_scores")
    )

    print(f"Flotation quality score records: {df_quality.count():,}")
    print("\nProcess quality score distribution:")
    (
        df_quality
        .groupBy(
            F.when(F.col("process_quality_score") >= 90, "Excellent (90+)")
             .when(F.col("process_quality_score") >= 75, "Good (75-90)")
             .when(F.col("process_quality_score") >= 60, "Acceptable (60-75)")
             .otherwise("Poor (< 60)").alias("quality_band")
        )
        .count()
        .orderBy("quality_band")
        .show()
    )
else:
    print("No Silver flotation data. Skipping Gold quality scores.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. AI4I 2020 Predictive Maintenance (Failure Calibration)
# MAGIC
# MAGIC This dataset provides labeled failure types. We use it to:
# MAGIC - Calculate failure type distributions
# MAGIC - Build a failure prediction calibration table
# MAGIC - Validate our health score thresholds against real failure labels

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3a. Create Gold Failure Predictions Table

# COMMAND ----------

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {GOLD}.failure_predictions (
        failure_type STRING NOT NULL,
        failure_count INT,
        total_records INT,
        failure_rate_pct DOUBLE,
        avg_air_temp_k DOUBLE,
        avg_process_temp_k DOUBLE,
        avg_rotational_speed_rpm DOUBLE,
        avg_torque_nm DOUBLE,
        avg_tool_wear_min DOUBLE,
        -- Threshold recommendations
        temp_threshold_k DOUBLE COMMENT 'Recommended temp threshold for this failure type',
        torque_threshold_nm DOUBLE COMMENT 'Recommended torque threshold',
        wear_threshold_min DOUBLE COMMENT 'Recommended tool wear threshold',
        calculated_at TIMESTAMP
    )
    USING DELTA
    COMMENT 'Failure type analysis from AI4I dataset for threshold calibration'
    TBLPROPERTIES ('quality' = 'gold')
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3b. Process Maintenance Labels

# COMMAND ----------

df_maint = spark.table(f"{BRONZE}.raw_maintenance_labels")
maint_count = df_maint.count()
print(f"Bronze maintenance records: {maint_count:,}")

if maint_count > 0:
    total = df_maint.count()
    failure_types = ["twf", "hdf", "pwf", "osf", "rnf"]
    failure_names = {
        "twf": "Tool Wear Failure",
        "hdf": "Heat Dissipation Failure",
        "pwf": "Power Failure",
        "osf": "Overstrain Failure",
        "rnf": "Random Failure",
    }

    rows = []
    for ft in failure_types:
        df_ft = df_maint.filter(F.col(ft) == 1)
        ft_count = df_ft.count()
        if ft_count == 0:
            continue

        stats = df_ft.agg(
            F.avg("air_temperature_k").alias("avg_air_temp"),
            F.avg("process_temperature_k").alias("avg_proc_temp"),
            F.avg("rotational_speed_rpm").alias("avg_rpm"),
            F.avg("torque_nm").alias("avg_torque"),
            F.avg("tool_wear_min").alias("avg_wear"),
            F.expr("percentile_approx(process_temperature_k, 0.90)").alias("p90_temp"),
            F.expr("percentile_approx(torque_nm, 0.90)").alias("p90_torque"),
            F.expr("percentile_approx(tool_wear_min, 0.90)").alias("p90_wear"),
        ).collect()[0]

        rows.append({
            "failure_type": failure_names[ft],
            "failure_count": ft_count,
            "total_records": total,
            "failure_rate_pct": round(ft_count / total * 100, 4),
            "avg_air_temp_k": round(stats["avg_air_temp"], 2) if stats["avg_air_temp"] else None,
            "avg_process_temp_k": round(stats["avg_proc_temp"], 2) if stats["avg_proc_temp"] else None,
            "avg_rotational_speed_rpm": round(stats["avg_rpm"], 2) if stats["avg_rpm"] else None,
            "avg_torque_nm": round(stats["avg_torque"], 2) if stats["avg_torque"] else None,
            "avg_tool_wear_min": round(stats["avg_wear"], 2) if stats["avg_wear"] else None,
            "temp_threshold_k": round(stats["p90_temp"], 2) if stats["p90_temp"] else None,
            "torque_threshold_nm": round(stats["p90_torque"], 2) if stats["p90_torque"] else None,
            "wear_threshold_min": round(stats["p90_wear"], 2) if stats["p90_wear"] else None,
        })

    if rows:
        df_predictions = (
            spark.createDataFrame(rows)
            .withColumn("calculated_at", F.current_timestamp())
        )

        (
            df_predictions.write
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .saveAsTable(f"{GOLD}.failure_predictions")
        )

        print("Failure type analysis:")
        df_predictions.select(
            "failure_type", "failure_count", "failure_rate_pct",
            "temp_threshold_k", "torque_threshold_nm", "wear_threshold_min"
        ).show(truncate=False)
    else:
        print("No failure records found in dataset.")
else:
    print("No maintenance data in Bronze. Upload the AI4I CSV and run notebook 02 first.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. NASA IMS Bearing Vibration (Feature Extraction)
# MAGIC
# MAGIC Bearing vibration data is processed into frequency-domain features.
# MAGIC These features feed into conveyor/crusher bearing health scoring.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4a. Create Silver Bearing Features Table

# COMMAND ----------

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {SILVER}.bearing_features (
        experiment_id INT NOT NULL,
        bearing_id INT NOT NULL,
        measurement_index INT NOT NULL,
        timestamp STRING,
        -- Time-domain features
        rms_amplitude DOUBLE,
        peak_amplitude DOUBLE,
        crest_factor DOUBLE COMMENT 'peak / rms',
        kurtosis DOUBLE COMMENT 'Peakedness of distribution',
        skewness DOUBLE,
        std_deviation DOUBLE,
        -- Frequency-domain features (computed from FFT)
        dominant_frequency_hz DOUBLE,
        spectral_energy DOUBLE,
        -- Health indicator
        bearing_health_index DOUBLE COMMENT '0 to 100, 100 = healthy',
        -- Metadata
        sample_count INT,
        processed_at TIMESTAMP
    )
    USING DELTA
    COMMENT 'Extracted vibration features from NASA IMS bearing dataset'
    TBLPROPERTIES ('quality' = 'silver')
""")

print("Silver bearing features table created")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4b. Extract Features from Raw Vibration Signals

# COMMAND ----------

df_bearing = spark.table(f"{BRONZE}.raw_bearing_vibration")
bearing_count = df_bearing.count()
print(f"Bronze bearing records: {bearing_count:,}")

if bearing_count > 0:
    # Use Spark aggregate functions on the vibration_signal array
    df_features = (
        df_bearing
        .filter(F.size("vibration_signal") > 0)
        .withColumn("sample_count", F.size("vibration_signal"))
        # Time-domain: use aggregate() for array processing
        .withColumn("rms_amplitude",
            F.round(F.sqrt(
                F.aggregate(
                    "vibration_signal",
                    F.lit(0.0),
                    lambda acc, x: acc + F.pow(x, 2)
                ) / F.size("vibration_signal")
            ), 6)
        )
        .withColumn("peak_amplitude",
            F.round(F.array_max(
                F.transform("vibration_signal", lambda x: F.abs(x))
            ), 6)
        )
        .withColumn("crest_factor",
            F.when(F.col("rms_amplitude") > 0,
                F.round(F.col("peak_amplitude") / F.col("rms_amplitude"), 4)
            ).otherwise(0.0)
        )
        .withColumn("std_deviation",
            F.round(F.sqrt(
                F.aggregate(
                    "vibration_signal",
                    F.struct(F.lit(0.0).alias("sum"), F.lit(0.0).alias("sum_sq"), F.lit(0).cast("long").alias("n")),
                    lambda acc, x: F.struct(
                        (acc["sum"] + x).alias("sum"),
                        (acc["sum_sq"] + F.pow(x, 2)).alias("sum_sq"),
                        (acc["n"] + 1).alias("n"),
                    ),
                    lambda acc: (acc["sum_sq"] / acc["n"]) - F.pow(acc["sum"] / acc["n"], 2)
                )
            ), 6)
        )
        # Simplified health index based on RMS and crest factor
        # Higher RMS and crest factor = worse bearing condition
        .withColumn("bearing_health_index",
            F.round(
                F.greatest(
                    F.lit(0.0),
                    F.lit(100.0)
                    - (F.col("rms_amplitude") * 200)  # RMS penalty
                    - F.greatest(F.lit(0.0), (F.col("crest_factor") - 3.0) * 5)  # Crest factor penalty
                ),
                2
            )
        )
        # Placeholder for frequency domain (full FFT needs numpy/scipy)
        .withColumn("dominant_frequency_hz", F.lit(None).cast(DoubleType()))
        .withColumn("spectral_energy", F.lit(None).cast(DoubleType()))
        .withColumn("kurtosis", F.lit(None).cast(DoubleType()))
        .withColumn("skewness", F.lit(None).cast(DoubleType()))
        .withColumn("processed_at", F.current_timestamp())
        .select(
            "experiment_id", "bearing_id", "measurement_index", "timestamp",
            "rms_amplitude", "peak_amplitude", "crest_factor",
            "kurtosis", "skewness", "std_deviation",
            "dominant_frequency_hz", "spectral_energy",
            "bearing_health_index", "sample_count", "processed_at",
        )
    )

    (
        df_features.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(f"{SILVER}.bearing_features")
    )

    feature_count = df_features.count()
    print(f"Bearing feature records: {feature_count:,}")

    print("\nBearing health index by experiment:")
    (
        df_features
        .groupBy("experiment_id", "bearing_id")
        .agg(
            F.round(F.avg("bearing_health_index"), 2).alias("avg_health"),
            F.round(F.min("bearing_health_index"), 2).alias("min_health"),
            F.round(F.avg("rms_amplitude"), 6).alias("avg_rms"),
            F.count("*").alias("measurements"),
        )
        .orderBy("experiment_id", "bearing_id")
        .show(20, truncate=False)
    )
else:
    print("No bearing data in Bronze. Upload the NASA IMS dataset and run notebook 02 first.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Dashboard Queries for External Datasets
# MAGIC
# MAGIC Additional SQL queries you can add to the Databricks dashboard.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Flotation Dashboard Queries

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Silica Concentrate Trend (use as line chart)
# MAGIC SELECT
# MAGIC     score_date,
# MAGIC     score_hour,
# MAGIC     avg_silica_concentrate_pct,
# MAGIC     process_quality_score,
# MAGIC     silica_breach_rate_pct
# MAGIC FROM mining_telemetry.gold.flotation_quality_scores
# MAGIC ORDER BY score_date, score_hour
# MAGIC LIMIT 500;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Flotation Quality Score Distribution
# MAGIC SELECT
# MAGIC     CASE
# MAGIC         WHEN process_quality_score >= 90 THEN 'Excellent (90+)'
# MAGIC         WHEN process_quality_score >= 75 THEN 'Good (75-90)'
# MAGIC         WHEN process_quality_score >= 60 THEN 'Acceptable (60-75)'
# MAGIC         ELSE 'Poor (< 60)'
# MAGIC     END AS quality_band,
# MAGIC     COUNT(*) AS hours
# MAGIC FROM mining_telemetry.gold.flotation_quality_scores
# MAGIC GROUP BY 1
# MAGIC ORDER BY quality_band;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Iron vs Silica Concentrate Scatter
# MAGIC SELECT
# MAGIC     avg_iron_concentrate_pct,
# MAGIC     avg_silica_concentrate_pct,
# MAGIC     process_quality_score
# MAGIC FROM mining_telemetry.gold.flotation_quality_scores
# MAGIC ORDER BY score_date, score_hour
# MAGIC LIMIT 1000;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Failure Prediction Dashboard Queries

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Failure Type Distribution
# MAGIC SELECT
# MAGIC     failure_type,
# MAGIC     failure_count,
# MAGIC     failure_rate_pct,
# MAGIC     temp_threshold_k,
# MAGIC     torque_threshold_nm,
# MAGIC     wear_threshold_min
# MAGIC FROM mining_telemetry.gold.failure_predictions
# MAGIC ORDER BY failure_count DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Integration Summary

# COMMAND ----------

print("=" * 60)
print("EXTERNAL DATASET INTEGRATION SUMMARY")
print("=" * 60)

integration_tables = {
    "Silver": [
        f"{SILVER}.cleaned_flotation_readings",
        f"{SILVER}.bearing_features",
    ],
    "Gold": [
        f"{GOLD}.flotation_quality_scores",
        f"{GOLD}.failure_predictions",
    ],
}

for layer, tables in integration_tables.items():
    print(f"\n--- {layer} ---")
    for table in tables:
        try:
            count = spark.table(table).count()
            name = table.split(".")[-1]
            print(f"  {name:40s} {count:>12,}")
        except Exception:
            name = table.split(".")[-1]
            print(f"  {name:40s} {'empty':>12s}")

print("\nAll external datasets integrated.")
print("Refresh the dashboard to see new visualizations.")
