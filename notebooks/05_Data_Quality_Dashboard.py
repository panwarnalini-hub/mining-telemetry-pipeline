# Databricks notebook source
# MAGIC %md
# MAGIC # 05 - Data Quality Dashboard & Pipeline Monitoring
# MAGIC
# MAGIC Provides visibility into pipeline health:
# MAGIC 1. Record counts and freshness across all layers
# MAGIC 2. Data quality metrics (quarantine rate, anomaly rate)
# MAGIC 3. Delta table statistics (file count, size, versions)
# MAGIC 4. Equipment health trend visualization
# MAGIC 5. SLA tracking (processing latency)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Configuration

# COMMAND ----------

from pyspark.sql import functions as F
from datetime import datetime

CATALOG_NAME = "mining_telemetry"
BRONZE = f"{CATALOG_NAME}.bronze"
SILVER = f"{CATALOG_NAME}.silver"
GOLD = f"{CATALOG_NAME}.gold"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Pipeline Record Counts

# COMMAND ----------

print("=" * 70)
print("PIPELINE HEALTH REPORT")
print(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print("=" * 70)

layer_tables = {
    "BRONZE": [
        f"{BRONZE}.raw_sensor_readings",
        f"{BRONZE}.raw_flotation_readings",
        f"{BRONZE}.raw_maintenance_labels",
        f"{BRONZE}.raw_bearing_vibration",
    ],
    "SILVER": [
        f"{SILVER}.cleaned_sensor_readings",
        f"{SILVER}.equipment_registry",
        f"{SILVER}.anomaly_events",
        f"{SILVER}.quarantine_records",
    ],
    "GOLD": [
        f"{GOLD}.hourly_equipment_kpis",
        f"{GOLD}.equipment_health_scores",
        f"{GOLD}.maintenance_alerts",
        f"{GOLD}.fleet_summary_daily",
    ],
}

for layer, tables in layer_tables.items():
    print(f"\n--- {layer} ---")
    for table in tables:
        try:
            count = spark.table(table).count()
            table_short = table.split(".")[-1]
            print(f"  {table_short:40s} {count:>12,}")
        except Exception as e:
            table_short = table.split(".")[-1]
            print(f"  {table_short:40s} {'N/A':>12s}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Data Quality Metrics

# COMMAND ----------

# Bronze to Silver conversion rate
bronze_count = spark.table(f"{BRONZE}.raw_sensor_readings").count()
silver_count = spark.table(f"{SILVER}.cleaned_sensor_readings").count()
quarantine_count = spark.table(f"{SILVER}.quarantine_records").count()

conversion_rate = (silver_count / bronze_count * 100) if bronze_count > 0 else 0
quarantine_rate = (quarantine_count / bronze_count * 100) if bronze_count > 0 else 0

print("DATA QUALITY METRICS")
print("-" * 50)
print(f"Bronze records:          {bronze_count:>12,}")
print(f"Silver records:          {silver_count:>12,}")
print(f"Quarantined records:     {quarantine_count:>12,}")
print(f"Conversion rate:         {conversion_rate:>11.2f}%")
print(f"Quarantine rate:         {quarantine_rate:>11.2f}%")

# COMMAND ----------

# Quality score distribution
print("\nQUALITY SCORE DISTRIBUTION")
print("-" * 50)
(
    spark.table(f"{SILVER}.cleaned_sensor_readings")
    .groupBy(
        F.when(F.col("quality_score") == 1.0, "1.0 (perfect)")
         .when(F.col("quality_score") >= 0.7, "0.7+ (good)")
         .when(F.col("quality_score") >= 0.4, "0.4+ (fair)")
         .otherwise("< 0.4 (poor)").alias("quality_band")
    )
    .agg(
        F.count("*").alias("records"),
        F.round(F.count("*") / F.lit(silver_count) * 100, 2).alias("pct")
    )
    .orderBy("quality_band")
    .show(truncate=False)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Anomaly Summary

# COMMAND ----------

print("ANOMALY SUMMARY")
print("-" * 50)

df_anomalies = spark.table(f"{SILVER}.anomaly_events")
anomaly_total = df_anomalies.count()
print(f"Total anomaly events: {anomaly_total:,}")

print("\nBy severity:")
df_anomalies.groupBy("severity").count().orderBy("severity").show()

print("By equipment (top 10):")
(
    df_anomalies
    .groupBy("equipment_id", "equipment_type")
    .agg(
        F.count("*").alias("total_anomalies"),
        F.sum(F.when(F.col("severity") == "critical", 1).otherwise(0)).alias("critical"),
        F.sum(F.when(F.col("severity") == "warning", 1).otherwise(0)).alias("warning"),
    )
    .orderBy(F.desc("total_anomalies"))
    .limit(10)
    .show(truncate=False)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Equipment Health Overview

# COMMAND ----------

print("EQUIPMENT HEALTH STATUS (Latest Day)")
print("-" * 50)

df_health = spark.table(f"{GOLD}.equipment_health_scores")
latest_date = df_health.agg(F.max("score_date")).collect()[0][0]

(
    df_health
    .filter(F.col("score_date") == latest_date)
    .select(
        "equipment_id", "equipment_type", "health_score",
        "health_status", "health_trend", "days_to_maintenance_estimate",
        "temperature_score", "pressure_score", "vibration_score", "performance_score"
    )
    .orderBy("health_score")
    .show(truncate=False)
)

# COMMAND ----------

# Health trend over time for degrading equipment
print("HEALTH SCORE TREND (Equipment with Degradation)")
print("-" * 50)

degrading_equip = (
    df_health
    .filter(F.col("score_date") == latest_date)
    .filter(F.col("health_status").isin("critical", "warning"))
    .select("equipment_id")
    .collect()
)

degrading_ids = [row["equipment_id"] for row in degrading_equip]

if degrading_ids:
    (
        df_health
        .filter(F.col("equipment_id").isin(degrading_ids))
        .select("equipment_id", "score_date", "health_score", "health_status", "health_trend")
        .orderBy("equipment_id", "score_date")
        .show(100, truncate=False)
    )
else:
    print("No equipment currently in critical or warning status.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Delta Table Statistics

# COMMAND ----------

print("DELTA TABLE STATISTICS")
print("-" * 70)

all_tables = []
for layer, tables in layer_tables.items():
    for t in tables:
        all_tables.append((layer, t))

for layer, table in all_tables:
    try:
        detail = spark.sql(f"DESCRIBE DETAIL {table}").collect()[0]
        history = spark.sql(f"DESCRIBE HISTORY {table} LIMIT 1").collect()[0]
        table_short = table.split(".")[-1]
        
        size_mb = round(detail["sizeInBytes"] / (1024 * 1024), 2)
        num_files = detail["numFiles"]
        version = history["version"]
        last_mod = history["timestamp"]
        
        print(f"  [{layer}] {table_short}")
        print(f"    Size: {size_mb} MB | Files: {num_files} | Version: {version} | Last modified: {last_mod}")
    except Exception as e:
        table_short = table.split(".")[-1]
        print(f"  [{layer}] {table_short}: {str(e)[:60]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Processing Latency (SLA Tracking)

# COMMAND ----------

print("PROCESSING LATENCY")
print("-" * 50)

try:
    df_latency = (
        spark.table(f"{SILVER}.cleaned_sensor_readings")
        .withColumn(
            "processing_lag_seconds",
            F.unix_timestamp("silver_processed_timestamp") -
            F.unix_timestamp("bronze_ingestion_timestamp")
        )
        .agg(
            F.avg("processing_lag_seconds").alias("avg_lag_sec"),
            F.max("processing_lag_seconds").alias("max_lag_sec"),
            F.min("processing_lag_seconds").alias("min_lag_sec"),
            F.percentile_approx("processing_lag_seconds", 0.95).alias("p95_lag_sec"),
        )
    )
    df_latency.show(truncate=False)
except Exception as e:
    print(f"Latency calculation not available: {str(e)[:80]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Maintenance Alerts Summary

# COMMAND ----------

print("ACTIVE MAINTENANCE ALERTS")
print("-" * 50)

(
    spark.table(f"{GOLD}.maintenance_alerts")
    .filter(F.col("acknowledged") == False)
    .orderBy(
        F.when(F.col("severity") == "critical", 1)
         .when(F.col("severity") == "warning", 2)
         .otherwise(3),
        F.desc("alert_timestamp")
    )
    .select(
        "alert_id", "equipment_id", "severity", "alert_type",
        "title", "recommended_action", "health_score"
    )
    .show(20, truncate=False)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Pipeline Lineage Summary

# COMMAND ----------

print("PIPELINE LINEAGE")
print("-" * 50)
print("""
Data Flow:

  [Volume: raw_landing/]
       |
       v
  BRONZE.raw_sensor_readings -----> SILVER.quarantine_records
       |                                (bad records)
       v
  SILVER.cleaned_sensor_readings
       |           |
       |           +----> SILVER.anomaly_events
       v
  GOLD.hourly_equipment_kpis
       |
       v
  GOLD.equipment_health_scores
       |
       v
  GOLD.maintenance_alerts
       |
       v
  GOLD.fleet_summary_daily

  [Parallel paths]
  BRONZE.raw_flotation_readings ----> SILVER.cleaned_flotation_readings
  BRONZE.raw_maintenance_labels ----> GOLD.failure_predictions (calibration)
  BRONZE.raw_bearing_vibration  ----> SILVER.bearing_features
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Recommendations

# COMMAND ----------

# Auto-generate recommendations based on current state
recommendations = []

if quarantine_rate > 1.0:
    recommendations.append(
        f"Quarantine rate is {quarantine_rate:.2f}%. "
        "Review quarantined records and investigate sensor calibration issues."
    )

if degrading_ids:
    recommendations.append(
        f"Equipment {', '.join(degrading_ids)} showing degrading health trends. "
        "Schedule proactive maintenance to prevent unplanned downtime."
    )

critical_alerts = spark.table(f"{GOLD}.maintenance_alerts").filter(
    (F.col("severity") == "critical") & (F.col("acknowledged") == False)
).count()

if critical_alerts > 0:
    recommendations.append(
        f"{critical_alerts} unacknowledged critical alerts. "
        "Immediate attention required."
    )

print("RECOMMENDATIONS")
print("-" * 50)
if recommendations:
    for i, rec in enumerate(recommendations, 1):
        print(f"  {i}. {rec}")
else:
    print("  No immediate action items. Pipeline operating normally.")

print("\n" + "=" * 70)
print("End of pipeline health report.")