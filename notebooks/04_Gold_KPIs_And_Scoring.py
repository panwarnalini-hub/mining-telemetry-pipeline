# Databricks notebook source
# MAGIC %md
# MAGIC # 04 - Gold: KPIs, Health Scores & Maintenance Alerts
# MAGIC
# MAGIC Builds business-ready aggregations from Silver data:
# MAGIC 1. Hourly equipment KPIs (utilization, availability, performance)
# MAGIC 2. Equipment health scores (0 to 100, weighted multi-sensor)
# MAGIC 3. Maintenance alerts with severity classification
# MAGIC 4. Daily fleet summary for dashboard consumption
# MAGIC 5. Failure predictions calibrated against AI4I labels

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Configuration

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql.types import *

CATALOG_NAME = "mining_telemetry"
SILVER = f"{CATALOG_NAME}.silver"
GOLD = f"{CATALOG_NAME}.gold"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Create Gold Tables

# COMMAND ----------

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {GOLD}.hourly_equipment_kpis (
        equipment_id STRING NOT NULL,
        equipment_type STRING NOT NULL,
        kpi_hour TIMESTAMP NOT NULL,
        kpi_date DATE,
        site_location STRING,
        -- Utilization
        total_readings INT,
        operating_readings INT,
        idle_readings INT,
        down_readings INT,
        utilization_pct DOUBLE COMMENT 'Operating / Total x 100',
        -- Performance (type-specific)
        avg_engine_temp_c DOUBLE,
        max_engine_temp_c DOUBLE,
        avg_oil_pressure_bar DOUBLE,
        avg_payload_tonnes DOUBLE,
        total_payload_tonnes DOUBLE,
        avg_speed_kmh DOUBLE,
        avg_hydraulic_pressure_bar DOUBLE,
        avg_cycle_time_sec DOUBLE,
        avg_vibration_rms DOUBLE,
        max_vibration_rms DOUBLE,
        avg_belt_speed_mps DOUBLE,
        avg_feed_rate_tph DOUBLE,
        avg_power_draw_kw DOUBLE,
        avg_css_mm DOUBLE,
        -- Quality
        anomaly_count INT,
        avg_quality_score DOUBLE
    )
    USING DELTA
    PARTITIONED BY (kpi_date)
    COMMENT 'Hourly KPIs per equipment unit'
    TBLPROPERTIES ('quality' = 'gold')
""")

# COMMAND ----------

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {GOLD}.equipment_health_scores (
        equipment_id STRING NOT NULL,
        equipment_type STRING NOT NULL,
        score_date DATE NOT NULL,
        site_location STRING,
        -- Health score components (each 0 to 100)
        temperature_score DOUBLE,
        pressure_score DOUBLE,
        vibration_score DOUBLE,
        performance_score DOUBLE,
        -- Composite
        health_score DOUBLE COMMENT 'Weighted composite 0 to 100',
        health_status STRING COMMENT 'critical, warning, watch, normal',
        -- Trend
        health_score_prev_day DOUBLE,
        health_trend STRING COMMENT 'improving, stable, degrading',
        days_to_maintenance_estimate INT,
        -- Metadata
        calculated_at TIMESTAMP
    )
    USING DELTA
    COMMENT 'Daily equipment health scores for predictive maintenance'
    TBLPROPERTIES ('quality' = 'gold')
""")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS mining_telemetry.gold.maintenance_alerts (
# MAGIC     alert_id STRING NOT NULL,
# MAGIC     equipment_id STRING NOT NULL,
# MAGIC     equipment_type STRING NOT NULL,
# MAGIC     alert_timestamp TIMESTAMP NOT NULL,
# MAGIC     alert_date DATE,
# MAGIC     severity STRING NOT NULL COMMENT 'critical, warning, watch',
# MAGIC     alert_type STRING COMMENT 'health_score, threshold, trend, anomaly_frequency',
# MAGIC     title STRING,
# MAGIC     description STRING,
# MAGIC     recommended_action STRING,
# MAGIC     health_score DOUBLE,
# MAGIC     acknowledged BOOLEAN DEFAULT FALSE,
# MAGIC     created_at TIMESTAMP
# MAGIC )
# MAGIC USING DELTA
# MAGIC PARTITIONED BY (alert_date)
# MAGIC COMMENT 'Maintenance alerts generated from health scores and anomalies'
# MAGIC TBLPROPERTIES (
# MAGIC     'quality' = 'gold',
# MAGIC     'delta.feature.allowColumnDefaults' = 'supported'
# MAGIC );

# COMMAND ----------

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {GOLD}.fleet_summary_daily (
        summary_date DATE NOT NULL,
        site_location STRING,
        total_equipment INT,
        active_equipment INT,
        down_equipment INT,
        fleet_availability_pct DOUBLE,
        avg_fleet_health_score DOUBLE,
        critical_alerts INT,
        warning_alerts INT,
        total_anomalies INT,
        total_payload_tonnes DOUBLE,
        avg_utilization_pct DOUBLE,
        calculated_at TIMESTAMP
    )
    USING DELTA
    COMMENT 'Daily fleet-level summary for executive dashboards'
    TBLPROPERTIES ('quality' = 'gold')
""")

print("Gold tables created")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Hourly Equipment KPIs

# COMMAND ----------

df_silver = spark.table(f"{SILVER}.cleaned_sensor_readings")

# Truncate to hour
df_hourly = (
    df_silver
    .withColumn("kpi_hour", F.date_trunc("hour", "reading_timestamp"))
    .withColumn("kpi_date", F.to_date("reading_timestamp"))
    .withColumn("vibration_rms",
        F.when(
            F.col("equipment_type") == "conveyor",
            F.sqrt(
                F.pow(F.coalesce("vibration_x_mms", F.lit(0)), 2) +
                F.pow(F.coalesce("vibration_y_mms", F.lit(0)), 2) +
                F.pow(F.coalesce("vibration_z_mms", F.lit(0)), 2)
            )
        ).when(
            F.col("equipment_type") == "crusher",
            F.col("crusher_vibration_mms")
        )
    )
)

df_kpis = (
    df_hourly
    .groupBy("equipment_id", "equipment_type", "kpi_hour", "kpi_date", "site_location")
    .agg(
        # Utilization
        F.count("*").alias("total_readings"),
        F.sum(F.when(F.col("operating_status").isin("loaded", "empty", "digging", "running", "crushing"), 1).otherwise(0)).alias("operating_readings"),
        F.sum(F.when(F.col("operating_status") == "idle", 1).otherwise(0)).alias("idle_readings"),
        F.sum(F.when(F.col("operating_status").contains("down"), 1).otherwise(0)).alias("down_readings"),
        # Haul truck
        F.avg("engine_temp_c").alias("avg_engine_temp_c"),
        F.max("engine_temp_c").alias("max_engine_temp_c"),
        F.avg("oil_pressure_bar").alias("avg_oil_pressure_bar"),
        F.avg("payload_tonnes").alias("avg_payload_tonnes"),
        F.sum(F.when(F.col("payload_tonnes") > 10, F.col("payload_tonnes"))).alias("total_payload_tonnes"),
        F.avg("speed_kmh").alias("avg_speed_kmh"),
        # Excavator
        F.avg("hydraulic_pressure_bar").alias("avg_hydraulic_pressure_bar"),
        F.avg("cycle_time_sec").alias("avg_cycle_time_sec"),
        # Conveyor / Crusher vibration
        F.avg("vibration_rms").alias("avg_vibration_rms"),
        F.max("vibration_rms").alias("max_vibration_rms"),
        F.avg("belt_speed_mps").alias("avg_belt_speed_mps"),
        # Crusher
        F.avg("feed_rate_tph").alias("avg_feed_rate_tph"),
        F.avg("power_draw_kw").alias("avg_power_draw_kw"),
        F.avg("css_mm").alias("avg_css_mm"),
        # Quality
        F.sum(F.when(F.col("is_anomaly"), 1).otherwise(0)).alias("anomaly_count"),
        F.avg("quality_score").alias("avg_quality_score"),
    )
    .withColumn(
        "utilization_pct",
        F.round(F.col("operating_readings") / F.col("total_readings") * 100, 2)
    )
)

(
    df_kpis.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{GOLD}.hourly_equipment_kpis")
)

kpi_count = df_kpis.count()
print(f"Hourly KPI records: {kpi_count:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Daily Equipment Health Scores
# MAGIC
# MAGIC Health score is a weighted composite of 4 components:
# MAGIC - Temperature score (25%): how close to max safe temp
# MAGIC - Pressure score (25%): deviation from normal operating range
# MAGIC - Vibration score (25%): vibration levels relative to thresholds
# MAGIC - Performance score (25%): utilization, anomaly frequency

# COMMAND ----------

df_daily = spark.table(f"{GOLD}.hourly_equipment_kpis")

df_daily_agg = (
    df_daily
    .groupBy("equipment_id", "equipment_type", "kpi_date", "site_location")
    .agg(
        F.avg("avg_engine_temp_c").alias("daily_avg_temp"),
        F.max("max_engine_temp_c").alias("daily_max_temp"),
        F.avg("avg_oil_pressure_bar").alias("daily_avg_oil_press"),
        F.avg("avg_hydraulic_pressure_bar").alias("daily_avg_hyd_press"),
        F.avg("avg_vibration_rms").alias("daily_avg_vib"),
        F.max("max_vibration_rms").alias("daily_max_vib"),
        F.avg("utilization_pct").alias("daily_utilization"),
        F.sum("anomaly_count").alias("daily_anomaly_count"),
        F.avg("avg_quality_score").alias("daily_avg_quality"),
    )
)

# Join with registry for max thresholds
df_registry = spark.table(f"{SILVER}.equipment_registry").select(
    "equipment_id",
    F.col("max_engine_temp_c").alias("max_safe_temp"),
    F.col("max_oil_pressure_bar").alias("max_safe_pressure"),
)

df_scored = (
    df_daily_agg
    .join(df_registry, on="equipment_id", how="left")
    .withColumn(
        "temperature_score",
        F.when(F.col("daily_max_temp").isNull(), 100.0)
         .when(F.col("max_safe_temp").isNull(), 80.0)
         .otherwise(
            F.greatest(
                F.lit(0.0),
                F.round((1.0 - F.col("daily_max_temp") / F.col("max_safe_temp")) * 100, 2)
            )
        )
    )
    .withColumn(
        "pressure_score",
        F.when(F.col("daily_avg_oil_press").isNull() & F.col("daily_avg_hyd_press").isNull(), 80.0)
         .when(F.col("daily_avg_oil_press").isNotNull(),
            F.greatest(F.lit(0.0), F.least(F.lit(100.0),
                F.round(F.col("daily_avg_oil_press") / F.lit(5.5) * 100, 2)
            ))
         )
         .otherwise(
            F.greatest(F.lit(0.0), F.least(F.lit(100.0),
                F.round((1.0 - F.abs(F.col("daily_avg_hyd_press") - 280) / 280) * 100, 2)
            ))
         )
    )
    .withColumn(
        "vibration_score",
        F.when(F.col("daily_max_vib").isNull(), 90.0)
         .when(F.col("daily_max_vib") > 15, 10.0)
         .when(F.col("daily_max_vib") > 10, 30.0)
         .when(F.col("daily_max_vib") > 7, 60.0)
         .otherwise(F.round(F.lit(100.0) - F.col("daily_max_vib") * 5, 2))
    )
    .withColumn(
        "performance_score",
        F.round(
            F.col("daily_avg_quality") * 50 +
            F.least(F.col("daily_utilization"), F.lit(100.0)) * 0.3 +
            F.greatest(F.lit(0.0), F.lit(20.0) - F.col("daily_anomaly_count") * 0.5),
            2
        )
    )
    .withColumn(
        "health_score",
        F.round(
            F.col("temperature_score") * 0.25 +
            F.col("pressure_score") * 0.25 +
            F.col("vibration_score") * 0.25 +
            F.col("performance_score") * 0.25,
            2
        )
    )
    .withColumn(
        "health_status",
        F.when(F.col("health_score") < 20, "critical")
         .when(F.col("health_score") < 50, "warning")
         .when(F.col("health_score") < 70, "watch")
         .otherwise("normal")
    )
)

# Calculate trend vs previous day
window_trend = Window.partitionBy("equipment_id").orderBy("kpi_date")

df_with_trend = (
    df_scored
    .withColumn("health_score_prev_day", F.lag("health_score").over(window_trend))
    .withColumn(
        "health_trend",
        F.when(F.col("health_score_prev_day").isNull(), "stable")
         .when(F.col("health_score") - F.col("health_score_prev_day") > 5, "improving")
         .when(F.col("health_score") - F.col("health_score_prev_day") < -5, "degrading")
         .otherwise("stable")
    )
    .withColumn(
        "days_to_maintenance_estimate",
        F.when(F.col("health_trend") == "degrading",
            F.greatest(F.lit(1),
                F.round(F.col("health_score") /
                    F.greatest(F.lit(1.0),
                        F.abs(F.col("health_score") - F.col("health_score_prev_day"))
                    )
                ).cast("int")
            )
        ).otherwise(F.lit(None).cast("int"))
    )
    .withColumn("calculated_at", F.current_timestamp())
    .withColumn("score_date", F.col("kpi_date"))
    .select(
        "equipment_id", "equipment_type", "score_date", "site_location",
        "temperature_score", "pressure_score", "vibration_score", "performance_score",
        "health_score", "health_status",
        "health_score_prev_day", "health_trend", "days_to_maintenance_estimate",
        "calculated_at"
    )
)

(
    df_with_trend.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{GOLD}.equipment_health_scores")
)

health_count = df_with_trend.count()
print(f"Health score records: {health_count:,}")
print("\nHealth status distribution:")
df_with_trend.groupBy("health_status").count().orderBy("health_status").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Maintenance Alerts Generation
# MAGIC
# MAGIC Generate alerts based on:
# MAGIC - Health score dropping below thresholds
# MAGIC - Degrading trend detected
# MAGIC - High anomaly frequency

# COMMAND ----------

df_health = spark.table(f"{GOLD}.equipment_health_scores")

# Health score based alerts
df_health_alerts = (
    df_health
    .filter(F.col("health_status").isin("critical", "warning"))
    .select(
        F.expr("uuid()").alias("alert_id"),
        "equipment_id", "equipment_type",
        F.col("calculated_at").alias("alert_timestamp"),
        F.col("score_date").alias("alert_date"),
        F.col("health_status").alias("severity"),
        F.lit("health_score").alias("alert_type"),
        F.concat(
            F.col("equipment_id"),
            F.lit(" health score: "),
            F.round("health_score", 1)
        ).alias("title"),
        F.concat(
            F.lit("Equipment "), F.col("equipment_id"),
            F.lit(" at "), F.col("site_location"),
            F.lit(" has health score "), F.round("health_score", 1),
            F.lit(" ("), F.col("health_status"), F.lit(")"),
            F.lit(". Trend: "), F.col("health_trend"),
            F.when(F.col("days_to_maintenance_estimate").isNotNull(),
                F.concat(F.lit(". Est. days to failure: "),
                         F.col("days_to_maintenance_estimate").cast("string"))
            ).otherwise(F.lit(""))
        ).alias("description"),
        F.when(F.col("health_status") == "critical",
            F.lit("Immediate inspection required. Consider taking equipment offline.")
        ).otherwise(
            F.lit("Schedule maintenance within the next shift cycle.")
        ).alias("recommended_action"),
        F.col("health_score"),
        F.lit(False).alias("acknowledged"),
        F.current_timestamp().alias("created_at"),
    )
)

# Degrading trend alerts
df_trend_alerts = (
    df_health
    .filter(
        (F.col("health_trend") == "degrading") &
        (F.col("health_status") == "watch")
    )
    .select(
        F.expr("uuid()").alias("alert_id"),
        "equipment_id", "equipment_type",
        F.col("calculated_at").alias("alert_timestamp"),
        F.col("score_date").alias("alert_date"),
        F.lit("watch").alias("severity"),
        F.lit("trend").alias("alert_type"),
        F.concat(
            F.col("equipment_id"),
            F.lit(" degrading trend detected")
        ).alias("title"),
        F.concat(
            F.lit("Equipment "), F.col("equipment_id"),
            F.lit(" health score dropped from "),
            F.round("health_score_prev_day", 1),
            F.lit(" to "), F.round("health_score", 1),
            F.lit(" over 24 hours.")
        ).alias("description"),
        F.lit("Monitor closely. Plan maintenance if trend continues.").alias("recommended_action"),
        F.col("health_score"),
        F.lit(False).alias("acknowledged"),
        F.current_timestamp().alias("created_at"),
    )
)

df_all_alerts = df_health_alerts.unionByName(df_trend_alerts)

(
    df_all_alerts.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{GOLD}.maintenance_alerts")
)

alert_count = df_all_alerts.count()
print(f"Maintenance alerts generated: {alert_count:,}")
print("\nAlerts by severity:")
df_all_alerts.groupBy("severity").count().orderBy("severity").show()
print("Alerts by equipment:")
df_all_alerts.groupBy("equipment_id", "severity").count().orderBy("equipment_id").show(20)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Fleet Summary (Daily)

# COMMAND ----------

df_health = spark.table(f"{GOLD}.equipment_health_scores")
df_alerts = spark.table(f"{GOLD}.maintenance_alerts")
df_kpis = spark.table(f"{GOLD}.hourly_equipment_kpis")
df_anomalies = spark.table(f"{SILVER}.anomaly_events")

# Aggregate health per day/site
df_fleet_health = (
    df_health
    .groupBy("score_date", "site_location")
    .agg(
        F.count("*").alias("total_equipment"),
        F.sum(F.when(F.col("health_status") != "critical", 1).otherwise(0)).alias("active_equipment"),
        F.sum(F.when(F.col("health_status") == "critical", 1).otherwise(0)).alias("down_equipment"),
        F.avg("health_score").alias("avg_fleet_health_score"),
    )
    .withColumn(
        "fleet_availability_pct",
        F.round(F.col("active_equipment") / F.col("total_equipment") * 100, 2)
    )
)

# Alert counts per day
df_alert_counts = (
    df_alerts
    .groupBy(F.col("alert_date").alias("a_date"))
    .agg(
        F.sum(F.when(F.col("severity") == "critical", 1).otherwise(0)).alias("critical_alerts"),
        F.sum(F.when(F.col("severity") == "warning", 1).otherwise(0)).alias("warning_alerts"),
    )
)

# KPI aggregates
df_kpi_daily = (
    df_kpis
    .groupBy("kpi_date")
    .agg(
        F.sum("total_payload_tonnes").alias("total_payload_tonnes"),
        F.avg("utilization_pct").alias("avg_utilization_pct"),
        F.sum("anomaly_count").alias("total_anomalies"),
    )
)

# Join everything
df_fleet = (
    df_fleet_health
    .join(df_alert_counts, df_fleet_health["score_date"] == df_alert_counts["a_date"], "left")
    .join(df_kpi_daily, df_fleet_health["score_date"] == df_kpi_daily["kpi_date"], "left")
    .select(
        F.col("score_date").alias("summary_date"),
        "site_location",
        "total_equipment", "active_equipment", "down_equipment",
        "fleet_availability_pct",
        F.round("avg_fleet_health_score", 2).alias("avg_fleet_health_score"),
        F.coalesce("critical_alerts", F.lit(0)).alias("critical_alerts"),
        F.coalesce("warning_alerts", F.lit(0)).alias("warning_alerts"),
        F.coalesce("total_anomalies", F.lit(0)).cast("int").alias("total_anomalies"),
        F.round("total_payload_tonnes", 1).alias("total_payload_tonnes"),
        F.round("avg_utilization_pct", 2).alias("avg_utilization_pct"),
        F.current_timestamp().alias("calculated_at"),
    )
)

(
    df_fleet.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{GOLD}.fleet_summary_daily")
)

print("Fleet summary:")
df_fleet.orderBy("summary_date").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Gold Layer Summary

# COMMAND ----------

print("=" * 60)
print("GOLD LAYER PROCESSING SUMMARY")
print("=" * 60)

gold_tables = [
    "hourly_equipment_kpis",
    "equipment_health_scores",
    "maintenance_alerts",
    "fleet_summary_daily",
]

for table in gold_tables:
    try:
        count = spark.table(f"{GOLD}.{table}").count()
        print(f"  {table}: {count:,} records")
    except Exception:
        print(f"  {table}: empty")

# Show critical equipment
print("\nEquipment requiring immediate attention:")
(
    spark.table(f"{GOLD}.equipment_health_scores")
    .filter(F.col("health_status").isin("critical", "warning"))
    .orderBy("health_score")
    .select("equipment_id", "equipment_type", "score_date",
            "health_score", "health_status", "health_trend",
            "days_to_maintenance_estimate")
    .show(20, truncate=False)
)

print("\nProceed to notebook 05_Data_Quality_Dashboard.")