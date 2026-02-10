# Mining Sensor Telemetry Pipeline

End-to-end data engineering pipeline for mining equipment monitoring, built on Databricks with Unity Catalog, Delta Lake, and medallion architecture (Bronze/Silver/Gold).

Processes sensor telemetry from 10 equipment units across 4 types (haul trucks, excavators, conveyors, crushers), integrates real-world Kaggle datasets, and powers a 5-page operational dashboard.

## Architecture

```
  Raw Files (JSON/CSV)
        |
        v
  [Volume: raw_landing/]
        |
        v
  BRONZE ---- Auto Loader, schema enforcement, append-only
        |          raw_sensor_readings (371K+ synthetic)
        |          raw_flotation_readings (737K Kaggle)
        |          raw_maintenance_labels (10K AI4I)
        |          raw_bearing_vibration (NASA IMS)
        |
        v
  SILVER ---- Cleaned, deduplicated, enriched, anomaly-flagged
        |          cleaned_sensor_readings
        |          cleaned_flotation_readings
        |          equipment_registry
        |          anomaly_events
        |          quarantine_records
        |          bearing_features
        |
        v
  GOLD ------ Business KPIs, health scores, alerts
                 hourly_equipment_kpis
                 equipment_health_scores
                 maintenance_alerts
                 fleet_summary_daily
                 flotation_quality_scores
                 failure_predictions
```

## Pipeline Features

**Bronze Layer**
- Auto Loader (cloudFiles) with incremental file processing
- Explicit schema enforcement with rescued data column
- Streaming checkpoints stored in Unity Catalog Volumes (not /tmp)
- Source file tracking via `_metadata.file_path`
- Handles 4 data sources with graceful skip for missing datasets

**Silver Layer**
- `try_to_timestamp` for resilient timestamp parsing (bad data becomes NULL, not a failed job)
- Physics-based range validation with hard/soft limits per sensor
- Quarantine table for bad records (auditable, not deleted)
- Deduplication on (equipment_id, timestamp) via Delta MERGE
- Quality score per record (0.0 to 1.0)
- Anomaly detection: threshold-based for engine temp, vibration RMS, hydraulic oil temp, crusher vibration
- Equipment registry enrichment via broadcast join
- Delta constraints for NOT NULL and value range enforcement

**Gold Layer**
- Hourly equipment KPIs: utilization, payload, vibration, cycle time
- Weighted health scores (0 to 100): temperature, pressure, vibration, performance components
- Health trend detection with days-to-failure estimates
- Maintenance alerts with severity classification (critical/warning/watch)
- Fleet summary for executive dashboards
- Flotation process quality scoring with silica breach tracking
- Failure type analysis from AI4I labels with P90 threshold calibration

## Datasets

| Dataset | Source | Records | Purpose |
|---------|--------|---------|---------|
| Synthetic Equipment IoT | Generated (notebook 01) | ~371K | 30 days, 10 units, 3 degradation profiles |
| Mining Process Flotation | [Kaggle](https://www.kaggle.com/datasets/edumagalhaes/quality-prediction-in-a-mining-process) | 737K | Real flotation plant sensor data |
| AI4I 2020 Predictive Maintenance | [Kaggle](https://www.kaggle.com/datasets/stephanmatzka/predictive-maintenance-dataset-ai4i-2020) | 10K | Labeled failure types for threshold calibration |
| NASA IMS Bearing | [NASA](https://data.nasa.gov/dataset/ims-bearings) | Variable | Run-to-failure vibration signals |

## Dashboard

5-page Databricks SQL Dashboard built on Gold/Silver tables:

- **Fleet Overview**: 6 KPI counters, health trend (avg/min/max), utilization by type, anomaly severity, daily payload
- **Equipment Drilldown**: health scorecard, per-unit trends, component breakdown, engine temp, vibration RMS, CSS vs power draw, cycle time
- **Anomalies & Alerts**: active alerts table, anomaly heatmap (daily x equipment), sensor type distribution, alert frequency
- **Data Quality**: pipeline record counts, Bronze-to-Silver funnel, quality score distribution, quarantine reasons, data freshness
- **Flotation & Failures**: silica concentrate trend, flotation quality distribution, failure type analysis from AI4I

## Equipment Fleet

| ID | Type | Model | Degradation Profile |
|----|------|-------|-------------------|
| HT-001 | Haul Truck | CAT 797F | Gradual |
| HT-002 | Haul Truck | CAT 797F | Accelerating (fails day 25) |
| HT-003 | Haul Truck | Komatsu 980E-5 | Healthy |
| EX-001 | Excavator | P&H 4100XPC | Gradual |
| EX-002 | Excavator | Liebherr R 9800 | Healthy |
| CV-001 | Conveyor | Metso 2400mm | Accelerating (fails day 22) |
| CV-002 | Conveyor | Metso 1800mm | Healthy |
| CR-001 | Crusher | FLSmidth Gyratory | Gradual (fails day 28) |
| CR-002 | Crusher | Metso HP800 | Healthy |

## Notebooks

Run in order:

| # | Notebook | Purpose |
|---|----------|---------|
| 00 | Setup Unity Catalog | Catalog, schemas, volumes, table definitions |
| 01 | Generate Synthetic Data | 30 days of equipment IoT data with degradation profiles |
| 02 | Bronze Raw Ingestion | Auto Loader for synthetic JSON + batch for Kaggle/NASA CSVs |
| 03 | Silver Clean Enrich | Validation, quarantine, dedup, enrichment, anomaly detection |
| 04 | Gold KPIs And Scoring | Health scores, maintenance alerts, fleet summary |
| 05 | Data Quality Dashboard | Pipeline health report, lineage, recommendations |
| 06 | Integrate External Datasets | Kaggle flotation + AI4I maintenance processing |

## Key Design Decisions

1. **Streaming checkpoints in Volumes, not /tmp**: /tmp is ephemeral on Databricks. Volumes give governance, persistence, and predictable recovery.

2. **try_to_timestamp over to_timestamp**: Bad timestamps become NULL instead of killing the entire job. Silver pipelines tolerate bad input.

3. **Quarantine table, not silent drops**: Bad records are observable and auditable. No silent data corruption.

4. **Physics-based validation with hard/soft limits**: Hard violations quarantine, soft violations flag. Each record gets an explainable quality score.

5. **inferSchema disabled for external CSVs**: Kaggle flotation CSV uses Brazilian decimal format (commas). AI4I has mixed types. Explicit casting prevents Delta merge conflicts.

6. **No premature Z-ORDER**: Removed in favor of partitioning by date and Databricks automatic optimizations. Optimization deferred until query patterns justify it.

7. **Delta feature opt-in for Gold tables**: DEFAULT values and column defaults enabled explicitly. Gold tables behave like APIs with contracts.

## Tech Stack

- Databricks (Community Edition / Serverless)
- Unity Catalog
- Delta Lake with CDF
- PySpark / Spark SQL
- Auto Loader (Structured Streaming)
- Databricks SQL Dashboard

## Setup

1. Import all notebooks into a Databricks workspace
2. Run notebook 00 to create infrastructure
3. Run notebook 01 to generate synthetic data
4. Run notebook 02 to ingest into Bronze
5. Run notebook 03 to process Silver
6. Run notebook 04 to build Gold
7. (Optional) Upload Kaggle/NASA datasets to volume, re-run 02, then run 06
8. Create dashboard using queries from `dashboard/dashboard_queries.sql`
