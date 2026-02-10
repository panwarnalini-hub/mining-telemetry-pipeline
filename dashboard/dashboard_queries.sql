-- ============================================================
-- Mining Fleet Health & Operations Dashboard
-- SQL Queries for Databricks SQL Dashboard
-- ============================================================
-- 
-- How to use:
-- 1. Open Databricks SQL > Dashboards > Create Dashboard
-- 2. Add a widget for each query below
-- 3. Set the Global Filter on score_date (already done)
-- 4. Suggested layout is noted in comments per query
--
-- Dashboard pages:
--   Page 1: Fleet Overview
--   Page 2: Equipment Drilldown
--   Page 3: Anomalies & Alerts
--   Page 4: Data Quality
-- ============================================================


-- ============================================================
-- PAGE 1: FLEET OVERVIEW
-- ============================================================

-- [1.1] Counter: Total Equipment
-- Widget type: Counter
SELECT COUNT(DISTINCT equipment_id) AS total_equipment
FROM mining_telemetry.gold.equipment_health_scores
WHERE score_date = (SELECT MAX(score_date) FROM mining_telemetry.gold.equipment_health_scores);


-- [1.2] Counter: Fleet Availability %
-- Widget type: Counter
SELECT ROUND(
    SUM(CASE WHEN health_status != 'critical' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1
) AS fleet_availability_pct
FROM mining_telemetry.gold.equipment_health_scores
WHERE score_date = (SELECT MAX(score_date) FROM mining_telemetry.gold.equipment_health_scores);


-- [1.3] Counter: Avg Fleet Health Score
-- Widget type: Counter
SELECT ROUND(AVG(health_score), 1) AS avg_health_score
FROM mining_telemetry.gold.equipment_health_scores
WHERE score_date = (SELECT MAX(score_date) FROM mining_telemetry.gold.equipment_health_scores);


-- [1.4] Counter: Active Critical Alerts
-- Widget type: Counter (red if > 0)
SELECT COUNT(*) AS critical_alerts
FROM mining_telemetry.gold.maintenance_alerts
WHERE severity = 'critical'
  AND acknowledged = FALSE;


-- [1.5] Fleet Health Status Distribution
-- Widget type: Bar chart (already built)
-- X: health_status, Y: equipment_count
SELECT
    health_status,
    COUNT(*) AS equipment_count
FROM mining_telemetry.gold.equipment_health_scores
WHERE score_date = (SELECT MAX(score_date) FROM mining_telemetry.gold.equipment_health_scores)
GROUP BY health_status
ORDER BY
    CASE health_status
        WHEN 'critical' THEN 1
        WHEN 'warning' THEN 2
        WHEN 'watch' THEN 3
        WHEN 'normal' THEN 4
    END;


-- [1.6] Fleet Health Score Trend (Daily Average)
-- Widget type: Line chart (already built)
-- X: score_date, Y: avg_health_score
SELECT
    score_date,
    ROUND(AVG(health_score), 2) AS avg_health_score,
    ROUND(MIN(health_score), 2) AS min_health_score,
    ROUND(MAX(health_score), 2) AS max_health_score
FROM mining_telemetry.gold.equipment_health_scores
GROUP BY score_date
ORDER BY score_date;


-- [1.7] Fleet Utilization by Equipment Type (Daily)
-- Widget type: Stacked bar chart
-- X: kpi_date, Color: equipment_type, Y: avg_utilization
SELECT
    kpi_date,
    equipment_type,
    ROUND(AVG(utilization_pct), 2) AS avg_utilization_pct
FROM mining_telemetry.gold.hourly_equipment_kpis
GROUP BY kpi_date, equipment_type
ORDER BY kpi_date, equipment_type;


-- [1.8] Daily Anomaly Count Trend
-- Widget type: Area chart
-- X: event_date, Y: anomaly_count, Color: severity
SELECT
    DATE(event_timestamp) AS event_date,
    severity,
    COUNT(*) AS anomaly_count
FROM mining_telemetry.silver.anomaly_events
GROUP BY DATE(event_timestamp), severity
ORDER BY event_date, severity;


-- [1.9] Total Payload Moved (Daily, Haul Trucks)
-- Widget type: Bar chart
-- X: kpi_date, Y: total_payload
SELECT
    kpi_date,
    ROUND(SUM(total_payload_tonnes), 0) AS total_payload_tonnes
FROM mining_telemetry.gold.hourly_equipment_kpis
WHERE equipment_type = 'haul_truck'
GROUP BY kpi_date
ORDER BY kpi_date;


-- ============================================================
-- PAGE 2: EQUIPMENT DRILLDOWN
-- ============================================================

-- [2.1] Equipment Health Scorecard (Latest Day)
-- Widget type: Table
-- Columns: equipment_id, type, health_score, status, trend, days_to_maintenance
SELECT
    equipment_id,
    equipment_type,
    health_score,
    health_status,
    temperature_score,
    pressure_score,
    vibration_score,
    performance_score,
    health_trend,
    days_to_maintenance_estimate
FROM mining_telemetry.gold.equipment_health_scores
WHERE score_date = (SELECT MAX(score_date) FROM mining_telemetry.gold.equipment_health_scores)
ORDER BY health_score ASC;


-- [2.2] Equipment Health Score Over Time (Per Unit)
-- Widget type: Line chart with equipment_id filter
-- X: score_date, Y: health_score, Color: equipment_id
SELECT
    score_date,
    equipment_id,
    health_score
FROM mining_telemetry.gold.equipment_health_scores
ORDER BY equipment_id, score_date;


-- [2.3] Health Score Components Breakdown (Latest Day)
-- Widget type: Stacked bar or grouped bar
-- X: equipment_id, Y: score, Color: component
SELECT
    equipment_id,
    'Temperature' AS component,
    temperature_score AS score
FROM mining_telemetry.gold.equipment_health_scores
WHERE score_date = (SELECT MAX(score_date) FROM mining_telemetry.gold.equipment_health_scores)
UNION ALL
SELECT equipment_id, 'Pressure', pressure_score
FROM mining_telemetry.gold.equipment_health_scores
WHERE score_date = (SELECT MAX(score_date) FROM mining_telemetry.gold.equipment_health_scores)
UNION ALL
SELECT equipment_id, 'Vibration', vibration_score
FROM mining_telemetry.gold.equipment_health_scores
WHERE score_date = (SELECT MAX(score_date) FROM mining_telemetry.gold.equipment_health_scores)
UNION ALL
SELECT equipment_id, 'Performance', performance_score
FROM mining_telemetry.gold.equipment_health_scores
WHERE score_date = (SELECT MAX(score_date) FROM mining_telemetry.gold.equipment_health_scores)
ORDER BY equipment_id, component;


-- [2.4] Haul Truck Engine Temp Trend (Hourly)
-- Widget type: Line chart with equipment_id filter
-- X: kpi_hour, Y: avg/max temp
SELECT
    kpi_hour,
    equipment_id,
    ROUND(avg_engine_temp_c, 2) AS avg_engine_temp,
    ROUND(max_engine_temp_c, 2) AS max_engine_temp
FROM mining_telemetry.gold.hourly_equipment_kpis
WHERE equipment_type = 'haul_truck'
  AND avg_engine_temp_c IS NOT NULL
ORDER BY equipment_id, kpi_hour;


-- [2.5] Conveyor Vibration RMS Trend (Hourly)
-- Widget type: Line chart
-- Shows degradation pattern for CV-001
SELECT
    kpi_hour,
    equipment_id,
    ROUND(avg_vibration_rms, 3) AS avg_vibration_rms,
    ROUND(max_vibration_rms, 3) AS max_vibration_rms
FROM mining_telemetry.gold.hourly_equipment_kpis
WHERE equipment_type = 'conveyor'
  AND avg_vibration_rms IS NOT NULL
ORDER BY equipment_id, kpi_hour;


-- [2.6] Crusher CSS and Power Draw Correlation
-- Widget type: Scatter plot
-- X: avg_css_mm, Y: avg_power_draw_kw, Color: equipment_id
SELECT
    equipment_id,
    kpi_hour,
    ROUND(avg_css_mm, 2) AS avg_css_mm,
    ROUND(avg_power_draw_kw, 2) AS avg_power_draw_kw,
    ROUND(avg_feed_rate_tph, 1) AS avg_feed_rate_tph
FROM mining_telemetry.gold.hourly_equipment_kpis
WHERE equipment_type = 'crusher'
  AND avg_css_mm IS NOT NULL
ORDER BY kpi_hour;


-- [2.7] Excavator Cycle Time Trend
-- Widget type: Line chart
-- Slower cycles indicate degradation
SELECT
    kpi_hour,
    equipment_id,
    ROUND(avg_cycle_time_sec, 1) AS avg_cycle_time_sec,
    ROUND(avg_hydraulic_pressure_bar, 1) AS avg_hydraulic_pressure_bar
FROM mining_telemetry.gold.hourly_equipment_kpis
WHERE equipment_type = 'excavator'
  AND avg_cycle_time_sec IS NOT NULL
ORDER BY equipment_id, kpi_hour;


-- ============================================================
-- PAGE 3: ANOMALIES & ALERTS
-- ============================================================

-- [3.1] Active Alerts Table
-- Widget type: Table (sortable, filterable)
SELECT
    alert_id,
    equipment_id,
    equipment_type,
    severity,
    alert_type,
    title,
    description,
    recommended_action,
    ROUND(health_score, 1) AS health_score,
    alert_timestamp
FROM mining_telemetry.gold.maintenance_alerts
WHERE acknowledged = FALSE
ORDER BY
    CASE severity
        WHEN 'critical' THEN 1
        WHEN 'warning' THEN 2
        WHEN 'watch' THEN 3
    END,
    alert_timestamp DESC;


-- [3.2] Anomaly Events by Equipment and Severity
-- Widget type: Heatmap or grouped bar
-- X: equipment_id, Y: count, Color: severity
SELECT
    equipment_id,
    severity,
    COUNT(*) AS anomaly_count
FROM mining_telemetry.silver.anomaly_events
GROUP BY equipment_id, severity
ORDER BY equipment_id, severity;


-- [3.3] Anomaly Events by Sensor Type
-- Widget type: Bar chart
-- Which sensors trigger the most anomalies
SELECT
    sensor_name,
    COUNT(*) AS event_count,
    ROUND(AVG(sensor_value), 2) AS avg_sensor_value,
    ROUND(AVG(threshold_value), 2) AS threshold
FROM mining_telemetry.silver.anomaly_events
GROUP BY sensor_name
ORDER BY event_count DESC;


-- [3.4] Anomaly Timeline (Daily by Equipment)
-- Widget type: Heatmap
-- X: date, Y: equipment_id, Value: anomaly_count
SELECT
    DATE(event_timestamp) AS event_date,
    equipment_id,
    COUNT(*) AS anomaly_count
FROM mining_telemetry.silver.anomaly_events
GROUP BY DATE(event_timestamp), equipment_id
ORDER BY event_date, equipment_id;


-- [3.5] Alert Frequency by Equipment
-- Widget type: Bar chart
SELECT
    equipment_id,
    alert_type,
    COUNT(*) AS alert_count
FROM mining_telemetry.gold.maintenance_alerts
GROUP BY equipment_id, alert_type
ORDER BY alert_count DESC;


-- ============================================================
-- PAGE 4: DATA QUALITY
-- ============================================================

-- [4.1] Pipeline Record Counts by Layer
-- Widget type: Table or counter row
SELECT 'Bronze - Sensor Readings' AS table_name, COUNT(*) AS records
FROM mining_telemetry.bronze.raw_sensor_readings
UNION ALL
SELECT 'Silver - Cleaned Readings', COUNT(*)
FROM mining_telemetry.silver.cleaned_sensor_readings
UNION ALL
SELECT 'Silver - Quarantine', COUNT(*)
FROM mining_telemetry.silver.quarantine_records
UNION ALL
SELECT 'Silver - Anomaly Events', COUNT(*)
FROM mining_telemetry.silver.anomaly_events
UNION ALL
SELECT 'Gold - Hourly KPIs', COUNT(*)
FROM mining_telemetry.gold.hourly_equipment_kpis
UNION ALL
SELECT 'Gold - Health Scores', COUNT(*)
FROM mining_telemetry.gold.equipment_health_scores
UNION ALL
SELECT 'Gold - Alerts', COUNT(*)
FROM mining_telemetry.gold.maintenance_alerts;


-- [4.2] Bronze to Silver Conversion Funnel
-- Widget type: Funnel or bar chart
SELECT
    'Bronze Input' AS stage,
    COUNT(*) AS records
FROM mining_telemetry.bronze.raw_sensor_readings
UNION ALL
SELECT 'Silver Output', COUNT(*)
FROM mining_telemetry.silver.cleaned_sensor_readings
UNION ALL
SELECT 'Quarantined', COUNT(*)
FROM mining_telemetry.silver.quarantine_records;


-- [4.3] Quality Score Distribution
-- Widget type: Pie chart or bar chart
SELECT
    CASE
        WHEN quality_score = 1.0 THEN 'Perfect (1.0)'
        WHEN quality_score >= 0.7 THEN 'Good (0.7+)'
        WHEN quality_score >= 0.4 THEN 'Fair (0.4+)'
        ELSE 'Poor (< 0.4)'
    END AS quality_band,
    COUNT(*) AS records,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS pct
FROM mining_telemetry.silver.cleaned_sensor_readings
GROUP BY
    CASE
        WHEN quality_score = 1.0 THEN 'Perfect (1.0)'
        WHEN quality_score >= 0.7 THEN 'Good (0.7+)'
        WHEN quality_score >= 0.4 THEN 'Fair (0.4+)'
        ELSE 'Poor (< 0.4)'
    END
ORDER BY quality_band;


-- [4.4] Quarantine Reasons Breakdown
-- Widget type: Bar chart
SELECT
    quarantine_reason,
    COUNT(*) AS records
FROM mining_telemetry.silver.quarantine_records
GROUP BY quarantine_reason
ORDER BY records DESC;


-- [4.5] Data Freshness (Latest Record per Table)
-- Widget type: Table
SELECT
    'raw_sensor_readings' AS table_name,
    MAX(ingestion_timestamp) AS latest_record,
    TIMESTAMPDIFF(MINUTE, MAX(ingestion_timestamp), CURRENT_TIMESTAMP()) AS minutes_behind
FROM mining_telemetry.bronze.raw_sensor_readings
UNION ALL
SELECT
    'cleaned_sensor_readings',
    MAX(silver_processed_timestamp),
    TIMESTAMPDIFF(MINUTE, MAX(silver_processed_timestamp), CURRENT_TIMESTAMP())
FROM mining_telemetry.silver.cleaned_sensor_readings
UNION ALL
SELECT
    'equipment_health_scores',
    MAX(calculated_at),
    TIMESTAMPDIFF(MINUTE, MAX(calculated_at), CURRENT_TIMESTAMP())
FROM mining_telemetry.gold.equipment_health_scores;


-- [4.6] Records Ingested by Equipment Type
-- Widget type: Pie chart
SELECT
    equipment_type,
    COUNT(*) AS record_count
FROM mining_telemetry.silver.cleaned_sensor_readings
GROUP BY equipment_type
ORDER BY record_count DESC;


-- [4.7] Daily Ingestion Volume
-- Widget type: Bar chart
-- Shows how many records were ingested per day
SELECT
    ingestion_date,
    COUNT(*) AS records_ingested
FROM mining_telemetry.bronze.raw_sensor_readings
GROUP BY ingestion_date
ORDER BY ingestion_date;
