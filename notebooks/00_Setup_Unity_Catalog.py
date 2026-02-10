# Databricks notebook source
# MAGIC %md
# MAGIC # 00 - Setup Unity Catalog Infrastructure
# MAGIC
# MAGIC Creates the catalog, schemas, volumes, and table definitions required for the
# MAGIC Mining Sensor Telemetry Pipeline (Medallion Architecture).
# MAGIC
# MAGIC Run this once before executing other notebooks.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Configuration

# COMMAND ----------

CATALOG_NAME = "mining_telemetry"
BRONZE_SCHEMA = "bronze"
SILVER_SCHEMA = "silver"
GOLD_SCHEMA = "gold"
RAW_VOLUME = "raw_landing"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Create Catalog

# COMMAND ----------

spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG_NAME}")
spark.sql(f"USE CATALOG {CATALOG_NAME}")

print(f"Catalog '{CATALOG_NAME}' ready")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Create Schemas (Bronze / Silver / Gold)

# COMMAND ----------

for schema in [BRONZE_SCHEMA, SILVER_SCHEMA, GOLD_SCHEMA]:
    spark.sql(f"""
        CREATE SCHEMA IF NOT EXISTS {CATALOG_NAME}.{schema}
        COMMENT '{schema.capitalize()} layer for mining sensor telemetry pipeline'
    """)
    print(f"Schema '{CATALOG_NAME}.{schema}' ready")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Create Volume (Landing Zone for Raw Files)

# COMMAND ----------

spark.sql(f"""
    CREATE VOLUME IF NOT EXISTS {CATALOG_NAME}.{BRONZE_SCHEMA}.{RAW_VOLUME}
    COMMENT 'Landing zone for raw sensor data files (JSON/CSV)'
""")

volume_path = f"/Volumes/{CATALOG_NAME}/{BRONZE_SCHEMA}/{RAW_VOLUME}"
print(f"Volume created at: {volume_path}")

CHECKPOINT_VOLUME = "checkpoints"

spark.sql(f"""
    CREATE VOLUME IF NOT EXISTS {CATALOG_NAME}.{BRONZE_SCHEMA}.{CHECKPOINT_VOLUME}
    COMMENT 'Structured Streaming checkpoints for Bronze ingestion'
""")

checkpoint_path = f"/Volumes/{CATALOG_NAME}/{BRONZE_SCHEMA}/{CHECKPOINT_VOLUME}"
print(f"Checkpoint volume created at: {checkpoint_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Create Equipment Registry Table (Reference Data)
# MAGIC
# MAGIC Master asset table. Silver layer joins against this
# MAGIC to enrich sensor readings with equipment metadata.

# COMMAND ----------

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {CATALOG_NAME}.{SILVER_SCHEMA}.equipment_registry (
        equipment_id STRING NOT NULL COMMENT 'Unique equipment identifier (e.g. HT-001, EX-002)',
        equipment_type STRING NOT NULL COMMENT 'Type: haul_truck, excavator, conveyor, crusher',
        model STRING COMMENT 'Equipment model name',
        manufacturer STRING COMMENT 'OEM manufacturer',
        site_location STRING COMMENT 'Mine site or pit location',
        commissioned_date DATE COMMENT 'Date equipment was commissioned',
        last_maintenance_date DATE COMMENT 'Date of last scheduled maintenance',
        operating_hours DOUBLE COMMENT 'Total operating hours since commissioning',
        status STRING COMMENT 'Current status: active, maintenance, decommissioned',
        max_engine_temp_c DOUBLE COMMENT 'Maximum safe engine temperature in Celsius',
        max_oil_pressure_bar DOUBLE COMMENT 'Maximum safe oil pressure in bar',
        max_payload_tonnes DOUBLE COMMENT 'Maximum rated payload in tonnes'
    )
    USING DELTA
    COMMENT 'Master registry of all mining equipment assets'
    TBLPROPERTIES (
        'delta.enableChangeDataFeed' = 'true',
        'quality' = 'reference'
    )
""")

print("Equipment registry table created")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Create Bronze Tables
# MAGIC
# MAGIC Defining schemas upfront so Auto Loader can enforce them during ingestion.
# MAGIC One table per data source.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6a. Raw Sensor Readings (Synthetic Equipment IoT)
# MAGIC
# MAGIC This is a wide table covering all 4 equipment types.
# MAGIC Each equipment type populates its own subset of columns;
# MAGIC the rest stay null. This keeps ingestion simple while
# MAGIC allowing a single Bronze table for all equipment sensors.

# COMMAND ----------

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {CATALOG_NAME}.{BRONZE_SCHEMA}.raw_sensor_readings (
        equipment_id STRING,
        equipment_type STRING,
        timestamp STRING,
        -- Haul truck sensors
        engine_temp_c DOUBLE,
        oil_pressure_bar DOUBLE,
        tire_pressure_psi DOUBLE,
        fuel_rate_lph DOUBLE,
        payload_tonnes DOUBLE,
        speed_kmh DOUBLE,
        gps_latitude DOUBLE,
        gps_longitude DOUBLE,
        -- Excavator sensors
        hydraulic_pressure_bar DOUBLE,
        boom_angle_deg DOUBLE,
        swing_torque_knm DOUBLE,
        bucket_fill_factor DOUBLE,
        hydraulic_oil_temp_c DOUBLE,
        cycle_time_sec DOUBLE,
        -- Conveyor sensors
        belt_speed_mps DOUBLE,
        motor_current_amps DOUBLE,
        vibration_x_mms DOUBLE,
        vibration_y_mms DOUBLE,
        vibration_z_mms DOUBLE,
        belt_alignment_mm DOUBLE,
        motor_temp_c DOUBLE,
        tonnage_tph DOUBLE,
        -- Crusher sensors
        feed_rate_tph DOUBLE,
        power_draw_kw DOUBLE,
        css_mm DOUBLE,
        crusher_vibration_mms DOUBLE,
        crusher_oil_temp_c DOUBLE,
        -- Common fields
        operating_status STRING,
        engine_hours DOUBLE,
        -- Ingestion metadata
        ingestion_timestamp TIMESTAMP,
        source_file STRING,
        ingestion_date DATE,
        _rescued_data STRING
    )
    USING DELTA
    PARTITIONED BY (ingestion_date)
    COMMENT 'Raw sensor readings from all mining equipment types'
    TBLPROPERTIES (
        'delta.enableChangeDataFeed' = 'true',
        'quality' = 'bronze'
    )
""")

print("Bronze: raw_sensor_readings created")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6b. Raw Flotation Readings (Kaggle Mining Process Dataset)

# COMMAND ----------

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {CATALOG_NAME}.{BRONZE_SCHEMA}.raw_flotation_readings (
        `date` STRING,
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
        -- Ingestion metadata
        ingestion_timestamp TIMESTAMP,
        source_file STRING,
        ingestion_date DATE,
        _rescued_data STRING
    )
    USING DELTA
    PARTITIONED BY (ingestion_date)
    COMMENT 'Raw flotation plant readings from Kaggle mining process dataset'
    TBLPROPERTIES (
        'delta.enableChangeDataFeed' = 'true',
        'quality' = 'bronze'
    )
""")

print("Bronze: raw_flotation_readings created")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6c. Raw Maintenance Labels (AI4I 2020 Dataset)

# COMMAND ----------

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {CATALOG_NAME}.{BRONZE_SCHEMA}.raw_maintenance_labels (
        uid STRING,
        product_id STRING,
        type STRING,
        air_temperature_k DOUBLE,
        process_temperature_k DOUBLE,
        rotational_speed_rpm DOUBLE,
        torque_nm DOUBLE,
        tool_wear_min DOUBLE,
        machine_failure INT,
        twf INT COMMENT 'Tool wear failure',
        hdf INT COMMENT 'Heat dissipation failure',
        pwf INT COMMENT 'Power failure',
        osf INT COMMENT 'Overstrain failure',
        rnf INT COMMENT 'Random failure',
        -- Ingestion metadata
        ingestion_timestamp TIMESTAMP,
        source_file STRING,
        ingestion_date DATE,
        _rescued_data STRING
    )
    USING DELTA
    PARTITIONED BY (ingestion_date)
    COMMENT 'Labeled maintenance data from AI4I 2020 predictive maintenance dataset'
    TBLPROPERTIES (
        'delta.enableChangeDataFeed' = 'true',
        'quality' = 'bronze'
    )
""")

print("Bronze: raw_maintenance_labels created")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6d. Raw Bearing Vibration (NASA IMS Dataset)

# COMMAND ----------

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {CATALOG_NAME}.{BRONZE_SCHEMA}.raw_bearing_vibration (
        experiment_id INT,
        bearing_id INT,
        measurement_index INT,
        timestamp STRING,
        vibration_signal ARRAY<DOUBLE>,
        sample_rate_hz INT,
        -- Ingestion metadata
        ingestion_timestamp TIMESTAMP,
        source_file STRING,
        ingestion_date DATE,
        _rescued_data STRING
    )
    USING DELTA
    PARTITIONED BY (ingestion_date)
    COMMENT 'Raw bearing vibration signals from NASA IMS bearing dataset'
    TBLPROPERTIES (
        'delta.enableChangeDataFeed' = 'true',
        'quality' = 'bronze'
    )
""")

print("Bronze: raw_bearing_vibration created")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Verify Setup

# COMMAND ----------

print("=" * 60)
print("MINING TELEMETRY PIPELINE - INFRASTRUCTURE SUMMARY")
print("=" * 60)

schemas = spark.sql(f"SHOW SCHEMAS IN {CATALOG_NAME}").collect()
print(f"\nCatalog: {CATALOG_NAME}")

for s in schemas:
    schema_name = s['databaseName']
    print(f"  |-- Schema: {schema_name}")
    tables = spark.sql(f"SHOW TABLES IN {CATALOG_NAME}.{schema_name}").collect()
    for t in tables:
        print(f"      |-- Table: {t['tableName']}")

volumes = spark.sql(f"SHOW VOLUMES IN {CATALOG_NAME}.{BRONZE_SCHEMA}").collect()
for v in volumes:
    print(f"  |-- Volume: {v['volume_name']}")

print(f"\nVolume path: /Volumes/{CATALOG_NAME}/{BRONZE_SCHEMA}/{RAW_VOLUME}")
print("\nSetup complete. Proceed to notebook 01_Generate_Synthetic_Data.")