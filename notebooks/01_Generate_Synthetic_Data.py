# Databricks notebook source
# MAGIC %md
# MAGIC # 01 - Generate Synthetic Mining Sensor Data
# MAGIC
# MAGIC Generates 30 days of time-series sensor data for 10 equipment units
# MAGIC across 4 types: haul trucks, excavators, conveyors, crushers.
# MAGIC
# MAGIC Each unit has a degradation profile:
# MAGIC - **healthy**: normal operating range, minor noise only
# MAGIC - **gradual**: slow linear degradation (0 to ~0.4 over 30 days)
# MAGIC - **accelerating**: exponential ramp leading to failure on a specific day
# MAGIC
# MAGIC Output is written as JSON files to the Unity Catalog volume
# MAGIC for Bronze layer ingestion. Equipment registry CSV is also generated.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Configuration

# COMMAND ----------

import json
import random
import math
import os
from datetime import datetime, timedelta

CATALOG_NAME = "mining_telemetry"
BRONZE_SCHEMA = "bronze"
SILVER_SCHEMA = "silver"
RAW_VOLUME = "raw_landing"
VOLUME_PATH = f"/Volumes/{CATALOG_NAME}/{BRONZE_SCHEMA}/{RAW_VOLUME}"

NUM_DAYS = 30
READINGS_PER_HOUR = 60  # 1 reading per minute
START_DATE = datetime(2024, 9, 1, 0, 0, 0)
SEED = 42

random.seed(SEED)

# Sub-directories in the volume
SENSOR_DIR = f"{VOLUME_PATH}/sensor_readings"
REGISTRY_DIR = f"{VOLUME_PATH}/equipment_registry"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Equipment Fleet Definition

# COMMAND ----------

EQUIPMENT_FLEET = [
    # Haul Trucks
    {"equipment_id": "HT-001", "equipment_type": "haul_truck", "model": "CAT 797F",
     "manufacturer": "Caterpillar", "site_location": "Pit-A North",
     "commissioned_date": "2020-03-15", "operating_hours": 18500.0,
     "status": "active", "max_engine_temp_c": 110.0, "max_oil_pressure_bar": 7.0,
     "max_payload_tonnes": 400.0,
     "degradation_profile": "gradual", "failure_day": None},

    {"equipment_id": "HT-002", "equipment_type": "haul_truck", "model": "CAT 797F",
     "manufacturer": "Caterpillar", "site_location": "Pit-A South",
     "commissioned_date": "2019-07-20", "operating_hours": 24200.0,
     "status": "active", "max_engine_temp_c": 110.0, "max_oil_pressure_bar": 7.0,
     "max_payload_tonnes": 400.0,
     "degradation_profile": "accelerating", "failure_day": 25},

    {"equipment_id": "HT-003", "equipment_type": "haul_truck", "model": "Komatsu 980E-5",
     "manufacturer": "Komatsu", "site_location": "Pit-B",
     "commissioned_date": "2021-01-10", "operating_hours": 12800.0,
     "status": "active", "max_engine_temp_c": 105.0, "max_oil_pressure_bar": 6.5,
     "max_payload_tonnes": 363.0,
     "degradation_profile": "healthy", "failure_day": None},

    # Excavators
    {"equipment_id": "EX-001", "equipment_type": "excavator", "model": "P&H 4100XPC",
     "manufacturer": "Komatsu", "site_location": "Pit-A North",
     "commissioned_date": "2018-11-05", "operating_hours": 32000.0,
     "status": "active", "max_engine_temp_c": 95.0, "max_oil_pressure_bar": 350.0,
     "max_payload_tonnes": 120.0,
     "degradation_profile": "gradual", "failure_day": None},

    {"equipment_id": "EX-002", "equipment_type": "excavator", "model": "Liebherr R 9800",
     "manufacturer": "Liebherr", "site_location": "Pit-B",
     "commissioned_date": "2020-06-18", "operating_hours": 19500.0,
     "status": "active", "max_engine_temp_c": 100.0, "max_oil_pressure_bar": 380.0,
     "max_payload_tonnes": 110.0,
     "degradation_profile": "healthy", "failure_day": None},

    # Conveyors
    {"equipment_id": "CV-001", "equipment_type": "conveyor", "model": "Overland Belt 2400mm",
     "manufacturer": "Metso", "site_location": "Pit-A to Crusher",
     "commissioned_date": "2019-02-01", "operating_hours": 42000.0,
     "status": "active", "max_engine_temp_c": 80.0, "max_oil_pressure_bar": None,
     "max_payload_tonnes": None,
     "degradation_profile": "accelerating", "failure_day": 22},

    {"equipment_id": "CV-002", "equipment_type": "conveyor", "model": "Overland Belt 1800mm",
     "manufacturer": "Metso", "site_location": "Crusher to Stockpile",
     "commissioned_date": "2019-02-01", "operating_hours": 42000.0,
     "status": "active", "max_engine_temp_c": 80.0, "max_oil_pressure_bar": None,
     "max_payload_tonnes": None,
     "degradation_profile": "healthy", "failure_day": None},

    # Crushers
    {"equipment_id": "CR-001", "equipment_type": "crusher", "model": "Gyratory 60-113",
     "manufacturer": "FLSmidth", "site_location": "Primary Crusher Station",
     "commissioned_date": "2018-05-10", "operating_hours": 38000.0,
     "status": "active", "max_engine_temp_c": 90.0, "max_oil_pressure_bar": 5.0,
     "max_payload_tonnes": None,
     "degradation_profile": "gradual", "failure_day": 28},

    {"equipment_id": "CR-002", "equipment_type": "crusher", "model": "Cone HP800",
     "manufacturer": "Metso", "site_location": "Secondary Crusher Station",
     "commissioned_date": "2020-09-22", "operating_hours": 16500.0,
     "status": "active", "max_engine_temp_c": 85.0, "max_oil_pressure_bar": 4.5,
     "max_payload_tonnes": None,
     "degradation_profile": "healthy", "failure_day": None},
]

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Degradation Profile Function

# COMMAND ----------

def get_degradation_factor(profile, day_index, failure_day, total_days=30):
    """
    Returns a value between 0.0 (healthy) and 1.0 (failed) based on the profile.
    
    - healthy: stays near 0 with minor noise
    - gradual: linear ramp from 0 to ~0.4
    - accelerating: exponential curve peaking at 1.0 on failure_day
    """
    if profile == "healthy":
        return max(0.0, random.gauss(0.02, 0.01))

    elif profile == "gradual":
        base = (day_index / total_days) * 0.4
        return min(0.5, max(0.0, base + random.gauss(0, 0.02)))

    elif profile == "accelerating":
        if failure_day is None:
            failure_day = total_days - 2
        if day_index >= failure_day:
            return min(1.0, 0.85 + random.gauss(0.1, 0.05))
        # Exponential ramp toward failure
        progress = day_index / failure_day
        factor = (math.exp(3 * progress) - 1) / (math.exp(3) - 1)
        return min(0.95, max(0.0, factor + random.gauss(0, 0.02)))

    return 0.0

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Sensor Reading Generators
# MAGIC
# MAGIC Each function produces a dictionary representing one sensor reading.
# MAGIC Physics correlations are modeled:
# MAGIC - Engine temp rises with payload and ambient temperature
# MAGIC - Oil pressure drops with degradation
# MAGIC - Vibration increases with bearing wear
# MAGIC - Cycle times slow down as equipment degrades

# COMMAND ----------

def gen_haul_truck(equip, ts, hour_idx, deg):
    """Haul truck: engine temp, oil pressure, tire pressure, fuel, payload, speed, GPS."""
    hour_of_day = ts.hour
    is_operating = 6 <= hour_of_day <= 23 and random.random() > 0.05

    if not is_operating:
        return {
            "equipment_id": equip["equipment_id"],
            "equipment_type": "haul_truck",
            "timestamp": ts.isoformat(),
            "engine_temp_c": round(25.0 + random.gauss(0, 2), 2),
            "oil_pressure_bar": 0.0,
            "tire_pressure_psi": round(100.0 + random.gauss(0, 0.5), 2),
            "fuel_rate_lph": 0.0,
            "payload_tonnes": 0.0,
            "speed_kmh": 0.0,
            "gps_latitude": round(-23.3615 + random.gauss(0, 0.001), 6),
            "gps_longitude": round(119.7595 + random.gauss(0, 0.001), 6),
            "operating_status": "idle",
            "engine_hours": round(hour_idx * 0.75, 1),
        }

    ambient_offset = 5 * math.sin((hour_of_day - 6) * math.pi / 16)
    is_loaded = random.random() > 0.4
    payload = max(0, min(400, random.gauss(340, 30) if is_loaded else random.gauss(5, 2)))
    speed = max(0, min(65, random.gauss(25, 5) if is_loaded else random.gauss(45, 8)))

    base_temp = 75 + ambient_offset
    load_temp = (payload / 400) * 20
    deg_temp = deg * 15
    engine_temp = base_temp + load_temp + deg_temp + random.gauss(0, 2)

    oil_pressure = max(1.0, min(7.0, 5.5 - (deg * 2.0) + random.gauss(0, 0.3)))
    tire_pressure = 100 - (deg * 15) + random.gauss(0, 1)
    fuel_rate = max(0, 200 + (payload / 400) * 300 + random.gauss(0, 20))

    return {
        "equipment_id": equip["equipment_id"],
        "equipment_type": "haul_truck",
        "timestamp": ts.isoformat(),
        "engine_temp_c": round(engine_temp, 2),
        "oil_pressure_bar": round(oil_pressure, 2),
        "tire_pressure_psi": round(tire_pressure, 2),
        "fuel_rate_lph": round(fuel_rate, 2),
        "payload_tonnes": round(payload, 2),
        "speed_kmh": round(speed, 2),
        "gps_latitude": round(-23.3615 + random.gauss(0, 0.005), 6),
        "gps_longitude": round(119.7595 + random.gauss(0, 0.005), 6),
        "operating_status": "loaded" if is_loaded else "empty",
        "engine_hours": round(hour_idx * 0.75, 1),
    }

# COMMAND ----------

def gen_excavator(equip, ts, hour_idx, deg):
    """Excavator: hydraulic pressure, boom angle, swing torque, bucket fill, oil temp."""
    hour_of_day = ts.hour
    is_operating = 6 <= hour_of_day <= 22 and random.random() > 0.08

    if not is_operating:
        return {
            "equipment_id": equip["equipment_id"],
            "equipment_type": "excavator",
            "timestamp": ts.isoformat(),
            "hydraulic_pressure_bar": round(50.0 + random.gauss(0, 5), 2),
            "boom_angle_deg": 0.0,
            "swing_torque_knm": 0.0,
            "bucket_fill_factor": 0.0,
            "hydraulic_oil_temp_c": round(30.0 + random.gauss(0, 2), 2),
            "cycle_time_sec": 0.0,
            "operating_status": "idle",
            "engine_hours": round(hour_idx * 0.67, 1),
        }

    hydraulic_pressure = min(380, random.gauss(280, 25) + (deg * 30))
    boom_angle = random.uniform(15, 75)
    swing_torque = random.gauss(450, 50)
    bucket_fill = max(0.3, min(1.0, random.gauss(0.85, 0.1)))
    oil_temp = 55 + (deg * 20) + random.gauss(0, 3)
    cycle_time = max(20, random.gauss(32, 4) + (deg * 8))

    return {
        "equipment_id": equip["equipment_id"],
        "equipment_type": "excavator",
        "timestamp": ts.isoformat(),
        "hydraulic_pressure_bar": round(hydraulic_pressure, 2),
        "boom_angle_deg": round(boom_angle, 2),
        "swing_torque_knm": round(swing_torque, 2),
        "bucket_fill_factor": round(bucket_fill, 3),
        "hydraulic_oil_temp_c": round(oil_temp, 2),
        "cycle_time_sec": round(cycle_time, 1),
        "operating_status": "digging",
        "engine_hours": round(hour_idx * 0.67, 1),
    }

# COMMAND ----------

def gen_conveyor(equip, ts, hour_idx, deg):
    """Conveyor: belt speed, motor current, vibration XYZ, alignment, motor temp, tonnage."""
    hour_of_day = ts.hour
    is_operating = 5 <= hour_of_day <= 23 and random.random() > 0.03

    if not is_operating:
        return {
            "equipment_id": equip["equipment_id"],
            "equipment_type": "conveyor",
            "timestamp": ts.isoformat(),
            "belt_speed_mps": 0.0,
            "motor_current_amps": round(5.0 + random.gauss(0, 0.5), 2),
            "vibration_x_mms": round(random.gauss(0.1, 0.02), 3),
            "vibration_y_mms": round(random.gauss(0.1, 0.02), 3),
            "vibration_z_mms": round(random.gauss(0.15, 0.03), 3),
            "belt_alignment_mm": round(random.gauss(0, 0.5), 2),
            "motor_temp_c": round(25.0 + random.gauss(0, 2), 2),
            "tonnage_tph": 0.0,
            "operating_status": "stopped",
            "engine_hours": round(hour_idx * 0.83, 1),
        }

    belt_speed = max(0, 4.5 + random.gauss(0, 0.1) - (deg * 0.5))
    motor_current = 180 + random.gauss(0, 10) + (deg * 40)
    base_vib = 2.5
    vib_x = base_vib + (deg * 8) + random.gauss(0, 0.3)
    vib_y = base_vib + (deg * 6) + random.gauss(0, 0.3)
    vib_z = base_vib * 1.2 + (deg * 10) + random.gauss(0, 0.4)
    alignment = random.gauss(0, 1) + (deg * 5)
    motor_temp = 55 + (deg * 18) + random.gauss(0, 2)
    tonnage = max(0, random.gauss(3500, 200) - (deg * 500))

    return {
        "equipment_id": equip["equipment_id"],
        "equipment_type": "conveyor",
        "timestamp": ts.isoformat(),
        "belt_speed_mps": round(belt_speed, 3),
        "motor_current_amps": round(motor_current, 2),
        "vibration_x_mms": round(vib_x, 3),
        "vibration_y_mms": round(vib_y, 3),
        "vibration_z_mms": round(vib_z, 3),
        "belt_alignment_mm": round(alignment, 2),
        "motor_temp_c": round(motor_temp, 2),
        "tonnage_tph": round(tonnage, 1),
        "operating_status": "running",
        "engine_hours": round(hour_idx * 0.83, 1),
    }

# COMMAND ----------

def gen_crusher(equip, ts, hour_idx, deg):
    """Crusher: feed rate, power draw, CSS, vibration, oil temp."""
    hour_of_day = ts.hour
    is_operating = 6 <= hour_of_day <= 22 and random.random() > 0.06

    if not is_operating:
        return {
            "equipment_id": equip["equipment_id"],
            "equipment_type": "crusher",
            "timestamp": ts.isoformat(),
            "feed_rate_tph": 0.0,
            "power_draw_kw": round(50 + random.gauss(0, 5), 2),
            "css_mm": round(150 + random.gauss(0, 1), 2),
            "crusher_vibration_mms": round(random.gauss(0.5, 0.1), 3),
            "crusher_oil_temp_c": round(30 + random.gauss(0, 2), 2),
            "operating_status": "idle",
            "engine_hours": round(hour_idx * 0.67, 1),
        }

    feed_rate = max(0, random.gauss(5000, 300) - (deg * 800))
    power_draw = 750 + random.gauss(0, 30) + (deg * 150)
    # CSS decreases with liner wear (bad)
    css = max(80, 150 - (deg * 40) + random.gauss(0, 2))
    vibration = 3.0 + (deg * 12) + random.gauss(0, 0.5)
    oil_temp = 55 + (deg * 20) + random.gauss(0, 2)

    # Occasional tramp metal events (spike in vibration and power)
    if random.random() < 0.005:
        vibration += random.uniform(8, 15)
        power_draw += random.uniform(100, 300)

    return {
        "equipment_id": equip["equipment_id"],
        "equipment_type": "crusher",
        "timestamp": ts.isoformat(),
        "feed_rate_tph": round(feed_rate, 1),
        "power_draw_kw": round(power_draw, 2),
        "css_mm": round(css, 2),
        "crusher_vibration_mms": round(vibration, 3),
        "crusher_oil_temp_c": round(oil_temp, 2),
        "operating_status": "crushing",
        "engine_hours": round(hour_idx * 0.67, 1),
    }

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Generate Data and Write to Volume
# MAGIC
# MAGIC Data is written as one JSON file per day per equipment unit.
# MAGIC This simulates how real IoT data would land in batches.

# COMMAND ----------

GENERATORS = {
    "haul_truck": gen_haul_truck,
    "excavator": gen_excavator,
    "conveyor": gen_conveyor,
    "crusher": gen_crusher,
}

# Create output directories
dbutils.fs.mkdirs(SENSOR_DIR)
dbutils.fs.mkdirs(REGISTRY_DIR)

total_records = 0

for equip in EQUIPMENT_FLEET:
    equip_id = equip["equipment_id"]
    equip_type = equip["equipment_type"]
    profile = equip["degradation_profile"]
    failure_day = equip["failure_day"]
    gen_func = GENERATORS[equip_type]

    print(f"Generating data for {equip_id} ({equip_type}, profile={profile})...")

    for day in range(NUM_DAYS):
        day_records = []
        current_date = START_DATE + timedelta(days=day)
        deg = get_degradation_factor(profile, day, failure_day, NUM_DAYS)

        # After failure day, equipment is down (very few readings)
        if failure_day is not None and day > failure_day:
            # Generate 2-3 idle readings to show equipment is down
            for m in range(random.randint(2, 3)):
                ts = current_date + timedelta(hours=random.randint(8, 16), minutes=random.randint(0, 59))
                reading = gen_func(equip, ts, day * 24, 0.0)
                reading["operating_status"] = "down_unplanned"
                day_records.append(reading)
        else:
            for hour in range(24):
                for minute in range(READINGS_PER_HOUR):
                    ts = current_date + timedelta(hours=hour, minutes=minute)
                    hour_idx = day * 24 + hour
                    reading = gen_func(equip, ts, hour_idx, deg)
                    day_records.append(reading)

        # Write day file
        date_str = current_date.strftime("%Y-%m-%d")
        file_path = f"{SENSOR_DIR}/{equip_id}_{date_str}.json"

        json_lines = "\n".join(json.dumps(r) for r in day_records)
        dbutils.fs.put(file_path, json_lines, overwrite=True)
        total_records += len(day_records)

    print(f"  Done: {equip_id}")

print(f"\nTotal sensor records generated: {total_records:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Generate Equipment Registry CSV

# COMMAND ----------

import csv
import io

registry_fields = [
    "equipment_id", "equipment_type", "model", "manufacturer",
    "site_location", "commissioned_date", "operating_hours",
    "status", "max_engine_temp_c", "max_oil_pressure_bar", "max_payload_tonnes"
]

output = io.StringIO()
writer = csv.DictWriter(output, fieldnames=registry_fields)
writer.writeheader()

for equip in EQUIPMENT_FLEET:
    row = {k: equip.get(k, "") for k in registry_fields}
    # Convert None to empty string for CSV
    row = {k: ("" if v is None else v) for k, v in row.items()}
    writer.writerow(row)

csv_content = output.getvalue()
registry_path = f"{REGISTRY_DIR}/equipment_registry.csv"
dbutils.fs.put(registry_path, csv_content, overwrite=True)

print(f"Equipment registry written to: {registry_path}")
print(f"Equipment count: {len(EQUIPMENT_FLEET)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Inject Data Quality Issues (Realistic Noise)
# MAGIC
# MAGIC Real sensor data always has issues. We inject some intentionally
# MAGIC so the Silver layer has something to clean:
# MAGIC - Duplicate readings (sensor retransmits)
# MAGIC - Null values (sensor dropout)
# MAGIC - Out-of-range spikes (electrical interference)
# MAGIC - Malformed timestamps

# COMMAND ----------

import copy

noise_records = []

# Pick 5 random equipment/day combos to inject noise
noise_targets = random.sample(
    [(e["equipment_id"], random.randint(0, NUM_DAYS - 1)) for e in EQUIPMENT_FLEET],
    k=5
)

for equip_id, day in noise_targets:
    date_str = (START_DATE + timedelta(days=day)).strftime("%Y-%m-%d")
    file_path = f"{SENSOR_DIR}/{equip_id}_{date_str}.json"

    try:
        content = dbutils.fs.head(file_path, 100000)
        lines = content.strip().split("\n")
        records = [json.loads(line) for line in lines[:50]]  # work with first 50
    except Exception:
        continue

    injected = []

    for i, rec in enumerate(records[:10]):
        # Duplicate: exact copy
        if i < 3:
            injected.append(json.dumps(rec))

        # Null injection: drop a sensor value
        if 3 <= i < 6:
            noisy = copy.deepcopy(rec)
            keys = [k for k, v in noisy.items() if isinstance(v, (int, float)) and v != 0]
            if keys:
                noisy[random.choice(keys)] = None
            injected.append(json.dumps(noisy))

        # Out-of-range spike
        if 6 <= i < 8:
            noisy = copy.deepcopy(rec)
            if "engine_temp_c" in noisy and noisy.get("engine_temp_c"):
                noisy["engine_temp_c"] = round(random.uniform(200, 500), 2)
            elif "vibration_x_mms" in noisy:
                noisy["vibration_x_mms"] = round(random.uniform(80, 150), 3)
            elif "crusher_vibration_mms" in noisy:
                noisy["crusher_vibration_mms"] = round(random.uniform(60, 100), 3)
            injected.append(json.dumps(noisy))

        # Malformed timestamp
        if 8 <= i < 10:
            noisy = copy.deepcopy(rec)
            noisy["timestamp"] = "INVALID_TS_" + str(random.randint(1000, 9999))
            injected.append(json.dumps(noisy))

    # Append noise to existing file
    if injected:
        noise_content = "\n" + "\n".join(injected)
        # Read existing content and append
        existing = dbutils.fs.head(file_path, 10000000)
        dbutils.fs.put(file_path, existing + noise_content, overwrite=True)
        print(f"  Injected {len(injected)} noisy records into {equip_id} day {day}")

print(f"\nNoise injection complete across {len(noise_targets)} files")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Verify Output

# COMMAND ----------

# Count files
sensor_files = dbutils.fs.ls(SENSOR_DIR)
print(f"Sensor reading files: {len(sensor_files)}")

registry_files = dbutils.fs.ls(REGISTRY_DIR)
print(f"Registry files: {len(registry_files)}")

# Sample a file
sample_file = sensor_files[0].path
sample_content = dbutils.fs.head(sample_file, 2000)
print(f"\nSample from {sensor_files[0].name}:")
print(sample_content[:500])

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Load Equipment Registry into Silver Table
# MAGIC
# MAGIC Since the registry is reference data, we load it directly
# MAGIC into the Silver table created in notebook 00.

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql import functions as F

registry_schema = StructType([
    StructField("equipment_id", StringType(), False),
    StructField("equipment_type", StringType(), False),
    StructField("model", StringType(), True),
    StructField("manufacturer", StringType(), True),
    StructField("site_location", StringType(), True),
    StructField("commissioned_date", StringType(), True),
    StructField("operating_hours", DoubleType(), True),
    StructField("status", StringType(), True),
    StructField("max_engine_temp_c", DoubleType(), True),
    StructField("max_oil_pressure_bar", DoubleType(), True),
    StructField("max_payload_tonnes", DoubleType(), True),
])

df_registry = (
    spark.read
    .option("header", True)
    .schema(registry_schema)
    .csv(f"{REGISTRY_DIR}/equipment_registry.csv")
    .withColumn("commissioned_date", F.to_date("commissioned_date", "yyyy-MM-dd"))
    .withColumn("last_maintenance_date",
                F.date_sub(F.lit(START_DATE.strftime("%Y-%m-%d")).cast("date"), random.randint(10, 90))
))

(
    df_registry.write
    .format("delta")
    .mode("overwrite")
    .option("mergeSchema", "true")
    .saveAsTable(f"{CATALOG_NAME}.{SILVER_SCHEMA}.equipment_registry")
)

print("Equipment registry loaded into Silver table:")
df_registry.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC Generated files in volume:
# MAGIC - `sensor_readings/` : ~300 JSON files (10 equipment x 30 days)
# MAGIC - `equipment_registry/` : 1 CSV file with 10 equipment records
# MAGIC
# MAGIC Includes intentional data quality issues for Silver layer testing:
# MAGIC duplicates, nulls, out-of-range spikes, malformed timestamps.
# MAGIC
# MAGIC Proceed to notebook 02_Bronze_Raw_Ingestion.