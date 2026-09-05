import json
import logging
import time

import psycopg
from psycopg.rows import dict_row
import paho.mqtt.client as mqtt


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger("teslamate_trip_metrics")

with open("/data/options.json", encoding="utf-8") as file:
    config = json.load(file)

CAR_ID = int(config.get("car_id", 1))
INTERVAL = max(int(config.get("interval_seconds", 30)), 30)
DISCOVERY_PREFIX = config.get("mqtt_discovery_prefix", "homeassistant")
MQTT_BASE = "teslamate_trip_metrics"


DRIVING_QUERY = """
SELECT ROUND(
  COALESCE(SUM(
    GREATEST(
      drives.start_ideal_range_km -
      drives.end_ideal_range_km,
      0
    ) * car.efficiency
  ), 0)::numeric,
  3
) AS total_kwh
FROM drives
JOIN cars AS car
  ON drives.car_id = car.id
WHERE drives.car_id = %(car_id)s
  AND drives.end_date IS NOT NULL;
"""

PARKED_QUERY = """
WITH position_deltas AS (
  SELECT
    p.car_id,
    p.drive_id,
    p.ideal_battery_range_km,
    LAG(p.drive_id) OVER w AS previous_drive_id,
    LAG(p.ideal_battery_range_km) OVER w AS previous_range
  FROM positions AS p
  WHERE p.car_id = %(car_id)s
    AND p.ideal_battery_range_km IS NOT NULL
  WINDOW w AS (PARTITION BY p.car_id ORDER BY p.date)
)
SELECT ROUND(
  COALESCE(SUM(
    GREATEST(previous_range - ideal_battery_range_km, 0) * car.efficiency
  ), 0)::numeric,
  3
) AS total_kwh
FROM position_deltas
JOIN cars AS car ON car.id = position_deltas.car_id
WHERE drive_id IS NULL
  AND previous_drive_id IS NULL;
"""

CHARGING_QUERY = """
SELECT ROUND(
  COALESCE(SUM(charge_energy_added), 0)::numeric,
  3
) AS total_kwh
FROM charging_processes
WHERE car_id = %(car_id)s
  AND end_date IS NOT NULL;
"""

LAST_DRIVE_QUERY = """
SELECT
  d.id AS drive_id,
  d.distance AS distance_km,
  d.end_date,
  FLOOR(EXTRACT(EPOCH FROM (d.end_date - d.start_date)) / 60)::integer AS duration_minutes,
  ROUND(
    (
      (d.start_ideal_range_km - d.end_ideal_range_km)
      * car.efficiency
    )::numeric,
    3
  ) AS energy_kwh,
  ROUND(
    (
      (
        (d.start_ideal_range_km - d.end_ideal_range_km)
        * car.efficiency
        * 100
      ) / NULLIF(d.distance, 0)
    )::numeric,
    1
  ) AS consumption_kwh_100km
FROM drives AS d
JOIN cars AS car
  ON car.id = d.car_id
WHERE d.car_id = %(car_id)s
  AND d.end_date IS NOT NULL
ORDER BY d.end_date DESC
LIMIT 1;
"""

def get_connection():
    return psycopg.connect(
        host=config["db_host"],
        port=config["db_port"],
        dbname=config["db_name"],
        user=config["db_user"],
        password=config["db_password"],
        row_factory=dict_row,
    )


def fetch_one(query):
    with get_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(query, {"car_id": CAR_ID})
            return cursor.fetchone() or {}


def publish_discovery(client, object_id, name, unit, device_class, state_class):
    topic = f"{DISCOVERY_PREFIX}/sensor/teslamate_trip_metrics/{object_id}/config"
    payload = {
        "name": name,
        "unique_id": f"teslamate_trip_metrics_{object_id}",
        "state_topic": f"{MQTT_BASE}/{object_id}/state",
        "availability_topic": f"{MQTT_BASE}/status",
        "device": {
            "identifiers": ["teslamate_trip_metrics"],
            "name": "TeslaMate Trip Metrics",
            "manufacturer": "TeslaMate",
            "model": "Trip Metrics",
        },
    }

    if unit:
        payload["unit_of_measurement"] = unit
    if device_class:
        payload["device_class"] = device_class
    if state_class:
        payload["state_class"] = state_class

    client.publish(topic, json.dumps(payload), retain=True)


def publish_state(client, object_id, value):
    client.publish(
        f"{MQTT_BASE}/{object_id}/state",
        str(value),
        retain=True,
    )


def setup_discovery(client):
    sensors = [
        (
            "total_driving_energy",
            "TeslaMate Total Driving Energy",
            "kWh",
            "energy",
            "total_increasing",
        ),
        (
            "total_parked_energy",
            "TeslaMate Phantom / Parked Energy",
            "kWh",
            "energy",
            "total_increasing",
        ),
        (
            "total_charged_energy",
            "TeslaMate Total Charged Energy",
            "kWh",
            "energy",
            "total_increasing",
        ),
        (
            "last_drive_distance",
            "TeslaMate Last Drive Distance",
            "km",
            "distance",
            "measurement",
        ),
        (
            "last_drive_energy",
            "TeslaMate Last Drive Energy",
            "kWh",
            "energy",
            "measurement",
        ),
        (
            "last_drive_consumption",
            "TeslaMate Last Drive Consumption",
            "kWh/100 km",
            None,
            "measurement",
        ),
        (
            "last_drive_duration",
            "TeslaMate Last Drive Duration",
            "min",
            "duration",
            "measurement",
        ),
        (
            "last_drive_id",
            "TeslaMate Last Drive ID",
            None,
            None,
            None,
        ),
    ]

    for object_id, name, unit, device_class, state_class in sensors:
        publish_discovery(
            client,
            object_id,
            name,
            unit,
            device_class,
            state_class,
        )


def main():
    client = mqtt.Client(
        callback_api_version=mqtt.CallbackAPIVersion.VERSION2
    )

    if config.get("mqtt_user"):
        client.username_pw_set(
            config["mqtt_user"],
            config["mqtt_password"],
        )

    client.connect(
        config["mqtt_host"],
        int(config["mqtt_port"]),
        60,
    )
    client.loop_start()

    client.will_set(f"{MQTT_BASE}/status", "offline", retain=True)
    client.publish(f"{MQTT_BASE}/status", "online", retain=True)

    setup_discovery(client)

    logger.info("TeslaMate Trip Metrics started (every %s seconds)", INTERVAL)

    while True:
        try:
            driving = fetch_one(DRIVING_QUERY)
            parked = fetch_one(PARKED_QUERY)
            charging = fetch_one(CHARGING_QUERY)
            last_drive = fetch_one(LAST_DRIVE_QUERY)

            publish_state(
                client,
                "total_driving_energy",
                driving.get("total_kwh", 0),
            )
            publish_state(
                client,
                "total_parked_energy",
                parked.get("total_kwh", 0),
            )
            publish_state(
                client,
                "total_charged_energy",
                charging.get("total_kwh", 0),
            )

            publish_state(
                client,
                "last_drive_id",
                last_drive.get("drive_id", 0),
            )
            publish_state(
                client,
                "last_drive_distance",
                last_drive.get("distance_km", 0),
            )
            publish_state(
                client,
                "last_drive_energy",
                last_drive.get("energy_kwh", 0),
            )
            publish_state(
                client,
                "last_drive_consumption",
                last_drive.get("consumption_kwh_100km", 0),
            )
            publish_state(
                client,
                "last_drive_duration",
                last_drive.get("duration_minutes", 0),
            )

            logger.info(
                "Published driving %s kWh, parked %s kWh, latest drive %s",
                driving.get("total_kwh", 0),
                parked.get("total_kwh", 0),
                last_drive.get("drive_id", 0),
            )

        except Exception as error:
            logger.exception("Could not publish TeslaMate metrics: %s", error)

        time.sleep(INTERVAL)


if __name__ == "__main__":
    main()
