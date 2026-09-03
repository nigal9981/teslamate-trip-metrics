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

DRIVING_QUERY = """
SELECT ROUND(
  COALESCE(SUM(
    GREATEST(
      start_position.ideal_battery_range_km - end_position.ideal_battery_range_km,
      0
    ) * car.efficiency
  ), 0)::numeric,
  3
) AS total_kwh
FROM drives
JOIN positions AS start_position ON drives.start_position_id = start_position.id
JOIN positions AS end_position ON drives.end_position_id = end_position.id
JOIN cars AS car ON drives.car_id = car.id
WHERE drives.car_id = 1
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
  WHERE p.car_id = 1
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
JOIN cars AS car ON position_deltas.car_id = car.id
WHERE drive_id IS NULL
  AND previous_drive_id IS NULL;
"""

TOPIC_BASE = "teslamate_trip_metrics"


def publish_discovery(client, metric, name, icon):
    payload = {
        "name": name,
        "unique_id": f"teslamate_trip_metrics_{metric}",
        "state_topic": f"{TOPIC_BASE}/{metric}",
        "unit_of_measurement": "kWh",
        "device_class": "energy",
        "state_class": "total_increasing",
        "icon": icon,
        "device": {
            "identifiers": ["teslamate_trip_metrics"],
            "name": "TeslaMate Trip Metrics",
            "manufacturer": "TeslaMate",
        },
    }
    client.publish(
        f"homeassistant/sensor/teslamate_trip_metrics/{metric}/config",
        json.dumps(payload),
        retain=True,
    )


def get_metric(query):
    with psycopg.connect(
        host=config["db_host"],
        port=config["db_port"],
        dbname=config["db_name"],
        user=config["db_user"],
        password=config["db_password"],
        connect_timeout=10,
        row_factory=dict_row,
    ) as connection:
        with connection.cursor() as cursor:
            cursor.execute(query)
            return float(cursor.fetchone()["total_kwh"])


client = mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION2)
client.username_pw_set(config["mqtt_user"], config["mqtt_password"])
client.connect(config["mqtt_host"], config["mqtt_port"], 60)
client.loop_start()

publish_discovery(
    client,
    "total_driving_energy",
    "TeslaMate Total Driving Energy",
    "mdi:car-electric",
)
publish_discovery(
    client,
    "total_parked_energy",
    "TeslaMate Total Parked Energy",
    "mdi:car-clock",
)

interval = max(int(config["interval_seconds"]), 30)

while True:
    try:
        driving_kwh = get_metric(DRIVING_QUERY)
        parked_kwh = get_metric(PARKED_QUERY)

        client.publish(
            f"{TOPIC_BASE}/total_driving_energy",
            f"{driving_kwh:.3f}",
            retain=True,
        )
        client.publish(
            f"{TOPIC_BASE}/total_parked_energy",
            f"{parked_kwh:.3f}",
            retain=True,
        )

        logger.info(
            "Published driving %.3f kWh and parked %.3f kWh",
            driving_kwh,
            parked_kwh,
        )
    except Exception as error:
        logger.error("Unable to update TeslaMate metrics: %s", error)

    time.sleep(interval)
