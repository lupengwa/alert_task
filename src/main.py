import time
from datetime import timedelta, datetime
from enum import Enum
from typing import Any

import sqlalchemy as sa
from psycopg2 import IntegrityError
from sqlalchemy import CursorResult


def database_connection() -> sa.Connection:
    engine = sa.create_engine("postgresql://postgres:postgres@postgres:5432/postgres")

    for attempt in range(5):
        try:
            conn = engine.connect()
        except sa.exc.OperationalError as e:
            if attempt == 4:
                raise e
            time.sleep(1)

    conn.execute(
        sa.text(
            "CREATE TABLE IF NOT EXISTS detections "
            "(id SERIAL PRIMARY KEY, time TIMESTAMP WITH TIME ZONE, type VARCHAR)"
        )
    )

    return conn


class Type(Enum):
    PEDESTRIAN = 'pedestrian'
    BICYCLE = 'bicycle'
    CAR = 'car'
    TRUCK = 'truck'
    VAN = 'van'


pedestrian_detected_start_time = None
bicycle_detected_start_time = None
threshold = timedelta(minutes=5)
timestamp_format = "%Y-%m-%dT%H:%M:%S"


def alert_unusual_activity(timestamp: str, detection_str: str):
    global bicycle_detected_start_time
    global pedestrian_detected_start_time
    event_time = datetime.strptime(timestamp, timestamp_format)
    try:
        detection_type = Type(detection_str)
        if detection_type == Type.BICYCLE:
            pedestrian_detected_start_time = None
            if not bicycle_detected_start_time:
                bicycle_detected_start_time = event_time
            elif event_time - bicycle_detected_start_time >= threshold:
                print(
                    f"Alert: a person has been detected for more than 5 mins between {bicycle_detected_start_time}-{event_time}")
        elif detection_type == Type.PEDESTRIAN:
            bicycle_detected_start_time = None
            if not pedestrian_detected_start_time:
                pedestrian_detected_start_time = event_time
            elif event_time - pedestrian_detected_start_time >= threshold:
                print(
                    f"Alert: a person has been detected for more than 5 mins between {pedestrian_detected_start_time}-{event_time}")
        else:
            pedestrian_detected_start_time = None
            bicycle_detected_start_time = None
    except ValueError:
        print(f"Invalid detection type:{detection_str}")


def ingest_data(conn: sa.Connection, timestamp: str, detection_type: str) -> bool:
    alert_unusual_activity(timestamp, detection_type)

    insert_query = sa.text("INSERT INTO detections (time, type) VALUES (:time, :type)")
    new_time_data = {"time": timestamp, "type": detection_type}
    try:
        conn.execute(insert_query, new_time_data)
        conn.commit()
        return True
    except IntegrityError as e:
        conn.rollback()
        return False


# given a category name, generate aggregate intervals for this category
def generate_type_aggregate(result: CursorResult[Any], aggregate_result: dict[str, list[tuple[str, str]]],
                            detection_type: str):
    aggregate_result[detection_type] = []
    current_interval = [None, None]
    for row in result:
        if not current_interval[0]:
            current_interval[0] = row[0]
        else:
            next_interval_start = None
            if row[0] <= current_interval[0] + timedelta(minutes=1):
                current_interval[1] = row[0]
            else:
                current_interval[1] = current_interval[0]
                next_interval_start = row[0]
            aggregate_result[detection_type].append((current_interval[0], current_interval[1]))
            current_interval[0] = next_interval_start
    # post operation
    if current_interval[0] and not current_interval[1]:
        current_interval[1] = current_interval[0]
        aggregate_result[detection_type].append((current_interval[0], current_interval[1]))


people_sql = """
WITH people_data AS (
SELECT
time
FROM
detections
WHERE
TYPE IN (:pedestrian, :bicycle)
)
SELECT
min(time) AS time
FROM
people_data
UNION
SELECT
COALESCE(nxt_result.nxt_time,
cur.time) AS time
FROM
people_data AS cur
LEFT JOIN LATERAL (
SELECT
nxt.time AS nxt_time
FROM
people_data AS nxt
WHERE
nxt.time >= cur.time + INTERVAL '1 minute'
ORDER BY
nxt.time
LIMIT 1
) nxt_result ON
TRUE
ORDER BY
time
"""

vehicle_sql = """
WITH vehicle_data AS (
SELECT
time
FROM
detections
WHERE
TYPE IN (:car, :truck,:van)
)
SELECT
min(time) AS time
FROM
vehicle_data
UNION
SELECT
COALESCE(nxt_result.nxt_time,
cur.time) AS time
FROM
vehicle_data AS cur
LEFT JOIN LATERAL (
SELECT
nxt.time AS nxt_time
FROM
vehicle_data AS nxt
WHERE
nxt.time >= cur.time + INTERVAL '1 minute'
ORDER BY
nxt.time
LIMIT 1
) nxt_result ON
TRUE
ORDER BY
time
"""


def aggregate_detections(conn: sa.Connection) -> dict[str, list[tuple[str, str]]]:
    aggregate_result = {}

    select_people_query = sa.text(people_sql)
    people_result = conn.execute(select_people_query,
                                 {"pedestrian": Type.PEDESTRIAN.value, "bicycle": Type.BICYCLE.value})
    generate_type_aggregate(people_result, aggregate_result, "people")

    select_vehicle_query = sa.text(vehicle_sql)
    vehicle_result = conn.execute(select_vehicle_query,
                                  {"car": Type.CAR.value, "truck": Type.TRUCK.value, "van": Type.VAN.value})
    generate_type_aggregate(vehicle_result, aggregate_result, "vehicles")

    return aggregate_result


def main():
    conn = database_connection()

    # Simulate real-time detections every 30 seconds
    detections = [
        ("2023-08-10T18:30:30", "pedestrian"),
        ("2023-08-10T18:31:00", "pedestrian"),
        ("2023-08-10T18:31:00", "car"),
        ("2023-08-10T18:31:30", "pedestrian"),
        ("2023-08-10T18:35:00", "pedestrian"),
        ("2023-08-10T18:35:30", "pedestrian"),
        ("2023-08-10T18:36:00", "pedestrian"),
        ("2023-08-10T18:37:00", "pedestrian"),
        ("2023-08-10T18:37:30", "pedestrian"),
    ]

    for timestamp, detection_type in detections:
        if ingest_data(conn, timestamp, detection_type):
            print(f'data({timestamp}, {detection_type}) insert succeeds')
        else:
            print(f'data({timestamp}, {detection_type}) insert failed')

    aggregate_results = aggregate_detections(conn)
    print(aggregate_results)


if __name__ == "__main__":
    main()
