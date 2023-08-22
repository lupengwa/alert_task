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


ped_detected_start_time = None
bicycle_detected_start_time = None
threshold = timedelta(minutes=5)
timestamp_format = "%Y-%m-%dT%H:%M:%S"


def alert_unusual_activity(timestamp: str, detection_str: str):
    global bicycle_detected_start_time
    global ped_detected_start_time
    event_time = datetime.strptime(timestamp, timestamp_format)
    try:
        detection_type = Type(detection_str)
        if detection_type == Type.BICYCLE:
            ped_detected_start_time = None
            if not bicycle_detected_start_time:
                bicycle_detected_start_time = event_time
            elif event_time - bicycle_detected_start_time >= threshold:
                print(
                    f"Alert: a person has been detected for more than 5 mins between {bicycle_detected_start_time}-{event_time}")
        elif detection_type == Type.PEDESTRIAN:
            bicycle_detected_start_time = None
            if not ped_detected_start_time:
                ped_detected_start_time = event_time
            elif event_time - ped_detected_start_time >= threshold:
                print(
                    f"Alert: a person has been detected for more than 5 mins between {ped_detected_start_time}-{event_time}")
        else:
            ped_detected_start_time = None
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


def check_data(conn: sa.Connection):
    select_query = sa.text("SELECT * FROM detections")
    result = conn.execute(select_query)
    for row in result:
        print(row)


# given a category name, generate aggregate intervals for this category
def generate_type_aggregate(result: CursorResult[Any], aggregate_result: dict[str, list[tuple[str, str]]],
                            detection_type: str):
    aggregate_result[detection_type] = []
    current_interval = [None, None]
    for row in result:
        if not current_interval[0]:
            current_interval[0] = row[0]
        elif row[0] <= current_interval[0] + timedelta(minutes=1):
            current_interval[1] = row[0]
        else:
            aggregate_result[detection_type].append((current_interval[0], current_interval[1]))
            current_interval[0] = row[0]
            current_interval[1] = None
    # post operation
    if not current_interval[1]:
        current_interval[1] = current_interval[0]
    aggregate_result[detection_type].append((current_interval[0], current_interval[1]))


def aggregate_detections(conn: sa.Connection) -> dict[str, list[tuple[str, str]]]:
    aggregate_result = {}

    select_people_query = sa.text("SELECT time FROM detections WHERE type = :pedestrian OR type= :bicycle ORDER BY time ASC")
    people_result = conn.execute(select_people_query,
                                 {"pedestrian": Type.PEDESTRIAN.value, "bicycle": Type.BICYCLE.value})
    generate_type_aggregate(people_result, aggregate_result, "people")

    select_vehicle_query = sa.text("SELECT time FROM detections WHERE type IN (:car, :truck, :van) ORDER BY time ASC")
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
