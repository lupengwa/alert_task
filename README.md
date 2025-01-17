# Video events management task

This repo contains a short interview coding task at Coram.AI

You are asked to implement backend APIs of a video event management system supporting basic operations of:
1. Ingestion of new video events.
2. Aggregation of recorded video events.
3. Alert triggered on unusual activity.

The events are stored in postgres database the backend interacts with. In our case backend are simple python functions.

## Setup
1. Install [docker](https://docs.docker.com/engine/install/) and [docker-compose](https://docs.docker.com/compose/install/)
2. Run the code using `./run.sh`. This script starts the database and runs implemented APIs.

## Tasks
All tasks should be implemented within `main.py` file:

### Ingestion
To start off, please complete the `ingest_data` function - feel free to change the signature if you want to. Please also consider the implications of the code - performance, maintenance, etc.  
The detections have a timestamp and a type property. The type will always be one of a set of pre-defined strings - "pedestrian", "bicycle", "car", "truck", "van". Assume the detections are ordered.


### Aggregation
We'd like to show our user an overview of the detections in the database. To do that please complete the "aggregate_detections" function, so that it returns **two lists of periods of activity**. One should contain the activity of people who were detected (the "pedestrian" and "bicycle" detection types) and the other should contain the activity of vehicles (the "car", "truck" and "van" detection types).
The goal is to aggregate the detections in the database into periods of activity in each category. Any detections less than 1 minute apart in the same category should be combined into an interval.

For example if we assume that the system has detected cars at the following timestamps: 18:30:30, 18:31:00, 18:31:30, 18:35:00, 18:35:30, 18:36:00, 18:37:30, 18:38:00. The aggregate function should return three periods of activity: 18:30:30-18:31:30, 18:35:00-18:36:00 and 18:37:30-18:38:00.

Please consider the performance of the function you write - how would it work with millions of data points. We'd encourage you to keep as much of the logic in the database query.


### Alerts
The last part is related to real-time alerts for unusual events. Our users would like to be notified if a person is detected for a continued period of time. Imagine that the ingestion function is called real-time as the detections happen every 30 seconds.

Please add additional logic which will print to the console if a person is detected in 5 consecutive intervals. Please consider performance - the write function will be called very often.
