# dcd-processor

Processing data from Kafka to Kafka


## Setup

Create file .env with the following variables

```text
LOG_LEVEL=DEBUG
KAFKA=true
KAFKA_HOST=
KAFKA_PORT=9092
AUTH_ENABLED=FASLE
```

Tune the data activity and count with the following env variable (in ms)

```text
CHECK_ACTIVITY_COUNT=60000
```