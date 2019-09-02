# DCD Processor

![GitHub package.json version](https://img.shields.io/github/package-json/v/datacentricdesign/dcd-processor)
![GitHub release (latest by date)](https://img.shields.io/github/v/release/datacentricdesign/dcd-processor)
![Docker Build Status](https://img.shields.io/docker/build/datacentricdesign/dcd-processor)

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

# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]
