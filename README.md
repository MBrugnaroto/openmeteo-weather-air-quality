<div align="center">
  <h1>🌦️ Weather and Air Quality Monitor</h1>
  <h3><em>Daily weather and air-quality monitoring for Brazilian cities, powered by Open-Meteo and Apache Airflow.</em></h3>
</div>

<p align="center">
  <a href="https://airflow.apache.org/"><img src="https://img.shields.io/badge/Apache%20Airflow-017CEE?logo=apacheairflow&logoColor=white" alt="Apache Airflow" /></a>
  <a href="https://www.python.org/"><img src="https://img.shields.io/badge/Python-3776AB?logo=python&logoColor=white" alt="Python" /></a>
  <a href="https://www.astronomer.io/docs/astro/cli/"><img src="https://img.shields.io/badge/Astronomer-Astro%20CLI-6E43A3?logo=astronomer&logoColor=white" alt="Astronomer Astro CLI" /></a>
  <a href="https://www.docker.com/"><img src="https://img.shields.io/badge/Docker-2496ED?logo=docker&logoColor=white" alt="Docker" /></a>
  <a href="https://open-meteo.com/"><img src="https://img.shields.io/badge/Data-Open--Meteo-1E40AF" alt="Open-Meteo" /></a>
</p>

## Executive Summary

This project collects hourly weather and air-quality forecasts for nine cities across Brazil's South, Southeast, and Northeast regions. Apache Airflow orchestrates ingestion, alert evaluation, and Markdown report generation.

## System Architecture & Technologies

| Component | Role |
| --- | --- |
| Regional Airflow DAGs | Fetch Open-Meteo forecasts and write date-partitioned raw and context data. |
| Airflow Assets | Trigger weather and air-quality alert DAGs when context data is published. |
| Pandas and PyArrow | Transform forecast data into Parquet datasets. |
| Pydantic AI tools | Generate city-level Markdown reports for qualifying alerts. |

### Data Flow - Data Lineage

```mermaid
flowchart TD
    CITIES["data/input/cities.json"] --> REGION["Daily regional DAGs"]
    subgraph INGESTION["Per-city ingestion"]
        REGION --> WEATHER["Weather ETL"]
        REGION --> AIR["Air-quality ETL"]
        WEATHER --> RAW_WEATHER[("raw/weather JSON")]
        WEATHER --> CONTEXT_WEATHER[("context/weather Parquet")]
        AIR --> RAW_AIR[("raw/air_quality JSON")]
        AIR --> CONTEXT_AIR[("context/air_quality Parquet")]
    end
    CONTEXT_WEATHER --> ASSET_WEATHER{{"CONTEXT_WEATHER Asset"}}
    CONTEXT_AIR --> ASSET_AIR{{"CONTEXT_AIR_QUALITY Asset"}}
    ASSET_WEATHER --> WEATHER_ALERT["Weather alert DAG"]
    ASSET_AIR --> AIR_ALERT["Air-quality alert DAG"]
    WEATHER_ALERT --> SEMANTIC_WEATHER[("semantic/weather Parquet")]
    AIR_ALERT --> SEMANTIC_AIR[("semantic/air_quality Parquet")]
    WEATHER_ALERT --> WEATHER_GATE{"Normal run and alert?"}
    AIR_ALERT --> AIR_GATE{"Normal run and alert?"}
    WEATHER_GATE -->|Yes| WEATHER_REPORT[("reports/weather Markdown")]
    AIR_GATE -->|Yes| AIR_REPORT[("reports/air_quality Markdown")]
```

### Technical Decisions

- Generate daily DAGs from `cities.json` to avoid duplicate regional workflows; mapped TaskFlow tasks run the shared ETL for each city.
- Publish context as Airflow Assets to trigger alerts after ingestion; events carry region, date, and backfill metadata.
- Partition raw JSON, context/semantic Parquet, and Markdown reports by date and region for traceable, date-scoped processing.
- Use the Open-Meteo SDK with a one-hour cache and five retries; forecast code does not use the separately declared HTTP connection.
- Alert at **30 °C or higher**; air quality triggers when daily means exceed **15 µg/m³ for PM2.5** or **45 µg/m³ for PM10**.
- Generate AI reports only for qualifying alerts on normal runs; backfills update semantic data, and short-circuit tasks skip empty reports.
- Schedule DAGs daily and configure two task retries.

## Project Structure

```text
.
├── dags/
│   ├── openmeteo_handler.py
│   ├── issue_temp_alert_report.py
│   └── issue_air_quality_report.py
├── include/openmeteo/        # etl.py, assets.py, agents/prompts/, agents/tools/
├── data/
│   ├── input/cities.json
│   ├── raw/
│   │   ├── weather/year=YYYY/month=MM/day=DD/<region>_<city>_<hour>.json
│   │   └── air_quality/year=YYYY/month=MM/day=DD/<region>_<city>_<hour>.json
│   ├── context/
│   │   ├── weather/year=YYYY/month=MM/day=DD/<region>_<city>.parquet
│   │   └── air_quality/year=YYYY/month=MM/day=DD/<region>_<city>.parquet
│   ├── semantic/
│   │   ├── weather/year=YYYY/month=MM/day=DD/region=<region>/weather_alerts.parquet
│   │   └── air_quality/year=YYYY/month=MM/day=DD/region=<region>/air_quality_alerts.parquet
│   └── reports/
│       ├── weather/year=YYYY/month=MM/day=DD/reports/region=<region>/<city>.md
│       └── air_quality/year=YYYY/month=MM/day=DD/reports/region=<region>/<city>.md
└── tests/dags/              # DAG, weather-alert, and air-quality tests
```

## Key Features

- Daily forecasts for nine cities in three Brazilian regions.
- Automated weather and air-quality alert/report workflows.
- Date-partitioned outputs; backfills update data without generating AI reports.

## Technologies

- Apache Airflow with Astronomer Runtime
- Astronomer Astro CLI and Docker
- Open-Meteo Weather API
- Open-Meteo Air Quality API
- Pandas and PyArrow for processing and Parquet
- Pydantic AI for tools and agent-generated reports
- Requests Cache and Retry Requests for HTTP caching and retries

The main Python dependencies are listed in
[`requirements.txt`](requirements.txt).

## Local Development Setup

### 0. Requirements

- Docker
- Astro CLI
- Network access to the Open-Meteo APIs
- An LLM connection configured in Airflow to generate reports

### 1. Start the environment

From the project root:

```bash
$ astro dev start
```

The environment is available at:

- Airflow UI: `http://localhost:8080`
- Default Astro username: `admin`
- Default Astro password: `admin`

### 2. Configure the Open-Meteo connection

The [`airflow_settings.yaml`](airflow_settings.yaml) file configures an HTTP
connection named `open-meteo-weather-api`. The local project includes this
configuration, although the current ingestion code calls the endpoints directly
through the Open-Meteo SDK.

### 3. Configure the AI agent

Both report DAGs use the AI connection named `WeatherAlert`. Configure it in
the Airflow UI under **Admin > Connections**, or through an environment variable
supported by your provider.

Tasks may have no actionable alerts. In that case, the `short_circuit` operator
stops the DAG before invoking the agent.

## Container configuration

The [`Dockerfile`](Dockerfile) uses Astronomer Runtime and runs Airflow with
the non-root `astro` user. The default UID is `1000`, aligned with the
workspace user.

If the host user has a different UID, update the default argument and recreate
the environment:

```dockerfile
ARG AIRFLOW_UID=<your_uid>
```

The `data/` directory is copied into the image during the build. After changing
[`data/input/cities.json`](data/input/cities.json), rebuild or restart the
environment to make the updated file available:

```bash
$ astro dev restart
```

### Repair legacy log permissions

Environments created previously may contain logs owned by the legacy `50000`
UID. Repair the volume once with:

```bash
$ docker exec --user root <scheduler-container> chown -R 1000:50000 /usr/local/airflow/logs
$ docker restart <dag-processor-container>
```

## Run tests

With the Astro environment running:

```bash
$ astro dev pytest
```

### Test coverage

- DAG imports, tags, retries, and generation of three regional DAGs.
- Temperature threshold, date/region filters, semantic partitions, and Markdown report reads/writes.
- Daily PM2.5/PM10 means and guidelines, date/region filters, partitioned alerts, and air-quality report reads/writes.

## Project Policies

- [Contributing](CONTRIBUTING.md)
- [Code of Conduct](CODE_OF_CONDUCT.md)
- [Security](SECURITY.md)
- [License](LICENSE)
