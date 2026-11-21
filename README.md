# Weather and Air Quality Monitor

Weather and air quality monitoring for Brazilian cities, built with Apache
Airflow, Astronomer Astro Runtime, and data from the Open-Meteo API.

The project retrieves daily temperature, PM2.5, and PM10 forecasts; stores the
data in date-partitioned files; identifies threshold exceedances; and can
generate Markdown reports with an AI agent.

## Overview

### Data flow

```text
cities.json
    |
    v
Regional Open-Meteo DAG -- one daily run per region
    |-- Weather ETL --> data/raw/weather/
    |                 --> data/context/weather/
    |
    `-- Air Quality ETL --> data/raw/air_quality/
                           --> data/context/air_quality/
                                           |
                                           v
                                  Airflow Assets
                                           |
                 +-------------------------+-------------------------+
                 v                                                   v
   Weather Alert DAG                                   Air Quality Alert DAG
   temperature >= 30 °C                               daily mean PM2.5 > 15 µg/m³
                                                       or PM10 > 45 µg/m³
                 |                                                   |
                 v                                                   v
           data/semantic/                                      data/semantic/
           data/reports/                                       data/reports/
```

Each region generates a dynamic daily DAG:

- `openmeteo_sul`
- `openmeteo_sudeste`
- `openmeteo_nordeste`

The monitored cities are listed in
[`data/input/cities.json`](data/input/cities.json).

## DAGs

| DAG | Responsibility |
| --- | --- |
| `openmeteo_<region>` | Calls the weather and air quality forecast endpoints, creates traceable data versions, and publishes the region's Asset. |
| `Open-Meteo Weather Alerts` | Loads region- and date-partitioned weather data, identifies temperatures greater than or equal to 30 °C, and generates a report for each affected city. |
| `Open-Meteo Air Quality Alerts` | Calculates daily PM2.5 and PM10 means, compares them with configured limits, and generates reports for exceedances. |

Assets are defined in
[`include/openmeteo/assets.py`](include/openmeteo/assets.py). When a weather or
air quality stage is complete, the corresponding alert DAG is triggered
automatically.

> [!NOTE]
> Backfill runs update partitioned data and semantic files, but do not trigger
> AI-agent report generation.

## Airflow concepts used

### Dynamic DAG generation

The project reads the region and city configuration at DAG parsing time and
calls `create_region_dag()` once for each region. This pattern produces one
independent DAG dynamically instead of duplicating its Python definition.

The generated DAGs use:

- `dag_id` and `dag_display_name` for identification and UI display;
- `schedule="@daily"` for scheduled execution;
- `start_date` for the scheduling window;
- `tags` for categorization;
- `default_args={"retries": 2}` for automatic task retries.

### TaskFlow API

The project uses `@dag`, `@task`, `@task_group`, and `@task.agent` from the
Airflow TaskFlow API. Decorators express tasks and their dependencies directly
in Python, while Airflow passes outputs between tasks as XComs where necessary.

Mapped tasks use `.partial()` and `.expand()` to run weather and air quality
ETL for each city in a region. Map-index templates customize task labels in the
UI with extracted and transformed data names.

### Task groups

The weather and air quality workflows are grouped with `TaskGroup` at two
levels:

- `openmeteo_etl` groups extraction and transformation for each city;
- `weather_etl` and `air_quality_etl` group the mapped ETL workflow for a
  complete region.

### Extract, transform, and context

The `fetch_data` task:

1. reads the city coordinates and configuration;
2. requests hourly forecast data;
3. builds an hourly timestamp series;
4. writes raw JSON partitioned by logical date and hour.

The `adding_context_to_data` task adds region, city, coordinates, and extraction
timestamp; converts the raw payload into a Pandas DataFrame; and writes it as
Parquet context data.

### Logical date and execution context

The project calls `get_current_context()` to read Airflow execution metadata,
including `logical_date` and `dag_run`. The logical date is used to partition
data consistently and select forecast records for the processed day.

`dag_run.run_type` distinguishes normal executions from backfills. This allows
the project to preserve semantic and physical data for backfilled dates without
generating agent reports for historical backfills.

### Airflow Assets

`CONTEXT_WEATHER` and `CONTEXT_AIR_QUALITY` are Airflow Assets that represent
the regional weather and air quality context datasets. The publishing tasks
declare them with `outlets=[...]` and attach metadata such as:

- `process_date`;
- `region`;
- `is_backfill`.

The alert DAGs schedule themselves with `schedule=[CONTEXT_WEATHER]` or
`schedule=[CONTEXT_AIR_QUALITY]`. They read the triggering Asset events with
`triggering_asset_events`, enabling data-aware DAG dependencies.

### Data-aware dependency chains

The workflow is not driven by a traditional one-DAG predecessor:

```text
regional extraction and transformation
                |
                v
        publish Weather Asset
                |
                v
      trigger temperature alert DAG
```

The same pattern applies to air quality. Multiple region Asset events can also
be processed together by the alert DAG.

### Short-circuit tasks

Both alert DAGs use `@task.short_circuit` before invoking the AI agent. If no
cities exceed the configured threshold, the task returns `False` and downstream
agent tasks are skipped.

### Retries and DAG dependencies

All DAGs set two retries in `default_args`. Airflow's TaskFlow syntax also
defines the execution chain explicitly, as in:

```python
weather_data >> publish_weather_asset()
air_quality_data >> publish_air_quality_asset()
```

### Connections

The project declares an HTTP connection named `open-meteo-weather-api` in
`airflow_settings.yaml`. The report agents also use a connection ID named
`WeatherAlert` in `@task.agent`.

In a local Astro environment, `airflow_settings.yaml` provides connection
defaults for development; production connections should be managed with
Airflow's configured secrets or connection backend.

### AI agent tasks and toolsets

`@task.agent` turns an LLM task into an Airflow task. The agent receives:

- a system prompt;
- an LLM connection;
- a Pydantic AI `FunctionToolset`.

The weather agent can load weather context and write a Markdown report. The
air quality agent can load PM data and write its own report. The agents are
instructed to return only Markdown and to base reports solely on tool results.

### Mapped agent reports

Alert lists are passed to `create_report.expand(alert=city_alerts)`. Airflow
creates one report-agent task instance for each alerting city, including the
city, region, date, and alert label.

### Data partitioning

The project partitions raw, contextual, semantic, and report data by:

```text
year=YYYY/month=MM/day=DD
```

Regional and report data may add `region=<region>` and city-specific filenames.
This layout supports deterministic loading, scoped queries, and backfills.

## Data and alerts

### Temperature

Ingestion requests the hourly `temperature_2m` variable. A city is added to the
alert set when at least one record has a temperature greater than or equal to
**30 °C**.

### Air quality

Ingestion requests `pm10` and `pm2_5`. Alerts use the daily mean for each city:

- PM2.5: daily limit of **15 µg/m³**
- PM10: daily limit of **45 µg/m³**

An exceedance occurs when the daily mean is greater than the limit, not when a
single hourly peak exceeds it.

### File structure

Results are written inside `data/` using year, month, and day partitions:

```text
data/
├── input/
│   └── cities.json
├── raw/
│   ├── weather/year=YYYY/month=MM/day=DD/<region>_<city>_<hour>.json
│   └── air_quality/year=YYYY/month=MM/day=DD/<region>_<city>_<hour>.json
├── context/
│   ├── weather/year=YYYY/month=MM/day=DD/<region>_<city>.parquet
│   └── air_quality/year=YYYY/month=MM/day=DD/<region>_<city>.parquet
├── semantic/
│   ├── weather/year=YYYY/month=MM/day=DD/region=<region>/weather_alerts.parquet
│   └── air_quality/year=YYYY/month=MM/day=DD/region=<region>/air_quality_alerts.parquet
└── reports/
    ├── weather/year=YYYY/month=MM/day=DD/reports/region=<region>/<city>.md
    └── air_quality/year=YYYY/month=MM/day=DD/reports/region=<region>/<city>.md
```

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

## Requirements

- Docker
- Astro CLI
- Network access to the Open-Meteo APIs
- An LLM connection configured in Airflow to generate reports

## Run locally

### 1. Start the environment

From the project root:

```bash
astro dev start
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

Then run:

```bash
astro dev stop
astro dev start
```

The `data/` directory is copied into the image during the build. After changing
[`data/input/cities.json`](data/input/cities.json), rebuild or restart the
environment to make the updated file available:

```bash
astro dev stop
astro dev start
```

### Repair legacy log permissions

Environments created previously may contain logs owned by the legacy `50000`
UID. Repair the volume once with:

```bash
docker exec --user root <scheduler-container> \
  chown -R 1000:50000 /usr/local/airflow/logs
docker restart <dag-processor-container>
```

## Run tests

With the Astro environment running:

```bash
astro dev pytest
```

The tests cover:

- generation of the three regional DAGs;
- temperature alert rules;
- daily means and exceedances for PM2.5 and PM10;
- region and date filtering;
- semantic partition creation;
- reading and writing Markdown reports.
