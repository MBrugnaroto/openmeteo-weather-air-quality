# Contributing

Thank you for helping improve the Weather and Air Quality Monitor.

## Development environment

The project is an Astronomer Astro project and is run with Docker and the Astro
CLI.

Prerequisites:

- Docker
- Astro CLI
- Network access to the Open-Meteo weather and air-quality APIs
- An Airflow connection named `WeatherAlert` only when testing or running
  agent-generated reports

From the repository root, start the local environment:

```bash
astro dev start
```

Airflow is normally available at `http://localhost:8080`.

## Project layout

- `dags/` contains the generated regional ingestion DAG and report DAGs.
- `include/openmeteo/` contains ETL, Airflow Asset, and Pydantic AI tool code.
- `data/input/cities.json` defines monitored regions and city coordinates.
- `tests/dags/` contains the pytest DAG and alert behavior tests.

Keep changes consistent with the existing Python style and the project's
Airflow TaskFlow and Pydantic AI patterns.

## Making a change

1. Create a focused branch.
2. Use an existing issue or describe the proposed behavior before making a
   broad or backward-incompatible change.
3. Update or add pytest coverage for behavior you change.
4. Run the relevant tests.
5. Review generated data, credentials, and local Airflow files before
   committing; do not add secrets or runtime artifacts.

The local city configuration is in
[`data/input/cities.json`](data/input/cities.json). Adding or changing a city
may require a local rebuild or restart so the container receives the updated
file.

## Running tests

With the Astro environment running:

```bash
astro dev pytest
```

The existing suite verifies regional DAG generation, retry settings, weather
and air-quality alert calculations, partitioning, filtering, and report file
writes.

## Pull requests

Include:

- a clear summary of the problem and solution;
- the related issue, if one exists;
- the test command and result;
- any new connection, configuration, or data-partition requirement;
- screenshots or DAG evidence only when a UI or scheduling behavior changed.

For conduct concerns, read the [Code of Conduct](CODE_OF_CONDUCT.md). Report
suspected vulnerabilities according to the [Security policy](SECURITY.md).
