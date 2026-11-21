import pendulum
import json

from typing import TypedDict
from pathlib import Path

from airflow.sdk import dag, task, get_current_context
from include.openmeteo.etl import process_data
from include.openmeteo.assets import CONTEXT_AIR_QUALITY, CONTEXT_WEATHER


class DatasetConfig(TypedDict):
    url: str
    hourly_param: list[str]


PROJECT_ROOT_PATH = Path(__file__).resolve().parents[1]
DATASETS: dict[str, DatasetConfig] = {
    "weather": {
        "url": "https://api.open-meteo.com/v1/forecast",
        "hourly_param": ["temperature_2m"],
        "data_type": "weather"
    },
    "air_quality": {
        "url": "https://air-quality-api.open-meteo.com/v1/air-quality",
        "hourly_param": ["pm10", "pm2_5"],
        "data_type": "air_quality"
    }
}


CITIES_PATH = PROJECT_ROOT_PATH / "data" / "input" / "cities.json"
with CITIES_PATH.open("r", encoding="utf-8") as file:
    CITIES_BY_REGION = json.load(file)["regioes"]


def create_region_dag(region: str, cities: list[dict]):
    dag_id = f"openmeteo_{region}"

    @dag(
        dag_id=dag_id,
        start_date=pendulum.datetime(2025, 4, 22),
        schedule="@daily",
        dag_display_name=f"Open-Meteo - {region.title()}",
        tags=["openmeteo", region],
        default_args={"retries": 2},
        auto_register=True
    )
    def regional_openmeteo_dag():
        @task(outlets=[CONTEXT_WEATHER])
        def publish_weather_asset(*, outlet_events):
            context = get_current_context()
            dag_run = context["dag_run"]

            outlet_events[CONTEXT_WEATHER].extra = {
                "process_date": context["logical_date"].strftime("%Y-%m-%d"),
                "region": region,
                "is_backfill": dag_run.run_type == "backfill",
            }

        @task(outlets=[CONTEXT_AIR_QUALITY])
        def publish_air_quality_asset(*, outlet_events):
            context = get_current_context()
            dag_run = context["dag_run"]

            outlet_events[CONTEXT_AIR_QUALITY].extra = {
                "process_date": context["logical_date"].strftime("%Y-%m-%d"),
                "is_backfill": dag_run.run_type == "backfill",
            }

        weather_data = (process_data
            .override(group_id="weather_etl", group_display_name="Weather ETL")
            .partial(config=DATASETS.get("weather"), region=region)
            .expand(city=cities)
        )

        air_quality_data = (process_data
            .override(group_id="air_quality_etl", group_display_name="Air Quality ETL")
            .partial(config=DATASETS.get("air_quality"), region=region)
            .expand(city=cities)
        )

        weather_data >> publish_weather_asset()
        air_quality_data >> publish_air_quality_asset()

    return regional_openmeteo_dag()


for region, cities in CITIES_BY_REGION.items():
    create_region_dag(region, cities)
