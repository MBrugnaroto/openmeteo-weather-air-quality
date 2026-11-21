from datetime import datetime
import pendulum
import json

from typing import TypedDict
from pathlib import Path

from airflow.sdk import dag, task, get_current_context, Param
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


@dag(
    start_date=pendulum.datetime(2025, 4, 22),
    schedule="@daily",
    doc_md=__doc__,
    dag_display_name="Open-Meteo Weather and Air Quality Extractor",
    params={
        "region": Param(
            "sul",
            type="string",
            title="Região",
            description="Região brasileira cujas cidades serão consultadas.",
            enum=["sul", "sudeste", "nordeste"],
        )
    }
)
def openmeteo_handler():

    @task
    def get_region() -> str:
        context = get_current_context()
        return context["params"]["region"]

    @task
    def get_cities_list(region: str) -> list:
        cities_path = PROJECT_ROOT_PATH / "data" / "input" / "cities.json"
        with cities_path.open("r", encoding="utf-8") as file:
            regions = json.load(file)

        try:
            return regions["regioes"][region]
        except KeyError as error:
            raise ValueError(
                f"Invalid region: '{region}'. Use sul, sudeste or nordeste."
            ) from error

    @task(outlets=[CONTEXT_WEATHER])
    def publish_weather_asset(*, outlet_events):
        context = get_current_context()
        dag_run = context["dag_run"]

        outlet_events[CONTEXT_WEATHER].extra = {
            "process_date": context["logical_date"].strftime("%Y-%m-%d"),
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

    region = get_region()
    cities = get_cities_list(region=region)
    weather_data = (process_data
        .override(group_id="weather_etl", group_display_name="Weather ETL")
        .partial(config=DATASETS.get('weather'), region=region)
        .expand(city=cities)
    )

    air_quality_data = (process_data
        .override(group_id="air_quality_etl", group_display_name="Air Quality ETL")
        .partial(config=DATASETS.get('air_quality'), region=region)
        .expand(city=cities)
    )

    weather_data >> publish_weather_asset()
    air_quality_data >> publish_air_quality_asset()


openmeteo_handler()
