import pendulum
import json

from typing import TypedDict
from pathlib import Path

from airflow.sdk import dag, task
from include.openmeteo.etl import process_data


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
)
def openmeteo_handler():

    @task
    def get_cities_list() -> list:
        cities_path = PROJECT_ROOT_PATH / "data" / "input" / "cities.json"
        with cities_path.open("r", encoding="utf-8") as file:
            regions = json.load(file)

        return regions['regioes']['sul']

    cities = get_cities_list()
    weather_data = (process_data
        .override(group_id="weather_etl", group_display_name="Weather ETL")
        .partial(config=DATASETS.get('weather'))
        .expand(city=cities)
    )

    air_quality_data = (process_data
        .override(group_id="air_quality_etl", group_display_name="Air Quality ETL")
        .partial(config=DATASETS.get('air_quality'))
        .expand(city=cities)
    )



openmeteo_handler()
