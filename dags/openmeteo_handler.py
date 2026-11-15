import pendulum
import openmeteo_requests
import requests_cache
import pandas as pd
import json

from typing import Literal, TypedDict, get_args
from pathlib import Path

from retry_requests import retry
from airflow.sdk import dag, task, TaskInstance, task_group, get_current_context


class DatasetConfig(TypedDict):
    url: str
    hourly_param: list[str]


PROJECT_ROOT_PATH = Path(__file__).resolve().parents[1]
DATATYPE = Literal["weather", "air_quality"]
DATASETS: dict[DATATYPE, DatasetConfig] = {
    "weather": {
        "url": "https://api.open-meteo.com/v1/forecast",
        "hourly_param": ["temperature_2m"]
    },
    "air_quality": {
        "url": "https://air-quality-api.open-meteo.com/v1/air-quality",
        "hourly_param": ["pm10", "pm2_5"]
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

    @task_group(group_id="openmeteo_etl", group_display_name="Open-Meteo ETL")
    def process_data(city: dict, data_type: DATATYPE) -> dict:
        @task
        def fetch_data(city: dict, data_type: DATATYPE) -> str:
            config: DatasetConfig = DATASETS.get(data_type)
            cache_session = requests_cache.CachedSession(f'.{data_type}cache', expire_after = 3600)
            retry_session = retry(cache_session, retries = 5, backoff_factor = 0.2)
            openmeteo = openmeteo_requests.Client(session = retry_session)

            params = {
                "latitude": city.get('lat'),
                "longitude": city.get('lon'),
                "hourly": config.get('hourly_param'),
            }
            responses = openmeteo.weather_api(config.get('url'), params=params)
            hourly = responses[0].Hourly()

            logical_date = get_current_context()['logical_date']
            year = logical_date.strftime("%Y")
            month = logical_date.strftime("%m")
            day = logical_date.strftime("%d")
            hour = pendulum.Formatter().format(logical_date, 'HH')

            hourly_data = {
                "forcast_ts": [
                    timestamp.isoformat()
                    for timestamp in pd.date_range(
                        start = pd.to_datetime(hourly.Time(), unit = "s", utc = True),
                        end = pd.to_datetime(hourly.TimeEnd(), unit = "s", utc = True),
                        freq = pd.Timedelta(seconds = hourly.Interval()),
                        inclusive = "left",
                    )
                ],
            }

            hourly_data.update({
                param: hourly.Variables(index).ValuesAsNumpy().tolist()
                for index, param in enumerate(config.get('hourly_param'))
            })

            output_path = (
                PROJECT_ROOT_PATH /
                "data" /
                "raw" /
                data_type /
                f"year={year}" /
                f"month={month}" /
                f"day={day}" /
                f"sul_{city.get('nome')}_{hour}.json"
            )

            output_path.parent.mkdir(parents=True, exist_ok=True)
            with open(output_path, "w", encoding='utf-8') as file:
                json.dump(hourly_data, file, indent=4, ensure_ascii=False)

            return {
                "city": city,
                "data_type": data_type,
                "year": year,
                "month": month,
                "day": day,
                "hour": hour,
                "raw_path": str(output_path)
            }

        @task
        def adding_context_to_data(ti: TaskInstance, extract_infos: dict):
            with open(extract_infos.get('raw_path'), "r", encoding="utf-8") as file:
                data = json.load(file)

            context = {
                "region:": "sul",
                "city": extract_infos.get("city").get('nome'),
                "lat": extract_infos.get("city").get('lat'),
                "lng": extract_infos.get("city").get('lon'),
                "extract_ts": ti.start_date.isoformat()
            }

            df = pd.DataFrame(context | data)

            year = extract_infos.get('year')
            month = extract_infos.get('month')
            day = extract_infos.get('day')
            output_path = (
                PROJECT_ROOT_PATH /
                'data' /
                'context' /
                extract_infos.get('data_type') /
                f"year={year}" /
                f"month={month}" /
                f"day={day}" /
                f"sul_{extract_infos.get("city").get('nome')}.parquet"
            )
            output_path.parent.mkdir(parents=True, exist_ok=True)

            df.to_parquet(output_path)

        extracted_infos = fetch_data(city=city, data_type=data_type)
        adding_context_to_data(extract_infos=extracted_infos)

    data_type: list = [data_type for data_type in DATASETS.keys()]
    process_data.expand(city=get_cities_list(), data_type=data_type)

openmeteo_handler()
