import pendulum
import openmeteo_requests
import requests_cache
import pandas as pd
import json

from pathlib import Path

from retry_requests import retry
from airflow.sdk import task, TaskInstance, task_group, get_current_context


PROJECT_ROOT_PATH = Path(__file__).resolve().parents[2]


@task_group(group_id="openmeteo_etl", group_display_name="Open-Meteo ETL")
def process_data(city: dict, config: dict) -> dict:

    @task(map_index_template="{{ extract_step }}")
    def fetch_data(city: dict, config: dict) -> str:
        data_type = config.get('data_type')
        city_name = city.get('nome')
        context = get_current_context()
        context["extract_step"] = (
            f"extract-{data_type}-{city_name}"
            .replace("_", "-")
            .replace(" ", "-")
            .lower()
        )
        logical_date = context["logical_date"]

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
            f"sul_{city_name}_{hour}.json"
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

    @task(map_index_template = "{{ transform_step }}")
    def adding_context_to_data(ti: TaskInstance, extract_infos: dict):
        city_name = extract_infos.get("city").get('nome')
        data_type = extract_infos.get('data_type')
        context = get_current_context()
        context["transform_step"] = (
            f"transform-{data_type}-{city_name}"
            .replace("_", "-")
            .replace(" ", "-")
            .lower()
        )

        with open(extract_infos.get('raw_path'), "r", encoding="utf-8") as file:
            data = json.load(file)

        context = {
            "region:": "sul",
            "city": city_name,
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
            data_type /
            f"year={year}" /
            f"month={month}" /
            f"day={day}" /
            f"sul_{city_name}.parquet"
        )
        output_path.parent.mkdir(parents=True, exist_ok=True)

        df.to_parquet(output_path)

    extracted_infos = fetch_data(city=city, config=config)
    adding_context_to_data(extract_infos=extracted_infos)