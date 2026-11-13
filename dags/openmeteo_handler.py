import pendulum
import openmeteo_requests
import requests_cache
import pandas as pd
import json
from pathlib import Path

from retry_requests import retry
from airflow.sdk import dag, task, TaskInstance, task_group, get_current_context


PROJECT_ROOT_PATH = Path(__file__).resolve().parents[1]


@dag(
    start_date=pendulum.datetime(2025, 4, 22),
    schedule="@daily",
    doc_md=__doc__,
    render_template_as_native_obj=True,
    dag_display_name='Open-Meteo Weather Extractor'
)
def openmeteo_handler():

    @task
    def get_cities_list() -> list:
        cities_path = PROJECT_ROOT_PATH / "data" / "input" / "cities.json"
        with cities_path.open("r", encoding="utf-8") as file:
            regions = json.load(file)

        return regions['regioes']['sul']

    @task_group(group_id='weather_etl', group_display_name='Weather ETL')
    def process_data(city: dict) -> dict:
        @task
        def fetch_weather_data(city: dict) -> str:
            cache_session = requests_cache.CachedSession('.weathercache', expire_after = 3600)
            retry_session = retry(cache_session, retries = 5, backoff_factor = 0.2)
            openmeteo = openmeteo_requests.Client(session = retry_session)

            url = "https://api.open-meteo.com/v1/forecast"
            params = {
                "latitude": city.get('lat'),
                "longitude": city.get('lon'),
                "hourly": "temperature_2m",
            }
            responses = openmeteo.weather_api(url, params = params)
            hourly = responses[0].Hourly()
            hourly_temperature_2m = hourly.Variables(0).ValuesAsNumpy()

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
            hourly_data["temperature_2m"] = hourly_temperature_2m.tolist()
            output_path = PROJECT_ROOT_PATH / "data" / "raw" / f"year={year}" / f"month={month}" / f"day={day}" / f"sul_{city.get('nome')}_{hour}.json"

            output_path.parent.mkdir(parents=True, exist_ok=True)
            with open(output_path, "w", encoding='utf-8') as file:
                json.dump(hourly_data, file, indent=4, ensure_ascii=False)

            return {
                "city": city,
                "year": year,
                "month": month,
                "day": day,
                "hour": hour,
                "raw_path": str(output_path)
            }

        @task
        def adding_semantics_to_data(ti: TaskInstance, extract_infos: dict):
            with open(extract_infos.get('raw_path'), "r", encoding="utf-8") as file:
                weather_data = json.load(file)

            semantic_data = {
                "region:": "sul",
                "city": extract_infos.get("city").get('nome'),
                "lat": extract_infos.get("city").get('lat'),
                "lng": extract_infos.get("city").get('lon'),
                "extract_ts": ti.start_date.isoformat()
            }

            df = pd.DataFrame(semantic_data | weather_data)

            year = extract_infos.get('year')
            month = extract_infos.get('month')
            day = extract_infos.get('day')
            output_path = PROJECT_ROOT_PATH / 'data' / 'context' / f"year={year}" / f"month={month}" / f"day={day}" / f"sul_{extract_infos.get("city").get('nome')}.parquet"
            output_path.parent.mkdir(parents=True, exist_ok=True)

            df.to_parquet(output_path)

        extracted_infos = fetch_weather_data(city=city)
        adding_semantics_to_data(extract_infos=extracted_infos)

    process_data.expand(city=get_cities_list())

openmeteo_handler()
