import http.client
import os
from typing import Optional

import pandas as pd
import json
from datetime import datetime, timedelta, timezone

from quartz_solar_forecast.inverters.inverter import AbstractInverter
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class AuroraVisionSettings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    api_key: str = Field(alias="AURORA_API_KEY")
    user_id: str = Field(alias="AURORA_USER_ID")
    password: str = Field(alias="AURORA_PASSWORD")
    plant_id: str = Field(alias="AURORA_PLANT_ID")


class AuroraVisionInverter(AbstractInverter):
    def __init__(self, settings: AuroraVisionSettings):
        self.__settings = settings

    def get_data(self, ts: pd.Timestamp) -> Optional[pd.DataFrame]:
        return get_aurora_vision_data(self.__settings, ts)


def authenticate_aurora(settings: AuroraVisionSettings) -> str:
    """Authenticate with the Aurora Vision API and retrieve a token."""
    conn = http.client.HTTPSConnection("api.auroravision.net")
    headers = {"X-AuroraVision-ApiKey": settings.api_key}
    auth_str = f"{settings.user_id}:{settings.password}"
    headers["Authorization"] = "Basic " + base64.b64encode(auth_str.encode()).decode()

    conn.request("GET", "/api/rest/authenticate", headers=headers)
    res = conn.getresponse()

    if res.status != 200:
        raise Exception(f"Authentication failed with status {res.status}")

    data = res.read()
    token = json.loads(data.decode("utf-8"))["result"]
    os.environ["AURORA_ACCESS_TOKEN"] = token
    return token


def process_aurora_data(data_json: dict) -> pd.DataFrame:
    """
    Process Aurora Vision data and convert it to a DataFrame with timestamp and power_kw columns.
    """
    records = [
        {
            "timestamp": datetime.strptime(entry["date"], "%Y%m%d").replace(tzinfo=timezone.utc),
            "power_kw": entry.get("dailyProduction", 0) / 24.0,  # Assume evenly distributed power production over 24 hours
        }
        for entry in data_json.get("result", [])
    ]
    return pd.DataFrame(records)


def get_aurora_vision_data(settings: AuroraVisionSettings, ts: pd.Timestamp) -> pd.DataFrame:
    """
    Fetch data from Aurora Vision API for a specific day.
    """
    token = os.getenv("AURORA_ACCESS_TOKEN")
    if not token:
        token = authenticate_aurora(settings)

    start_date = ts.strftime("%Y%m%d")
    end_date = start_date  # Single day query

    conn = http.client.HTTPSConnection("api.auroravision.net")
    headers = {"X-AuroraVision-Token": token}
    url = f"/api/rest/v1/plant/{settings.plant_id}/dailyProduction?startDate={start_date}&endDate={end_date}"

    conn.request("GET", url, headers=headers)
    res = conn.getresponse()

    if res.status == 401:  # Handle token expiration
        token = authenticate_aurora(settings)
        headers["X-AuroraVision-Token"] = token
        conn.request("GET", url, headers=headers)
        res = conn.getresponse()

    if res.status != 200:
        raise Exception(f"Failed to fetch data: HTTP {res.status}")

    data = res.read()
    data_json = json.loads(data.decode("utf-8"))

    return process_aurora_data(data_json)