import logging
from datetime import datetime

import requests
from airflow.sdk import Variable

from common import storage

log = logging.getLogger(__name__)

API = "https://api.openweathermap.org/data/3.0/onecall/timemachine"


def extract(city, lat, lon, ds, **_):
    # resume guard: skip if raw already landed for this (city, ds)
    if storage.stage_exists("raw", city, ds):
        log.info("raw exists for %s %s, skipping extract", city, ds)
        return
    # ds (YYYY-MM-DD) -> unix dt so historical backfills return the right date
    dt = int(datetime.strptime(ds, "%Y-%m-%d").timestamp())
    resp = requests.get(
        API,
        params={"lat": lat, "lon": lon, "dt": dt, "appid": Variable.get("WEATHER_API_KEY")},
        timeout=10,
    )
    resp.raise_for_status()
    storage.write_stage("raw", city, ds, resp.json())


def transform(city, ds, **_):
    if storage.stage_exists("transformed", city, ds):
        log.info("transformed exists for %s %s, skipping transform", city, ds)
        return
    raw = storage.read_stage("raw", city, ds)
    # timemachine returns a "data" array, we take the first (and only) element
    rec = raw["data"][0]
    storage.write_stage(
        "transformed",
        city,
        ds,
        {
            "timestamp": rec["dt"],
            "city": city,
            "temp": rec["temp"],
            "humidity": rec["humidity"],
            "cloudiness": rec["clouds"],
            "wind_speed": rec["wind_speed"],
        },
    )


def quality_check(city, ds, **_):
    # dq reads the transformed stage and asserts plausible ranges.
    # a failed assertion fails the task, leaving prior stages on disk.
    rec = storage.read_stage("transformed", city, ds)
    assert rec["temp"] is not None, "temp is missing"
    assert rec["humidity"] is not None and 0 <= rec["humidity"] <= 100, "humidity out of range"
    assert rec["wind_speed"] is not None and rec["wind_speed"] >= 0, "wind_speed invalid"
    log.info("dq passed for %s %s", city, ds)


def load(city, ds, **_):
    if storage.stage_exists("final", city, ds):
        log.info("final exists for %s %s, skipping load", city, ds)
        return
    # final dataset = dq-validated transformed record
    rec = storage.read_stage("transformed", city, ds)
    storage.write_stage("final", city, ds, rec)
