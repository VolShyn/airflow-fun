import os
from pathlib import Path

# external storage root on the local filesystem.
# override with the WEATHER_DATA_DIR env var if /tmp is not desired.
DATA_DIR = Path(os.environ.get("WEATHER_DATA_DIR", "/tmp/weather_hw3"))

# the first city is also used by the http sensor to probe api availability
CITIES = [
    {"name": "Lviv", "lat": 49.8397, "lon": 24.0297},
    {"name": "Kyiv", "lat": 50.4501, "lon": 30.5234},
    {"name": "Kharkiv", "lat": 49.9935, "lon": 36.2304},
    {"name": "Odesa", "lat": 46.4825, "lon": 30.7233},
    {"name": "Zhmerynka", "lat": 49.0391, "lon": 28.1112},
]

# this city is handled by the cross-dag split (ingestion -> processing);
# the factory generates single-dag pipelines for the rest.
CROSS_DAG_CITY = "Lviv"

# default wind threshold (m/s) for the data quality "plausible" range
WIND_ALERT_THRESHOLD = 10.0
