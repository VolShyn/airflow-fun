import json
import logging

from common.config import DATA_DIR

log = logging.getLogger(__name__)

# external storage layout: DATA_DIR/<stage>/<city>/<ds>.json
# each etl step writes its own stage, the next step reads the previous one.
# a deterministic path per (stage, city, ds) is what makes steps resumable:
# a re-run finds the file already there and skips the work.


def _path(stage, city, ds):
    return DATA_DIR / stage / city.lower() / f"{ds}.json"


def write_stage(stage, city, ds, payload):
    p = _path(stage, city, ds)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(payload))
    log.info("wrote %s", p)


def read_stage(stage, city, ds):
    return json.loads(_path(stage, city, ds).read_text())


def stage_exists(stage, city, ds):
    return _path(stage, city, ds).exists()
