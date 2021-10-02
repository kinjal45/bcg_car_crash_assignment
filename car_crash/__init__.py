import logging
import car_crash.analysis
import car_crash.utils

logging.basicConfig(
    format="%(asctime)s %(levelname)s: %(message)s",
    level=logging.INFO,
    datefmt="%Y-%m-%d %H:%M:%S",
)
logging.getLogger("py4j").setLevel(logging.INFO)
