import os
import urllib.error
import urllib.request
from datetime import datetime
from logging import getLogger
from pathlib import Path

FILENAME_KEY = {
    "notes": "notes",
    "noteRatings": "ratings",
    "noteStatusHistory": "noteStatusHistory",
    "userEnrollment": "userEnrollment",
}
BASE_URL = "https://ton.twimg.com/birdwatch-public-data/"


class BirdwatchHandlerProps:
    def __init__(self, output_dir: str, handle_name: str):
        self.output_dir = output_dir
        self.handle_name = handle_name

    def to_dict(self):
        return {
            "output_dir": self.output_dir,
            "handle_name": self.handle_name,
        }

    @staticmethod
    def from_dict(data: dict):
        return BirdwatchHandlerProps(
            output_dir=data["output_dir"],
            handle_name=data["handle_name"],
        )


class BirdwatchHandler:
    def __init__(self, props: BirdwatchHandlerProps):
        self.props = props
        self.logger = getLogger(__name__)

    def fetch(self):
        output_path = os.path.join(self.props.output_dir, self.props.handle_name)
        if not Path(output_path).exists():
            Path(output_path).parent.mkdir(parents=True, exist_ok=True)
        for key, value in FILENAME_KEY.items():
            index = 0
            while True:
                url = f"{BASE_URL}{datetime.today().strftime('%Y/%m/%d')}/{key}/{value}-{str(index).zfill(5)}.tsv"
                self.logger.info("Downloading: %s", url)
                if not os.path.exists(
                    os.path.join(output_path, f"{value}-{str(index).zfill(5)}.tsv")
                ):
                    try:
                        urllib.request.urlretrieve(
                            url,
                            os.path.join(
                                output_path, f"{value}-{str(index).zfill(5)}.tsv"
                            ),
                        )
                    except urllib.error.HTTPError as ex:
                        self.logger.error(ex)
                        break
                index += 1
