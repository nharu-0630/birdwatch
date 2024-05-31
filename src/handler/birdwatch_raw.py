import os
import urllib.error
import urllib.request
from datetime import date, datetime, timedelta
from logging import getLogger
from pathlib import Path

FILENAME_KEY = {
    "notes": "notes",
    "noteRatings": "ratings",
    "noteStatusHistory": "noteStatusHistory",
    "userEnrollment": "userEnrollment",
}
BASE_URL = "https://ton.twimg.com/birdwatch-public-data/"


class BirdwatchRawHandlerProps:
    def __init__(self, output_dir: str):
        self.output_dir = output_dir

    def to_dict(self):
        return {
            "output_dir": self.output_dir,
        }

    @staticmethod
    def from_dict(data: dict):
        return BirdwatchRawHandlerProps(
            output_dir=data["output_dir"],
        )


class BirdwatchRawHandler:
    def __init__(self, props: BirdwatchRawHandlerProps):
        self.props = props
        self.logger = getLogger(__name__)

    def fetch(self):
        self.__fetch_target_date(date.today())
        self.__fetch_target_date(date.today() - timedelta(days=1))

    def __fetch_target_date(self, target_date: date):
        output_path = os.path.join(self.props.output_dir, str(target_date))
        if not Path(output_path).exists():
            Path(output_path).mkdir(parents=True, exist_ok=True)
        for key, value in FILENAME_KEY.items():
            index = 0
            while True:
                url = f"{BASE_URL}{target_date.strftime('%Y/%m/%d')}/{key}/{value}-{str(index).zfill(5)}.tsv"
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
