import json
import os
import time
from datetime import timedelta
from glob import glob
from logging import getLogger
from pathlib import Path

import pandas as pd
from twitter.scraper import Scraper


class BirdwatchRefBatchProps:
    def __init__(self, input_dir: str, output_dir: str, cookie_path: str):
        self.input_dir = input_dir
        self.output_dir = output_dir
        self.cookie_path = cookie_path

    def to_dict(self):
        return {
            "input_dir": self.input_dir,
            "output_dir": self.output_dir,
            "cookie_path": self.cookie_path,
        }

    @staticmethod
    def from_dict(data: dict):
        return BirdwatchRefBatchProps(
            input_dir=data["input_dir"],
            output_dir=data["output_dir"],
            cookie_path=data["cookie_path"],
        )


BATCH_SIZE = 220
RATE_LIMIT = 500


class BirdwatchRefBatch:
    def __init__(self, props: BirdwatchRefBatchProps):
        self.props = props
        self.logger = getLogger(__name__)

    def run(self):
        notes_path_list = glob(
            os.path.join(self.props.input_dir, "notes-*.json"), recursive=False
        )
        notes_data_list = []
        for notes_path in notes_path_list:
            with open(notes_path, "rb") as file:
                if file.read(2) == b"\x1f\x8b":
                    notes_data_list.append(
                        pd.read_csv(notes_path, delimiter="\t", compression="gzip")
                    )
                else:
                    notes_data_list.append(pd.read_csv(notes_path, delimiter="\t"))
        notes_data = pd.concat(notes_data_list)
        # ---
        # Ignore not contains Japanese
        notes_data = notes_data[
            notes_data["summary"].str.contains("[\u3041-\u309F]+", na=False)
        ]
        # Ignore fetched tweet
        fetched_output_path = os.path.join(self.props.output_dir, "fetched.jsonl")
        if not Path(fetched_output_path).exists():
            Path(fetched_output_path).parent.mkdir(parents=True, exist_ok=True)
            Path(fetched_output_path).touch()
        with open(fetched_output_path, "r") as f:
            fetched_output_data = f.read().splitlines()
        notes_data = notes_data[~notes_data["tweetId"].isin(fetched_output_data)]
        # ---
        with open(self.props.cookie_path, "r") as f:
            cookie = json.load(f)
        scraper = Scraper(
            cookies={
                "ct0": cookie["ct0"],
                "auth_token": cookie["auth_token"],
            },
            out=self.props.output_dir,
        )
        last_fetched_at = None
        for i in range(0, len(notes_data), BATCH_SIZE):
            if last_fetched_at is not None:
                elapsed_time = time.time() - last_fetched_at
                sleep_time = max(
                    0,
                    (timedelta(minutes=15).total_seconds() / RATE_LIMIT) - elapsed_time,
                )
                time.sleep(sleep_time)
            end_index = min(i + BATCH_SIZE, len(notes_data))
            batch_ids = notes_data["tweetId"].iloc[i:end_index].tolist()
            scraper.tweets_by_ids(batch_ids)
            last_fetched_at = time.time()
            with open(fetched_output_path, "a") as f:
                for fetch_id in batch_ids:
                    f.write(f"{fetch_id}\n")
