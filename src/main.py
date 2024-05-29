import logging
import os
import threading
import time
from datetime import datetime
from os.path import abspath, dirname, join
from pathlib import Path

import schedule
from dotenv import load_dotenv

from handler.birdwatch import BirdwatchHandler, BirdwatchHandlerProps
from handler.twitter import TwitterHandler, TwitterHandlerProps
from handler.youtube import YouTubeHandler, YouTubeHandlerProps


def run_concurrently(job):
    threading.Thread(target=job).start()


def get_youtube_handler() -> YouTubeHandler:
    output_dir = os.path.join(str(os.environ.get("RAW_DATA_DIR")), "YouTube")
    props = YouTubeHandlerProps(
        output_dir=output_dir,
        handle_name="PressChannel",
        api_key=str(os.environ.get("YOUTUBE_API_KEY")),
        channel_ids=[
            "UCGCZAYq5Xxojl_tSXcVJhiQ",  # ANNnewsCH
            "UC6AG81pAkf6Lbi_1VC5NmPA",  # TBS NEWS DIG Powered by JNN
            "UCuTAXTexrhetbOe3zgskJBQ",  # 日テレNEWS
            "UCkKVQ_GNjd8FbAuT6xDcWgg",  # テレ東BIZ
            "UCMKvT0YVLufHMdGLH89J1oA",  # 朝日新聞デジタル
            "UCkKJhKO73xF1pK5h9R82ZGQ",  # MBS NEWS
            "UCoQBJMzcwmXrRSHBFAlTsIw",  # FNNプライムオンライン
        ],
        delta_days=3,
        period_days=1,
        request_count=3,
        comment_request_count=50,
    )
    return YouTubeHandler(props)


def get_twitter_handler() -> TwitterHandler:
    output_dir = os.path.join(str(os.environ.get("RAW_DATA_DIR")), "Twitter")
    props = TwitterHandlerProps(
        output_dir=output_dir,
        handle_name="PressUser",
        cookie_path=str(os.environ.get("TWITTER_COOKIE_PATH")),
        screen_names=["YahooNewsTopics", "livedoornews", "nhk_news"],
        delta_days=3,
        period_hours=3,
        min_retweet=30,
        min_favorite=30,
        quote_min_retweet=0,
        quote_min_favorite=0,
        request_count=3,
        quote_request_count=10,
    )
    return TwitterHandler(props)


def get_birdwatch_handler() -> BirdwatchHandler:
    output_dir = os.path.join(str(os.environ.get("RAW_DATA_DIR")), "Birdwatch")
    props = BirdwatchHandlerProps(output_dir=output_dir, handle_name="Birdwatch")
    return BirdwatchHandler(props)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s %(name)s:%(lineno)s %(funcName)s [%(levelname)s]: %(message)s",
    )

    dir_path = dirname(abspath("__file__"))
    dotenv_path = join(dir_path, ".env")
    load_dotenv(dotenv_path, verbose=True)

    youtube_handler = get_youtube_handler()
    schedule.every(4).hours.do(run_concurrently, youtube_handler.fetch)

    twitter_handler = get_twitter_handler()
    schedule.every(3).hours.do(run_concurrently, twitter_handler.fetch)

    birdwatch_handler = get_birdwatch_handler()
    schedule.every(4).hours.do(run_concurrently, birdwatch_handler.fetch)

    threads = []
    threads.append(threading.Thread(target=youtube_handler.fetch))
    threads.append(threading.Thread(target=twitter_handler.fetch))
    threads.append(threading.Thread(target=birdwatch_handler.fetch))

    for thread in threads:
        thread.start()

    for thread in threads:
        thread.join()

    while True:
        schedule.run_pending()
        time.sleep(1)
