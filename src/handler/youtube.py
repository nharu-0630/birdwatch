import json
import os
from datetime import date, datetime, timedelta
from logging import getLogger
from pathlib import Path
from time import sleep
from urllib import parse

import requests


class YouTubeHandlerProps:
    def __init__(
        self,
        output_dir: str,
        handle_name: str,
        api_key: str,
        channel_ids: list[str],
        delta_days: int,
        period_days: int,
        request_count: int,
        comment_request_count: int,
    ):
        self.output_dir = output_dir
        self.handle_name = handle_name
        self.api_key = api_key
        self.channel_ids = channel_ids
        self.delta_days = delta_days
        self.period_days = period_days
        self.request_count = request_count
        self.comment_request_count = comment_request_count

    def to_dict(self):
        return {
            "output_dir": self.output_dir,
            "handle_name": self.handle_name,
            "api_key": self.api_key,
            "channel_ids": self.channel_ids,
            "delta_days": self.delta_days,
            "period_days": self.period_days,
            "request_count": self.request_count,
            "comment_request_count": self.comment_request_count,
        }

    @staticmethod
    def from_dict(props):
        return YouTubeHandlerProps(
            props["output_dir"],
            props["handle_name"],
            props["api_key"],
            props["channel_ids"],
            props["delta_days"],
            props["period_days"],
            props["request_count"],
            props["comment_request_count"],
        )


class YouTubeHandler:
    def __init__(self, props: YouTubeHandlerProps):
        self.props = props
        self.logger = getLogger(__name__)

    def fetch(self):
        for channel_id in self.props.channel_ids:
            self.__fetch_channel(channel_id)

    def __fetch_channel(self, channel_id: str):
        published_after = (
            datetime.today()
            - timedelta(days=self.props.delta_days)
            - timedelta(days=self.props.period_days)
        )
        published_before = datetime.now() - timedelta(days=self.props.delta_days)
        page_token = None
        for _ in range(self.props.request_count):
            sleep(5)
            res = self.__search_by_channel_id(
                channel_id, page_token, published_after, published_before
            )
            self.__append_to_output(res, "raw")
            for item in res["items"]:
                try:
                    video_id = item["id"]["videoId"]
                    if self.__is_fetched(video_id):
                        continue
                    self.__append_to_fetched(video_id)
                    detail = self.__video_by_id(video_id)
                    self.__append_to_output(detail, "detail_raw")
                    video = {
                        "video": {
                            "id": video_id,
                            "title": detail["items"][0]["snippet"]["title"],
                            "description": detail["items"][0]["snippet"]["description"],
                            "channel_title": detail["items"][0]["snippet"][
                                "channelTitle"
                            ],
                            "category_id": detail["items"][0]["snippet"]["categoryId"],
                            "view_count": detail["items"][0]["statistics"]["viewCount"],
                            "like_count": detail["items"][0]["statistics"]["likeCount"],
                            "favorite_count": detail["items"][0]["statistics"][
                                "favoriteCount"
                            ],
                        },
                        "comments": self.__fetch_comments(video_id),
                        "timestamp": datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ"),
                    }
                    if "tags" in detail["items"][0]["snippet"]:
                        video["video"]["tags"] = detail["items"][0]["snippet"]["tags"]
                    if "commentCount" in detail["items"][0]["statistics"]:
                        video["video"]["comment_count"] = detail["items"][0][
                            "statistics"
                        ]["commentCount"]
                    self.__append_to_output(video)
                except Exception as ex:
                    self.logger.error(ex)
                    continue
            if "nextPageToken" in res:
                page_token = res["nextPageToken"]
            else:
                page_token = None
            if page_token is None:
                break

    def __fetch_comments(self, video_id: str):
        comments = []
        comment_ids = []
        page_token = None
        for _ in range(self.props.comment_request_count):
            sleep(5)
            res = self.__comment_threads_by_video_id(video_id)
            self.__append_to_output(res, "comment_raw")
            for item in res["items"]:
                try:
                    comment_id = item["id"]
                    text = item["snippet"]["topLevelComment"]["snippet"]["textDisplay"]
                    author_display_name = item["snippet"]["topLevelComment"]["snippet"][
                        "authorDisplayName"
                    ]
                    author_channel_id = item["snippet"]["topLevelComment"]["snippet"][
                        "authorChannelId"
                    ]["value"]
                    like_count = item["snippet"]["topLevelComment"]["snippet"][
                        "likeCount"
                    ]
                    published_at = item["snippet"]["topLevelComment"]["snippet"][
                        "publishedAt"
                    ]
                    updated_at = item["snippet"]["topLevelComment"]["snippet"][
                        "updatedAt"
                    ]
                    if comment_id in comment_ids:
                        continue
                    comment_ids.append(comment_id)
                    comments.append(
                        {
                            "comment_id": comment_id,
                            "text": text,
                            "author_display_name": author_display_name,
                            "author_channel_id": author_channel_id,
                            "like_count": like_count,
                            "published_at": published_at,
                            "updated_at": updated_at,
                        }
                    )
                except Exception as ex:
                    self.logger.error(ex)
                    continue
            if "nextPageToken" in res:
                page_token = res["nextPageToken"]
            else:
                page_token = None
            if page_token is None:
                break
        return comments

    def __search_by_channel_id(
        self,
        channel_id: str,
        page_token: str | None = None,
        published_after: datetime | None = None,
        published_before: datetime | None = None,
    ):
        url = "https://www.googleapis.com/youtube/v3/search?"
        payload = {
            "key": self.props.api_key,
            "part": "id,snippet",
            "channelId": channel_id,
            "maxResults": 50,
            "order": "date",
            "pageToken": "" if page_token is None else page_token,
            "type": "video",
        }
        if published_after is not None:
            payload["publishedAfter"] = published_after.strftime("%Y-%m-%dT%H:%M:%SZ")
        if published_before is not None:
            payload["publishedBefore"] = published_before.strftime("%Y-%m-%dT%H:%M:%SZ")
        url += parse.urlencode(payload)
        content = requests.request("GET", url)
        self.logger.info(f"Request: {url}")
        res = json.loads(content.text)
        return res

    def __video_by_id(self, video_id: str):
        url = "https://www.googleapis.com/youtube/v3/videos?"
        payload = {
            "key": self.props.api_key,
            "part": "contentDetails,id,liveStreamingDetails,localizations,player,recordingDetails,snippet,statistics,status,topicDetails",
            "id": video_id,
        }
        url += parse.urlencode(payload)
        content = requests.request("GET", url)
        self.logger.info(f"Request: {url}")
        res = json.loads(content.text)
        return res

    def __comment_threads_by_video_id(
        self, video_id: str, page_token: str | None = None
    ):
        url = "https://www.googleapis.com/youtube/v3/commentThreads?"
        payload = {
            "key": self.props.api_key,
            "part": "id,snippet,replies",
            "videoId": video_id,
            "maxResults": 100,
            "order": "relevance",
            "pageToken": "" if page_token is None else page_token,
            "textFormat": "plainText",
        }
        url += parse.urlencode(payload)
        content = requests.request("GET", url)
        self.logger.info(f"Request: {url}")
        res = json.loads(content.text)
        return res

    def __append_to_output(self, data: dict, suffix: str | None = None):
        output_path = os.path.join(
            self.props.output_dir,
            self.props.handle_name,
            f"{date.today()}{'_' + suffix if suffix is not None else ''}.jsonl",
        )
        if not Path(output_path).exists():
            Path(output_path).parent.mkdir(parents=True, exist_ok=True)
            Path(output_path).touch()
        with open(output_path, "a") as f:
            json.dump(data, f, ensure_ascii=False)
            f.write("\n")

    def __is_fetched(self, id_str: str, suffix: str | None = None):
        fetched_output_path = os.path.join(
            self.props.output_dir,
            self.props.handle_name,
            f"fetched{'_' + suffix if suffix is not None else ''}.yaml",
        )
        if not Path(fetched_output_path).exists():
            return False
        with open(fetched_output_path, "r") as f:
            fetched_ids = f.read().splitlines()
        return id_str in fetched_ids

    def __append_to_fetched(self, id_str: str, suffix: str | None = None):
        fetched_output_path = os.path.join(
            self.props.output_dir,
            self.props.handle_name,
            f"fetched{'_' + suffix if suffix is not None else ''}.yaml",
        )
        if not Path(fetched_output_path).exists():
            Path(fetched_output_path).parent.mkdir(parents=True, exist_ok=True)
            Path(fetched_output_path).touch()
        with open(fetched_output_path, "a") as f:
            f.write(id_str + "\n")
