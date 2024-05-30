import datetime
import json
import logging
import os
import re
import time
from datetime import date, timedelta
from logging import getLogger
from pathlib import Path

import requests
from requests.cookies import RequestsCookieJar

logger = getLogger(__name__)

USER_AGENT = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36"
CLIENT_UUID = "a0f767a2-2b96-4667-b672-b70cf9e2acc8"
BEARER_TOKEN = "AAAAAAAAAAAAAAAAAAAAANRILgAAAAAAnNwIzUejRCOuH5E6I8xnZz4puTs%3D1Zv7ttfk8LF81IUq16cHjhLTvJu4FA33AGWWjCpTnA"
TWEET_COUNT = 20


class TwitterQuoteHandlerProps:
    def __init__(
        self,
        output_dir: str,
        handle_name: str,
        cookie_path: str,
        screen_names: list[str],
        delta_days: int,
        period_hours: int,
        min_retweet: int,
        min_favorite: int,
        quote_min_retweet: int,
        quote_min_favorite: int,
        request_count: int,
        quote_request_count: int,
    ):
        self.output_dir = output_dir
        self.handle_name = handle_name
        self.cookie_path = cookie_path
        self.screen_names = screen_names
        self.delta_days = delta_days
        self.period_hours = period_hours
        self.min_retweet = min_retweet
        self.min_favorite = min_favorite
        self.quote_min_retweet = quote_min_retweet
        self.quote_min_favorite = quote_min_favorite
        self.request_count = request_count
        self.quote_request_count = quote_request_count

    def to_dict(self):
        return {
            "output_dir": self.output_dir,
            "handle_name": self.handle_name,
            "cookie_path": self.cookie_path,
            "screen_names": self.screen_names,
            "delta_days": self.delta_days,
            "period_hours": self.period_hours,
            "min_retweet": self.min_retweet,
            "min_favorite": self.min_favorite,
            "quote_min_retweet": self.quote_min_retweet,
            "quote_min_favorite": self.quote_min_favorite,
            "request_count": self.request_count,
            "quote_request_count": self.quote_request_count,
        }

    @staticmethod
    def from_dict(props: dict):
        return TwitterQuoteHandlerProps(
            output_dir=props["output_dir"],
            handle_name=props["handle_name"],
            cookie_path=props["cookie_path"],
            screen_names=props["screen_names"],
            delta_days=props["delta_days"],
            period_hours=props["period_hours"],
            min_retweet=props["min_retweet"],
            min_favorite=props["min_favorite"],
            quote_min_retweet=props["quote_min_retweet"],
            quote_min_favorite=props["quote_min_favorite"],
            request_count=props["request_count"],
            quote_request_count=props["quote_request_count"],
        )


class TwitterQuoteHandler:
    def __init__(self, props: TwitterQuoteHandlerProps):
        self.props = props
        self.logger = getLogger(__name__)

    def fetch(self):
        for screen_name in self.props.screen_names:
            self.__fetch_user_tweets(screen_name)

    def __fetch_user_tweets(self, screen_name: str):
        since_datetime = (
            datetime.datetime.now()
            - timedelta(days=self.props.delta_days)
            - timedelta(hours=self.props.period_hours)
        )
        until_datetime = datetime.datetime.now() - timedelta(days=self.props.delta_days)
        with open(self.props.cookie_path, "r") as f:
            cookies_dict = json.load(f)
        cookies = RequestsCookieJar()
        for key, value in cookies_dict.items():
            cookies.set(key, value)

        cursor = None
        for _ in range(self.props.request_count):
            time.sleep(15)
            query = (
                "From:"
                + screen_name
                + " min_retweets:"
                + str(self.props.min_retweet)
                + " min_faves:"
                + str(self.props.min_favorite)
                + " since:"
                + since_datetime.strftime("%Y-%m-%d_%H:%M:%S_UTC")
                + " until:"
                + until_datetime.strftime("%Y-%m-%d_%H:%M:%S_UTC")
                + '","count":'
                + str(TWEET_COUNT)
            )
            url = (
                'https://twitter.com/i/api/graphql/lZ0GCEojmtQfiUQa5oJSEw/SearchTimeline?variables={"rawQuery":"'
                + query
                + ((',"cursor":"' + cursor + '"') if cursor is not None else "")
                + ',"querySource":"typed_query","product":"Top"}&features={"responsive_web_graphql_exclude_directive_enabled":true,"verified_phone_label_enabled":false,"responsive_web_home_pinned_timelines_enabled":true,"creator_subscriptions_tweet_preview_api_enabled":true,"responsive_web_graphql_timeline_navigation_enabled":true,"responsive_web_graphql_skip_user_profile_image_extensions_enabled":false,"c9s_tweet_anatomy_moderator_badge_enabled":true,"tweetypie_unmention_optimization_enabled":true,"responsive_web_edit_tweet_api_enabled":true,"graphql_is_translatable_rweb_tweet_is_translatable_enabled":true,"view_counts_everywhere_api_enabled":true,"longform_notetweets_consumption_enabled":true,"responsive_web_twitter_article_tweet_consumption_enabled":false,"tweet_awards_web_tipping_enabled":false,"freedom_of_speech_not_reach_fetch_enabled":true,"standardized_nudges_misinfo":true,"tweet_with_visibility_results_prefer_gql_limited_actions_policy_enabled":true,"longform_notetweets_rich_text_read_enabled":true,"longform_notetweets_inline_media_enabled":true,"responsive_web_media_download_video_enabled":false,"responsive_web_enhance_cards_enabled":false}'
            )
            headers = {
                "authority": "twitter.com",
                "accept": "*/*",
                "accept-language": "ja,en-US;q=0.9,en;q=0.8",
                "authorization": "Bearer " + BEARER_TOKEN,
                "content-type": "application/json",
                "dnt": "1",
                "referer": "https://twitter.com/search?q="
                + query
                + "&src=typed_query&f=top",
                "user-agent": USER_AGENT,
                "x-client-uuid": CLIENT_UUID,
                "x-csrf-token": cookies_dict["ct0"],
                "x-twitter-active-user": "yes",
                "x-twitter-auth-type": "OAuth2Session",
                "x-twitter-client-language": "ja",
            }
            content = requests.request(
                "GET", url, headers=headers, data={}, cookies=cookies
            )
            self.logger.info(f"Request: {url}")
            res = json.loads(content.text)
            raw_output_path = os.path.join(
                self.props.output_dir,
                self.props.handle_name,
                f"{date.today()}_raw.jsonl",
            )
            if not Path(raw_output_path).exists():
                Path(raw_output_path).parent.mkdir(parents=True, exist_ok=True)
                Path(raw_output_path).touch()
            with open(raw_output_path, "a") as f:
                json.dump(res, f, ensure_ascii=False)
                f.write("\n")
            for entry in res["data"]["search_by_raw_query"]["search_timeline"][
                "timeline"
            ]["instructions"][0]["entries"]:
                try:
                    if "itemContent" not in entry["content"]:
                        continue
                    if "tweet_results" not in entry["content"]["itemContent"]:
                        continue
                    tweet_result_legacy = entry["content"]["itemContent"][
                        "tweet_results"
                    ]["result"]["legacy"]
                    user_result_legacy = entry["content"]["itemContent"][
                        "tweet_results"
                    ]["result"]["core"]["user_results"]["result"]["legacy"]

                    user_id_str = tweet_result_legacy["user_id_str"]
                    name = user_result_legacy["name"]
                    screen_name = user_result_legacy["screen_name"]
                    description = user_result_legacy["description"]
                    followers_count = user_result_legacy["followers_count"]
                    friends_count = user_result_legacy["friends_count"]
                    listed_count = user_result_legacy["listed_count"]
                    user_created_at = user_result_legacy["created_at"]

                    id_str = tweet_result_legacy["id_str"]
                    full_text = tweet_result_legacy["full_text"]
                    created_at = tweet_result_legacy["created_at"]
                    quote_count = tweet_result_legacy["quote_count"]
                    reply_count = tweet_result_legacy["reply_count"]
                    retweet_count = tweet_result_legacy["retweet_count"]
                    favorite_count = tweet_result_legacy["favorite_count"]
                    bookmark_count = tweet_result_legacy["bookmark_count"]
                    views_count = entry["content"]["itemContent"]["tweet_results"][
                        "result"
                    ]["views"]["count"]

                    expanded_url = ""
                    if "urls" in tweet_result_legacy["entities"]:
                        if len(tweet_result_legacy["entities"]["urls"]) > 0:
                            expanded_url = tweet_result_legacy["entities"]["urls"][0][
                                "expanded_url"
                            ]
                    fetched_output_path = os.path.join(
                        self.props.output_dir,
                        self.props.handle_name,
                        "fetched.jsonl",
                    )
                    if not Path(fetched_output_path).exists():
                        Path(fetched_output_path).parent.mkdir(
                            parents=True, exist_ok=True
                        )
                        Path(fetched_output_path).touch()
                    with open(fetched_output_path, "r") as f:
                        fetched_ids = f.read().splitlines()
                    if id_str in fetched_ids:
                        continue
                    with open(fetched_output_path, "a") as f:
                        f.write(id_str + "\n")

                    headline = {}
                    if expanded_url.startswith("https://news.yahoo.co.jp/pickup/"):
                        headline = self.__fetch_pickup_yahoo_news(expanded_url)

                    timestamp = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ")

                    referer = (
                        "https://twitter.com/search?q="
                        + query
                        + "&src=typed_query&f=top"
                    )
                    quotes = self.__fetch_quote_tweet(referer, id_str)

                    tweet = {
                        "user": {
                            "user_id_str": user_id_str,
                            "name": name,
                            "screen_name": screen_name,
                            "description": description,
                            "followers_count": followers_count,
                            "friends_count": friends_count,
                            "listed_count": listed_count,
                            "user_created_at": user_created_at,
                        },
                        "tweet": {
                            "id_str": id_str,
                            "full_text": full_text,
                            "created_at": created_at,
                            "quote_count": quote_count,
                            "reply_count": reply_count,
                            "retweet_count": retweet_count,
                            "favorite_count": favorite_count,
                            "bookmark_count": bookmark_count,
                            "views_count": views_count,
                            "expanded_url": expanded_url,
                        },
                        "quotes": quotes,
                        "headline": headline,
                        "timestamp": timestamp,
                    }
                    output_path = os.path.join(
                        self.props.output_dir,
                        self.props.handle_name,
                        f"{date.today()}.jsonl",
                    )
                    if not Path(output_path).exists():
                        Path(output_path).parent.mkdir(parents=True, exist_ok=True)
                        Path(output_path).touch()
                    with open(output_path, "a") as f:
                        json.dump(tweet, f, ensure_ascii=False)
                        f.write("\n")
                except Exception as ex:
                    self.logger.error(ex)
                    continue
            bottom = res["data"]["search_by_raw_query"]["search_timeline"]["timeline"][
                "instructions"
            ][0]["entries"][-1]
            if "entryId" in bottom:
                if bottom["entryId"] == "cursor-bottom-0":
                    if cursor == bottom["content"]["value"]:
                        cursor = None
                        break
                    cursor = bottom["content"]["value"]
                else:
                    cursor = None
            else:
                cursor = None
            if cursor is None:
                break

    def __fetch_quote_tweet(self, referer, tweet_id):
        with open(self.props.cookie_path, "r") as f:
            cookies_dict = json.load(f)
        cookies = RequestsCookieJar()
        for key, value in cookies_dict.items():
            cookies.set(key, value)

        quotes = []

        cursor = None
        for _ in range(self.props.quote_request_count):
            time.sleep(15)
            url = (
                'https://twitter.com/i/api/graphql/lZ0GCEojmtQfiUQa5oJSEw/SearchTimeline?variables={"rawQuery":"quoted_tweet_id:'
                + tweet_id
                + " min_retweets:"
                + str(self.props.quote_min_retweet)
                + " min_faves:"
                + str(self.props.quote_min_favorite)
                + '","count":'
                + str(TWEET_COUNT)
                + ((',"cursor":"' + cursor + '"') if cursor is not None else "")
                + ',"querySource":"tdqt","product":"Top"}&features={"responsive_web_graphql_exclude_directive_enabled":true,"verified_phone_label_enabled":false,"responsive_web_home_pinned_timelines_enabled":true,"creator_subscriptions_tweet_preview_api_enabled":true,"responsive_web_graphql_timeline_navigation_enabled":true,"responsive_web_graphql_skip_user_profile_image_extensions_enabled":false,"c9s_tweet_anatomy_moderator_badge_enabled":true,"tweetypie_unmention_optimization_enabled":true,"responsive_web_edit_tweet_api_enabled":true,"graphql_is_translatable_rweb_tweet_is_translatable_enabled":true,"view_counts_everywhere_api_enabled":true,"longform_notetweets_consumption_enabled":true,"responsive_web_twitter_article_tweet_consumption_enabled":false,"tweet_awards_web_tipping_enabled":false,"freedom_of_speech_not_reach_fetch_enabled":true,"standardized_nudges_misinfo":true,"tweet_with_visibility_results_prefer_gql_limited_actions_policy_enabled":true,"longform_notetweets_rich_text_read_enabled":true,"longform_notetweets_inline_media_enabled":true,"responsive_web_media_download_video_enabled":false,"responsive_web_enhance_cards_enabled":false}'
            )
            headers = {
                "authority": "twitter.com",
                "accept": "*/*",
                "accept-language": "ja,en-US;q=0.9,en;q=0.8",
                "authorization": "Bearer " + BEARER_TOKEN,
                "content-type": "application/json",
                "dnt": "1",
                "referer": referer,
                "user-agent": USER_AGENT,
                "x-client-uuid": CLIENT_UUID,
                "x-csrf-token": cookies_dict["ct0"],
                "x-twitter-active-user": "yes",
                "x-twitter-auth-type": "OAuth2Session",
                "x-twitter-client-language": "ja",
            }
            content = requests.request(
                "GET", url, headers=headers, data={}, cookies=cookies
            )
            self.logger.info(f"Request: {url}")
            res = json.loads(content.text)
            raw_output_path = os.path.join(
                self.props.output_dir,
                self.props.handle_name,
                f"{date.today()}_raw.jsonl",
            )
            if not Path(raw_output_path).exists():
                Path(raw_output_path).parent.mkdir(parents=True, exist_ok=True)
                Path(raw_output_path).touch()
            with open(raw_output_path, "a") as f:
                json.dump(res, f, ensure_ascii=False)
                f.write("\n")
            for entry in res["data"]["search_by_raw_query"]["search_timeline"][
                "timeline"
            ]["instructions"][0]["entries"]:
                try:
                    if "itemContent" not in entry["content"]:
                        continue
                    if "tweet_results" not in entry["content"]["itemContent"]:
                        continue
                    tweet_result_legacy = entry["content"]["itemContent"][
                        "tweet_results"
                    ]["result"]["legacy"]
                    user_result_legacy = entry["content"]["itemContent"][
                        "tweet_results"
                    ]["result"]["core"]["user_results"]["result"]["legacy"]

                    user_id_str = tweet_result_legacy["user_id_str"]
                    name = user_result_legacy["name"]
                    screen_name = user_result_legacy["screen_name"]
                    description = user_result_legacy["description"]
                    followers_count = user_result_legacy["followers_count"]
                    friends_count = user_result_legacy["friends_count"]
                    listed_count = user_result_legacy["listed_count"]
                    user_created_at = user_result_legacy["created_at"]

                    id_str = tweet_result_legacy["id_str"]
                    full_text = tweet_result_legacy["full_text"]
                    created_at = tweet_result_legacy["created_at"]
                    quote_count = tweet_result_legacy["quote_count"]
                    reply_count = tweet_result_legacy["reply_count"]
                    retweet_count = tweet_result_legacy["retweet_count"]
                    favorite_count = tweet_result_legacy["favorite_count"]
                    bookmark_count = tweet_result_legacy["bookmark_count"]
                    views_count = entry["content"]["itemContent"]["tweet_results"][
                        "result"
                    ]["views"]["count"]

                    timestamp = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ")

                    quotes.append(
                        {
                            "user": {
                                "user_id_str": user_id_str,
                                "name": name,
                                "screen_name": screen_name,
                                "description": description,
                                "followers_count": followers_count,
                                "friends_count": friends_count,
                                "listed_count": listed_count,
                                "user_created_at": user_created_at,
                            },
                            "tweet": {
                                "id_str": id_str,
                                "full_text": full_text,
                                "created_at": created_at,
                                "quote_count": quote_count,
                                "reply_count": reply_count,
                                "retweet_count": retweet_count,
                                "favorite_count": favorite_count,
                                "bookmark_count": bookmark_count,
                                "views_count": views_count,
                            },
                            "timestamp": timestamp,
                        }
                    )
                except Exception as ex:
                    self.logger.error(ex)
                    continue
            bottom = res["data"]["search_by_raw_query"]["search_timeline"]["timeline"][
                "instructions"
            ][0]["entries"][-1]
            if "entryId" in bottom:
                if bottom["entryId"] == "cursor-bottom-0":
                    if cursor == bottom["content"]["value"]:
                        cursor = None
                        break
                    cursor = bottom["content"]["value"]
                else:
                    cursor = None
            else:
                cursor = None
            if cursor is None:
                break
        return quotes

    def __fetch_pickup_yahoo_news(self, url):
        try:
            headers = {
                "authority": "news.yahoo.co.jp",
                "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
                "accept-language": "ja,en-US;q=0.9,en;q=0.8",
                "referer": url,
                "user-agent": USER_AGENT,
            }
            content = requests.request("GET", url, headers=headers, data={})
            self.logger.info(f"Request: {url}")
            scripts = re.compile(r"<script.*?>(.*?)</script>", re.DOTALL).findall(
                content.text
            )
            res = None
            for script in scripts:
                if "__PRELOADED_STATE__" not in script:
                    continue
                script = script.replace("window.__PRELOADED_STATE__ = ", "")
                res = json.loads(script)
                break
            if res is None:
                return {}

            data = res["pageData"]
            topic = res["topicsDetail"]

            path = data["path"]
            topittl = data["pageParam"]["topittl"]
            topitime = data["pageParam"]["topitime"]
            title = topic["article"]["title"]
            description = data["description"]
            media_name = topic["article"]["mediaName"]
            pub_data = data["pubDate"]
            update_date = data["updateDate"]
            total_comment_count = res["commentShort"]["totalCommentCount"]
            return {
                "path": path,
                "topittl": topittl,
                "topitime": topitime,
                "title": title,
                "description": description,
                "media_name": media_name,
                "pub_data": pub_data,
                "update_date": update_date,
                "total_comment_count": total_comment_count,
            }
        except Exception as ex:
            self.logger.error(ex)
            return {}
