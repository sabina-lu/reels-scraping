import csv
import json
import os
import re
import sys
import time
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional
from urllib.parse import quote, urlencode

import requests


INSTAGRAM_HOME = "https://www.instagram.com/"

# Endpoint A: newer private API
GRAPHQL_URL_A = "https://www.instagram.com/api/graphql"
DOC_ID_A = "10015901848480474"
LSD_TOKEN = "AVqbxe3J_YA"

# Endpoint B: fallback
GRAPHQL_URL_B = "https://www.instagram.com/graphql/query"
DOC_ID_B = "24368985919464652"

DATA_DIR = "data"
STATIC_CSV = os.path.join(DATA_DIR, "reels_static_info.csv")
DYNAMIC_CSV = os.path.join(DATA_DIR, "reels_dynamic_info.csv")

STATIC_FIELDNAMES = [
    "kol_account",
    "reels_shortcode",
    "post_time",
    "duration",
    "caption",
]

DYNAMIC_FIELDNAMES = [
    "reels_shortcode",
    "views",
    "plays",
    "likes",
    "comments",
    "timestamp",
]


class InstagramReelError(Exception):
    pass


class InvalidInstagramUrlError(InstagramReelError):
    pass


class PrivateOrNotFoundError(InstagramReelError):
    pass


class RateLimitedError(InstagramReelError):
    pass


@dataclass
class ReelSummary:
    shortcode: str
    caption: Optional[str]
    taken_at: Optional[int]
    video_duration: Optional[float]
    like_count: Optional[int]
    comment_count: Optional[int]
    view_count: Optional[int]
    play_count: Optional[int]


def ensure_parent_dir(filepath: str) -> None:
    parent = os.path.dirname(filepath)
    if parent:
        os.makedirs(parent, exist_ok=True)


def ensure_csv_exists(filepath: str, fieldnames: List[str]) -> None:
    ensure_parent_dir(filepath)
    if os.path.exists(filepath):
        return

    with open(filepath, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()


def extract_shortcode_from_url(url: str) -> str:
    clean_url = url.split("?")[0].strip()
    pattern = r"instagram\.com/(?:[^/]+/)?(?:reel|p)/([^/?#]+)"
    match = re.search(pattern, clean_url)
    if not match:
        raise InvalidInstagramUrlError("不是有效的 Instagram Reel/Post URL")
    return match.group(1)


def ts_to_str(ts: Optional[int]) -> str:
    if ts in (None, ""):
        return ""
    try:
        return datetime.fromtimestamp(int(ts)).strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return ""


class InstagramReelScraper:
    def __init__(
        self,
        timeout: int = 15,
        max_retries: int = 3,
        retry_delay: float = 2.0,
    ) -> None:
        self.timeout = timeout
        self.max_retries = max_retries
        self.retry_delay = retry_delay

        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            ),
            "Accept": "*/*",
            "Accept-Language": "en-US,en;q=0.9",
            "Origin": "https://www.instagram.com",
            "Referer": "https://www.instagram.com/",
            "Content-Type": "application/x-www-form-urlencoded",
            "X-IG-App-ID": "936619743392459",
        })
        self._session_bootstrapped = False

    def _bootstrap_session(self) -> None:
        if self._session_bootstrapped:
            return
        resp = self.session.get(INSTAGRAM_HOME, timeout=self.timeout)
        resp.raise_for_status()
        csrf = self.session.cookies.get("csrftoken")
        if csrf:
            self.session.headers["X-CSRFToken"] = csrf
        self._session_bootstrapped = True

    def _post(
        self,
        url: str,
        payload: str,
        extra_headers: Optional[Dict[str, str]] = None
    ) -> Dict[str, Any]:
        headers = dict(self.session.headers)
        if extra_headers:
            headers.update(extra_headers)

        last_error: Optional[Exception] = None

        for attempt in range(1, self.max_retries + 1):
            try:
                resp = self.session.post(
                    url,
                    data=payload,
                    headers=headers,
                    timeout=self.timeout,
                )

                if resp.status_code == 429:
                    raise RateLimitedError("被 Instagram rate limit 了，請稍後再試")
                if resp.status_code == 404:
                    raise PrivateOrNotFoundError("Reel 不存在、是私密內容，或目前無法存取")

                resp.raise_for_status()

                data = resp.json()
                if "data" not in data:
                    raise InstagramReelError(
                        f"回應格式不符合預期：{json.dumps(data, ensure_ascii=False)[:400]}"
                    )
                return data

            except (RateLimitedError, PrivateOrNotFoundError):
                raise
            except (requests.RequestException, ValueError, InstagramReelError) as exc:
                last_error = exc
                if attempt == self.max_retries:
                    break
                time.sleep(self.retry_delay)

        raise last_error or InstagramReelError("未知錯誤")

    def _request_endpoint_a(self, shortcode: str) -> Optional[Dict[str, Any]]:
        variables = json.dumps({"shortcode": shortcode}, separators=(",", ":"))
        payload = urlencode({
            "variables": variables,
            "doc_id": DOC_ID_A,
            "lsd": LSD_TOKEN,
        })
        extra_headers = {
            "X-FB-LSD": LSD_TOKEN,
            "X-ASBD-ID": "129477",
            "Sec-Fetch-Site": "same-origin",
        }

        try:
            resp_json = self._post(GRAPHQL_URL_A, payload, extra_headers=extra_headers)
        except (PrivateOrNotFoundError, RateLimitedError):
            raise
        except InstagramReelError:
            return None

        item = (resp_json.get("data") or {}).get("xdt_shortcode_media")
        return item if isinstance(item, dict) else None

    def _request_endpoint_b(self, shortcode: str) -> Dict[str, Any]:
        variables = json.dumps({"shortcode": shortcode}, separators=(",", ":"))
        payload = f"variables={quote(variables)}&doc_id={DOC_ID_B}"
        resp_json = self._post(GRAPHQL_URL_B, payload)

        data = resp_json.get("data") or {}
        node = data.get("xdt_api__v1__media__shortcode__web_info")
        if isinstance(node, dict):
            items = node.get("items") or []
            if items and isinstance(items[0], dict):
                return items[0]

        node2 = data.get("shortcode_media")
        if isinstance(node2, dict):
            return node2

        preview = json.dumps(data, ensure_ascii=False)[:400]
        raise PrivateOrNotFoundError(
            f"找不到 media item（私密/已刪除/API 結構變更）。data 預覽：{preview}"
        )

    @staticmethod
    def _summary_from_a(shortcode: str, item: Dict[str, Any]) -> ReelSummary:
        caption = (
            ((item.get("edge_media_to_caption") or {}).get("edges") or [{}])[0]
            .get("node", {})
            .get("text")
        )

        like_count = (
            item.get("like_count")
            or (item.get("edge_media_preview_like") or {}).get("count")
            or (item.get("edge_liked_by") or {}).get("count")
        )

        comment_count = (
            item.get("comment_count")
            or (item.get("edge_media_to_comment") or {}).get("count")
            or (item.get("edge_media_preview_comment") or {}).get("count")
        )

        return ReelSummary(
            shortcode=item.get("shortcode") or shortcode,
            caption=caption,
            taken_at=item.get("taken_at"),
            video_duration=item.get("video_duration"),
            like_count=like_count,
            comment_count=comment_count,
            view_count=item.get("video_view_count"),
            play_count=item.get("video_play_count"),
        )

    def _summary_from_b(self, shortcode: str, item: Dict[str, Any]) -> ReelSummary:
        caption_obj = item.get("caption") if isinstance(item.get("caption"), dict) else {}

        view_count = item.get("view_count") or item.get("ig_play_count") or item.get("fb_play_count")
        play_count = item.get("play_count") or item.get("ig_play_count") or item.get("view_count")

        return ReelSummary(
            shortcode=item.get("code") or shortcode,
            caption=caption_obj.get("text"),
            taken_at=item.get("taken_at"),
            video_duration=item.get("video_duration"),
            like_count=item.get("like_count"),
            comment_count=item.get("comment_count"),
            view_count=view_count,
            play_count=play_count,
        )

    def get_reel_data(self, shortcode: str) -> ReelSummary:
        self._bootstrap_session()

        item_a = self._request_endpoint_a(shortcode)
        if item_a is not None:
            return self._summary_from_a(shortcode, item_a)

        item_b = self._request_endpoint_b(shortcode)
        return self._summary_from_b(shortcode, item_b)

    def get_reel_data_from_url(self, url: str) -> ReelSummary:
        return self.get_reel_data(extract_shortcode_from_url(url))


def read_static_rows(static_csv: str) -> List[Dict[str, str]]:
    if not os.path.exists(static_csv):
        return []

    with open(static_csv, newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        rows = []
        for row in reader:
            normalized = {field: (row.get(field) or "").strip() for field in STATIC_FIELDNAMES}
            if normalized["reels_shortcode"]:
                rows.append(normalized)
        return rows


def write_static_rows(static_csv: str, rows: List[Dict[str, str]]) -> None:
    ensure_csv_exists(static_csv, STATIC_FIELDNAMES)
    with open(static_csv, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=STATIC_FIELDNAMES)
        writer.writeheader()
        writer.writerows(rows)


def append_dynamic_row(dynamic_csv: str, row: Dict[str, Any]) -> None:
    ensure_csv_exists(dynamic_csv, DYNAMIC_FIELDNAMES)
    with open(dynamic_csv, "a", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=DYNAMIC_FIELDNAMES)
        writer.writerow(row)


def should_fill_static_field(value: str) -> bool:
    return value.strip() == ""


def fill_static_fields(row: Dict[str, str], result: ReelSummary) -> bool:
    updated_fields = {}

    post_time = ts_to_str(result.taken_at)
    duration = "" if result.video_duration is None else str(result.video_duration)
    caption = result.caption or ""

    if should_fill_static_field(row.get("post_time", "")) and post_time:
        row["post_time"] = post_time
        updated_fields["post_time"] = post_time

    if should_fill_static_field(row.get("duration", "")) and duration:
        row["duration"] = duration
        updated_fields["duration"] = duration

    if should_fill_static_field(row.get("caption", "")) and caption:
        row["caption"] = caption
        updated_fields["caption"] = caption[:20]

    return updated_fields


def batch_scrape(
    static_csv: str = STATIC_CSV,
    dynamic_csv: str = DYNAMIC_CSV,
    request_delay: float = 1.5,
) -> None:
    static_rows = read_static_rows(static_csv)
    if not static_rows:
        print(f"[WARN] 在 {static_csv} 中找不到任何 shortcode，結束。")
        return

    print(f"[INFO] 共找到 {len(static_rows)} 個 shortcode，開始爬取…")

    scraper = InstagramReelScraper()
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    success_count = 0
    fail_count = 0
    static_updated_count = 0

    for i, row in enumerate(static_rows, start=1):
        shortcode = row["reels_shortcode"]
        print(f"[{i}/{len(static_rows)}] 正在爬取 {shortcode} …", end=" ", flush=True)

        try:
            result = scraper.get_reel_data(shortcode)

            updated = fill_static_fields(row, result)

            if updated:
                static_updated_count += 1
                print(f"✓ static updated [{shortcode}] → {updated}")

            append_dynamic_row(dynamic_csv, {
                "reels_shortcode": result.shortcode,
                "views": result.view_count,
                "plays": result.play_count,
                "likes": result.like_count,
                "comments": result.comment_count,
                "timestamp": timestamp,
            })

            print(
                f"✓ views={result.view_count}, plays={result.play_count}, "
                f"likes={result.like_count}, comments={result.comment_count}"
            )
            success_count += 1

        except RateLimitedError as exc:
            print(f"✗ Rate limited：{exc} → 等待 30 秒後繼續…")
            fail_count += 1
            time.sleep(30)
            continue

        except PrivateOrNotFoundError as exc:
            print(f"✗ 私密/不存在：{exc}")
            fail_count += 1

        except InstagramReelError as exc:
            print(f"✗ 跳過：{exc}")
            fail_count += 1

        except Exception as exc:
            print(f"✗ {type(exc).__name__}：{exc}")
            fail_count += 1

        if i < len(static_rows):
            time.sleep(request_delay)

    write_static_rows(static_csv, static_rows)

    print(
        f"\n[DONE] 完成！成功 {success_count} 筆，失敗 {fail_count} 筆，"
        f"補齊 static {static_updated_count} 筆。dynamic 已寫入 {dynamic_csv}"
    )


def main() -> None:
    """
    Usage:
      python instagram_reel_scraper.py
      python instagram_reel_scraper.py --static a.csv --dynamic b.csv
      python instagram_reel_scraper.py <instagram_reel_url>
    """
    args = sys.argv[1:]

    if args and not args[0].startswith("--"):
        url = args[0]
        scraper = InstagramReelScraper()

        try:
            result = scraper.get_reel_data_from_url(url)
            output = {
                "shortcode": result.shortcode,
                "post_time": ts_to_str(result.taken_at),
                "duration": result.video_duration,
                "caption": result.caption,
                "like_count": result.like_count,
                "comment_count": result.comment_count,
                "view_count": result.view_count,
                "play_count": result.play_count,
            }
            print(json.dumps(output, ensure_ascii=False, indent=2))

        except InstagramReelError as exc:
            print(f"抓取失敗：{exc}")
            sys.exit(2)

        except requests.RequestException as exc:
            print(f"網路錯誤：{exc}")
            sys.exit(3)

        return

    static_csv = STATIC_CSV
    dynamic_csv = DYNAMIC_CSV

    i = 0
    while i < len(args):
        if args[i] == "--static" and i + 1 < len(args):
            static_csv = args[i + 1]
            i += 2
            continue
        if args[i] == "--dynamic" and i + 1 < len(args):
            dynamic_csv = args[i + 1]
            i += 2
            continue
        i += 1

    batch_scrape(static_csv=static_csv, dynamic_csv=dynamic_csv)


if __name__ == "__main__":
    main()