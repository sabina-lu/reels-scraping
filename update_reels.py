import csv
import json
import os
import re
import sys
import time
from dataclasses import dataclass, asdict
from datetime import datetime
from typing import Any, Dict, List, Optional
from urllib.parse import quote, urlencode

import requests


INSTAGRAM_HOME = "https://www.instagram.com/"

# ── Endpoint A: newer private API (returns view/play counts via xdt_shortcode_media)
GRAPHQL_URL_A = "https://www.instagram.com/api/graphql"
DOC_ID_A      = "10015901848480474"
LSD_TOKEN     = "AVqbxe3J_YA"   # static LSD token used by the browser

# ── Endpoint B: fallback (original endpoint, fewer fields)
GRAPHQL_URL_B = "https://www.instagram.com/graphql/query"
DOC_ID_B      = "24368985919464652"

DATA_DIR = "data"
STATIC_CSV = os.path.join(DATA_DIR, "reels_static_info.csv")
DYNAMIC_CSV = os.path.join(DATA_DIR, "reels_dynamic_info.csv")

# ---------------------------------------------------------------------------
# Exceptions
# ---------------------------------------------------------------------------

class InstagramReelError(Exception):
    """Base exception for scraper errors."""

class InvalidInstagramUrlError(InstagramReelError):
    """Raised when the provided URL is not a valid Instagram reel/post URL."""

class PrivateOrNotFoundError(InstagramReelError):
    """Raised when the reel is private or not found."""

class RateLimitedError(InstagramReelError):
    """Raised when Instagram rate limits the request."""


# ---------------------------------------------------------------------------
# Data model
# ---------------------------------------------------------------------------

@dataclass
class ReelSummary:
    shortcode: str
    # media_id: Optional[str]
    # caption: Optional[str]
    like_count: Optional[int]
    comment_count: Optional[int]
    view_count: Optional[int]
    play_count: Optional[int]
    # taken_at: Optional[int]
    # product_type: Optional[str]
    # video_duration: Optional[float]
    # username: Optional[str]
    # full_name: Optional[str]
    # is_verified: Optional[bool]
    # profile_pic_url: Optional[str]
    # thumbnail_url: Optional[str]
    # video_url: Optional[str]
    # all_video_versions: List[Dict[str, Any]]
    # audio_title: Optional[str]
    # audio_artist: Optional[str]
    # raw_item: Dict[str, Any]


# ---------------------------------------------------------------------------
# URL helper
# ---------------------------------------------------------------------------

def extract_shortcode_from_url(url: str) -> str:
    clean_url = url.split("?")[0].strip()
    pattern = r"instagram\.com/(?:[^/]+/)?(?:reel|p)/([^/?#]+)"
    match = re.search(pattern, clean_url)
    if not match:
        raise InvalidInstagramUrlError("不是有效的 Instagram Reel/Post URL")
    return match.group(1)


# ---------------------------------------------------------------------------
# Scraper
# ---------------------------------------------------------------------------

class InstagramReelScraper:
    def __init__(
        self,
        timeout: int = 15,
        max_retries: int = 3,
        retry_delay: float = 2.0,
    ) -> None:
        self.timeout     = timeout
        self.max_retries = max_retries
        self.retry_delay = retry_delay

        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            ),
            "Accept":          "*/*",
            "Accept-Language": "en-US,en;q=0.9",
            "Origin":          "https://www.instagram.com",
            "Referer":         "https://www.instagram.com/",
            "Content-Type":    "application/x-www-form-urlencoded",
            "X-IG-App-ID":     "936619743392459",
        })
        self._session_bootstrapped = False

    # ── session bootstrap ──────────────────────────────────────────────────

    def _bootstrap_session(self) -> None:
        if self._session_bootstrapped:
            return
        resp = self.session.get(INSTAGRAM_HOME, timeout=self.timeout)
        resp.raise_for_status()
        csrf = self.session.cookies.get("csrftoken")
        if csrf:
            self.session.headers["X-CSRFToken"] = csrf
        self._session_bootstrapped = True

    # ── low-level POST ─────────────────────────────────────────────────────

    def _post(self, url: str, payload: str, extra_headers: Optional[Dict] = None) -> Dict[str, Any]:
        """POST with retry logic; returns parsed JSON."""
        headers = dict(self.session.headers)
        if extra_headers:
            headers.update(extra_headers)

        last_error: Optional[Exception] = None
        for attempt in range(1, self.max_retries + 1):
            try:
                resp = self.session.post(url, data=payload, headers=headers, timeout=self.timeout)

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
                raise   # don't retry these
            except (requests.RequestException, ValueError, InstagramReelError) as e:
                last_error = e
                if attempt == self.max_retries:
                    break
                time.sleep(self.retry_delay)

        raise last_error or InstagramReelError("未知錯誤")

    # ── Endpoint A: api/graphql → xdt_shortcode_media ─────────────────────

    def _request_endpoint_a(self, shortcode: str) -> Optional[Dict[str, Any]]:
        """
        Newer endpoint. Returns item dict if successful, None if the
        response structure doesn't match (so we can fall back to B).
        """
        variables = json.dumps({"shortcode": shortcode}, separators=(",", ":"))
        payload = urlencode({
            "variables": variables,
            "doc_id":    DOC_ID_A,
            "lsd":       LSD_TOKEN,
        })
        extra = {
            "X-FB-LSD":  LSD_TOKEN,
            "X-ASBD-ID": "129477",
            "Sec-Fetch-Site": "same-origin",
        }
        try:
            resp_json = self._post(GRAPHQL_URL_A, payload, extra_headers=extra)
        except (PrivateOrNotFoundError, RateLimitedError):
            raise
        except InstagramReelError:
            return None  # silently fall back to B

        item = (resp_json.get("data") or {}).get("xdt_shortcode_media")
        if isinstance(item, dict):
            return item
        return None

    # ── Endpoint B: graphql/query → xdt_api__v1__media__shortcode__web_info

    def _request_endpoint_b(self, shortcode: str) -> Dict[str, Any]:
        variables = json.dumps({"shortcode": shortcode}, separators=(",", ":"))
        payload   = f"variables={quote(variables)}&doc_id={DOC_ID_B}"
        resp_json = self._post(GRAPHQL_URL_B, payload)

        data  = resp_json.get("data") or {}
        node  = data.get("xdt_api__v1__media__shortcode__web_info")
        if isinstance(node, dict):
            items = node.get("items") or []
            if items and isinstance(items[0], dict):
                return items[0]

        # Last-resort fallback key
        node2 = data.get("shortcode_media")
        if isinstance(node2, dict):
            return node2

        preview = json.dumps(data, ensure_ascii=False)[:400]
        raise PrivateOrNotFoundError(
            f"找不到 media item（私密/已刪除/API 結構變更）。data 預覽：{preview}"
        )

    # ── build ReelSummary from endpoint-A item ─────────────────────────────

    @staticmethod
    def _summary_from_a(shortcode: str, item: Dict[str, Any]) -> ReelSummary:
        """
        Endpoint A response uses different field names:
          - video_view_count  → view_count
          - video_play_count  → play_count
          - owner             → user block
          - edge_media_to_caption.edges[0].node.text → caption
          - thumbnail_src / display_url → thumbnail
        """
        owner   = item.get("owner") or {}
        caption = (
            ((item.get("edge_media_to_caption") or {})
             .get("edges") or [{}])[0]
            .get("node", {})
            .get("text")
        )

        # thumbnail: prefer thumbnail_src, fall back to display_url
        thumbnail = item.get("thumbnail_src") or item.get("display_url")

        # video url: direct field in endpoint-A
        video_url = item.get("video_url")

        # audio (clips_music_attribution_info in endpoint A)
        music = item.get("clips_music_attribution_info") or {}
        audio_title  = music.get("song_name")
        audio_artist = music.get("artist_name")

        # Endpoint A uses edge_* wrappers for like/comment counts
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
            shortcode    = item.get("shortcode") or shortcode,
            # media_id     = item.get("id"),
            # caption      = caption,
            like_count   = like_count,
            comment_count= comment_count,
            view_count   = item.get("video_view_count"),
            play_count   = item.get("video_play_count")
            # taken_at     = item.get("taken_at"),
            # product_type = item.get("product_type"),
            # video_duration = item.get("video_duration"),
            # username     = owner.get("username"),
            # full_name    = owner.get("full_name"),
            # is_verified  = owner.get("is_verified"),
            # profile_pic_url = owner.get("profile_pic_url"),
            # thumbnail_url= thumbnail,
            # video_url    = video_url,
            # all_video_versions = [],
            # audio_title  = audio_title,
            # audio_artist = audio_artist,
            # raw_item     = item,
        )

    # ── build ReelSummary from endpoint-B item ─────────────────────────────

    @staticmethod
    def _pick_best_video_url(video_versions: List[Dict[str, Any]]) -> Optional[str]:
        if not video_versions:
            return None
        best = max(video_versions, key=lambda v: (v.get("width") or 0) * (v.get("height") or 0))
        return best.get("url")

    @staticmethod
    def _pick_thumbnail(item: Dict[str, Any]) -> Optional[str]:
        candidates = []
        img2 = item.get("image_versions2") or {}
        candidates.extend(img2.get("candidates") or [])
        candidates.extend(item.get("display_resources") or [])
        if not candidates:
            return None
        best = max(candidates, key=lambda v: (v.get("width") or 0) * (v.get("height") or 0))
        return best.get("url")

    @staticmethod
    def _extract_audio_b(item: Dict[str, Any]) -> Dict[str, Optional[str]]:
        clips = item.get("clips_metadata") or {}
        music = clips.get("music_info") or {}
        asset = music.get("music_asset_info") or {}
        return {
            "audio_title":  asset.get("title") or music.get("music_canonical_id"),
            "audio_artist": asset.get("display_artist"),
        }

    def _summary_from_b(self, shortcode: str, item: Dict[str, Any]) -> ReelSummary:
        user        = item.get("user") if isinstance(item.get("user"), dict) else {}
        caption_obj = item.get("caption") if isinstance(item.get("caption"), dict) else {}
        video_versions = item.get("video_versions") or []
        audio = self._extract_audio_b(item)

        view_count = item.get("view_count") or item.get("ig_play_count") or item.get("fb_play_count")
        play_count = item.get("play_count") or item.get("ig_play_count") or item.get("view_count")

        return ReelSummary(
            shortcode    = item.get("code") or shortcode,
            # media_id     = item.get("id"),
            # caption      = caption_obj.get("text"),
            like_count   = item.get("like_count"),
            comment_count= item.get("comment_count"),
            view_count   = view_count,
            play_count   = play_count
            # taken_at     = item.get("taken_at"),
            # product_type = item.get("product_type"),
            # video_duration = item.get("video_duration"),
            # username     = user.get("username"),
            # full_name    = user.get("full_name"),
            # is_verified  = user.get("is_verified"),
            # profile_pic_url = user.get("profile_pic_url"),
            # thumbnail_url= self._pick_thumbnail(item),
            # video_url    = self._pick_best_video_url(video_versions),
            # all_video_versions = video_versions,
            # audio_title  = audio["audio_title"],
            # audio_artist = audio["audio_artist"],
            # raw_item     = item,
        )

    # ── public API ─────────────────────────────────────────────────────────

    def get_reel_data(self, shortcode: str) -> ReelSummary:
        """
        Try endpoint A first (has view/play counts).
        Fall back to endpoint B if A fails or returns no item.
        """
        self._bootstrap_session()

        # ── Try A ──
        item_a = self._request_endpoint_a(shortcode)
        if item_a is not None:
            return self._summary_from_a(shortcode, item_a)

        # ── Fallback B ──
        item_b = self._request_endpoint_b(shortcode)
        return self._summary_from_b(shortcode, item_b)

    def get_reel_data_from_url(self, url: str) -> ReelSummary:
        return self.get_reel_data(extract_shortcode_from_url(url))


# ---------------------------------------------------------------------------
# CSV helpers
# ---------------------------------------------------------------------------

DYNAMIC_FIELDNAMES = ["reels_shortcode", "views", "plays", "likes", "comments", "timestamp"]


def read_shortcodes_from_static(static_csv: str) -> List[str]:
    shortcodes = []
    with open(static_csv, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            sc = row.get("reels_shortcode", "").strip()
            if sc:
                shortcodes.append(sc)
    return shortcodes


def append_dynamic_row(dynamic_csv: str, row: Dict[str, Any]) -> None:
    file_exists = os.path.isfile(dynamic_csv)
    with open(dynamic_csv, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=DYNAMIC_FIELDNAMES)
        if not file_exists:
            writer.writeheader()
        writer.writerow(row)


# ---------------------------------------------------------------------------
# Batch scrape
# ---------------------------------------------------------------------------

def batch_scrape(
    static_csv: str = STATIC_CSV,
    dynamic_csv: str = DYNAMIC_CSV,
    request_delay: float = 1.5,
) -> None:
    shortcodes = read_shortcodes_from_static(static_csv)
    if not shortcodes:
        print(f"[WARN] 在 {static_csv} 中找不到任何 shortcode，結束。")
        return

    print(f"[INFO] 共找到 {len(shortcodes)} 個 shortcode，開始爬取…")

    scraper   = InstagramReelScraper()
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    success_count = fail_count = 0

    for i, shortcode in enumerate(shortcodes, start=1):
        print(f"[{i}/{len(shortcodes)}] 正在爬取 {shortcode} …", end=" ", flush=True)
        try:
            result = scraper.get_reel_data(shortcode)

            append_dynamic_row(dynamic_csv, {
                "reels_shortcode": shortcode,
                "views":    result.view_count,
                "plays":    result.play_count,
                "likes":    result.like_count,
                "comments": result.comment_count,
                "timestamp": timestamp,
            })

            print(
                f"✓  views={result.view_count}, plays={result.play_count}, "
                f"likes={result.like_count}, comments={result.comment_count}"
            )
            success_count += 1

        except RateLimitedError as e:
            print(f"✗  Rate limited：{e}  → 等待 30 秒後繼續…")
            fail_count += 1
            time.sleep(30)
            continue

        except PrivateOrNotFoundError as e:
            print(f"✗  私密/不存在：{e}")
            fail_count += 1

        except InstagramReelError as e:
            print(f"✗  跳過：{e}")
            fail_count += 1

        except Exception as e:
            print(f"✗  {type(e).__name__}：{e}")
            fail_count += 1

        if i < len(shortcodes):
            time.sleep(request_delay)

    print(
        f"\n[DONE] 完成！成功 {success_count} 筆，失敗 {fail_count} 筆。"
        f" 結果已寫入 {dynamic_csv}"
    )


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main() -> None:
    """
    Usage:
      python instagram_reel_scraper.py                          # batch mode
      python instagram_reel_scraper.py --static a.csv --dynamic b.csv
      python instagram_reel_scraper.py <instagram_reel_url> [--raw]
    """
    args = sys.argv[1:]

    if args and not args[0].startswith("--"):
        url      = args[0]
        save_raw = "--raw" in args
        scraper  = InstagramReelScraper()
        try:
            result = scraper.get_reel_data_from_url(url)
            output = {
                "shortcode":     result.shortcode,
                # "media_id":      result.media_id,
                # "username":      result.username,
                # "full_name":     result.full_name,
                # "is_verified":   result.is_verified,
                # "caption":       result.caption,
                "like_count":    result.like_count,
                "comment_count": result.comment_count,
                "view_count":    result.view_count,
                "play_count":    result.play_count
                # "taken_at":      result.taken_at,
                # "product_type":  result.product_type,
                # "video_duration":result.video_duration,
                # "thumbnail_url": result.thumbnail_url,
                # "video_url":     result.video_url,
                # "profile_pic_url": result.profile_pic_url,
                # "audio_title":   result.audio_title,
                # "audio_artist":  result.audio_artist,
                # "video_versions_count": len(result.all_video_versions),
            }
            print(json.dumps(output, ensure_ascii=False, indent=2))
            if save_raw:
                filename = f"{result.shortcode}_raw.json"
                with open(filename, "w", encoding="utf-8") as f:
                    json.dump(asdict(result), f, ensure_ascii=False, indent=2)
                print(f"\n已輸出 raw 資料到：{filename}")
        except InstagramReelError as e:
            print(f"抓取失敗：{e}"); sys.exit(2)
        except requests.RequestException as e:
            print(f"網路錯誤：{e}"); sys.exit(3)
        return

    static_csv  = STATIC_CSV
    dynamic_csv = DYNAMIC_CSV
    for i, arg in enumerate(args):
        if arg == "--static"  and i + 1 < len(args): static_csv  = args[i + 1]
        if arg == "--dynamic" and i + 1 < len(args): dynamic_csv = args[i + 1]

    batch_scrape(static_csv=static_csv, dynamic_csv=dynamic_csv)


if __name__ == "__main__":
    main()
