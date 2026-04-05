"""
kol_new_posts_scraper.py
========================
讀取 KOL 帳號清單，找出過去 N 天內發布的新 Reel，
將尚未記錄在 reels_static_info.csv 的新貼文追加進去。

流程：
  Step 1: username → user_id       (web_profile_info，帶 Cookie)
  Step 2: user_id  → 最新貼文列表  (api/v1/feed/user/{user_id}，帶 Cookie)
  Step 3: shortcode → 詳細資訊     (api/graphql Endpoint A)

Cookie 設定：config.json → { "cookie": "..." }
或環境變數：IG_COOKIE=...

Usage:
  python kol_new_posts_scraper.py
  python kol_new_posts_scraper.py --kol kol_list.csv --static reels_static_info.csv --days 2
  python kol_new_posts_scraper.py --config my_config.json
"""

import csv
import json
import os
import random
import sys
import time
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional, Set
from urllib.parse import urlencode

import requests


# ---------------------------------------------------------------------------
# 設定
# ---------------------------------------------------------------------------

DATA_DIR = "data"
KOL_CSV       = os.path.join(DATA_DIR, "kol_info.csv")
STATIC_CSV    = os.path.join(DATA_DIR, "reels_static_info.csv")
CONFIG_FILE   = "config.json"
DAYS_BACK     = 2

DELAY_MIN       = 1.5
DELAY_MAX       = 4.0
BATCH_SIZE      = 25
BATCH_PAUSE_MIN = 30
BATCH_PAUSE_MAX = 60
RETRY_DELAYS    = [5, 15, 45]

INSTAGRAM_HOME   = "https://www.instagram.com/"
PROFILE_INFO_URL = "https://www.instagram.com/api/v1/users/web_profile_info/"
FEED_URL         = "https://www.instagram.com/api/v1/feed/user/{user_id}/"
GRAPHQL_URL_A    = "https://www.instagram.com/api/graphql"
DOC_REEL_DETAIL  = "10015901848480474"
LSD_TOKEN        = "AVqbxe3J_YA"

# media_type=2 + product_type=clips → Reel
REEL_MEDIA_TYPE   = 2
REEL_PRODUCT_TYPE = "clips"

STATIC_FIELDNAMES = ["kol_account", "reels_shortcode", "post_time", "duration", "caption"]


# ---------------------------------------------------------------------------
# Exceptions
# ---------------------------------------------------------------------------

class ScraperError(Exception):
    pass

class RateLimitedError(ScraperError):
    pass

class AuthError(ScraperError):
    pass

class AccountNotFoundError(ScraperError):
    pass


# ---------------------------------------------------------------------------
# 設定檔讀取
# ---------------------------------------------------------------------------

def load_cookie(config_file: str = CONFIG_FILE) -> str:
    env_cookie = os.environ.get("IG_COOKIE", "").strip()
    if env_cookie:
        return env_cookie
    if os.path.isfile(config_file):
        cookie = json.load(open(config_file, encoding="utf-8")).get("cookie", "").strip()
        if cookie:
            return cookie
    raise AuthError(
        "找不到 Cookie！請在 config.json 設定 {\"cookie\": \"...\"} "
        "或設定環境變數 IG_COOKIE=..."
    )


# ---------------------------------------------------------------------------
# Session
# ---------------------------------------------------------------------------

class SessionManager:
    def __init__(self, cookie: str, timeout: int = 15) -> None:
        self.timeout = timeout
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
            "X-IG-App-ID":     "936619743392459",
            "Cookie":          cookie,
        })
        csrf = self._parse_csrf(cookie)
        if csrf:
            self.session.headers["X-CSRFToken"] = csrf

    @staticmethod
    def _parse_csrf(cookie: str) -> Optional[str]:
        for part in cookie.split(";"):
            part = part.strip()
            if part.startswith("csrftoken="):
                return part.split("=", 1)[1].strip()
        return None

    def _handle_status(self, resp: requests.Response) -> None:
        if resp.status_code == 429:
            raise RateLimitedError(f"Rate limited (429)")
        if resp.status_code in (401, 403):
            raise AuthError(f"HTTP {resp.status_code}：Cookie 失效或帳號被封鎖")
        if resp.status_code == 404:
            raise AccountNotFoundError("帳號不存在或為私密帳號")
        resp.raise_for_status()

    def get_with_retry(self, url: str, params: Optional[Dict] = None) -> Dict[str, Any]:
        for attempt, wait in enumerate([0] + RETRY_DELAYS):
            if wait:
                jitter = random.uniform(0, wait * 0.3)
                print(f"      → 等待 {wait + jitter:.0f} 秒後重試（第 {attempt} 次）…", flush=True)
                time.sleep(wait + jitter)
            try:
                resp = self.session.get(url, params=params, timeout=self.timeout)
                self._handle_status(resp)
                return resp.json()
            except (RateLimitedError, AuthError, AccountNotFoundError):
                raise
            except Exception:
                if attempt == len(RETRY_DELAYS):
                    raise
        raise ScraperError("未知錯誤")

    def post_with_retry(self, url: str, payload: str,
                        extra_headers: Optional[Dict] = None) -> Dict[str, Any]:
        headers = dict(self.session.headers)
        headers["Content-Type"] = "application/x-www-form-urlencoded"
        if extra_headers:
            headers.update(extra_headers)
        for attempt, wait in enumerate([0] + RETRY_DELAYS):
            if wait:
                jitter = random.uniform(0, wait * 0.3)
                print(f"      → 等待 {wait + jitter:.0f} 秒後重試（第 {attempt} 次）…", flush=True)
                time.sleep(wait + jitter)
            try:
                resp = self.session.post(url, data=payload, headers=headers, timeout=self.timeout)
                self._handle_status(resp)
                return resp.json()
            except (RateLimitedError, AuthError, AccountNotFoundError):
                raise
            except Exception:
                if attempt == len(RETRY_DELAYS):
                    raise
        raise ScraperError("未知錯誤")


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

def sleep_random(min_s: float = DELAY_MIN, max_s: float = DELAY_MAX) -> None:
    time.sleep(random.uniform(min_s, max_s))

def sleep_batch_pause() -> None:
    pause = random.uniform(BATCH_PAUSE_MIN, BATCH_PAUSE_MAX)
    print(f"\n  ── 批次暫停 {pause:.0f} 秒 ──\n", flush=True)
    time.sleep(pause)


# ---------------------------------------------------------------------------
# Step 1: username → user_id
# ---------------------------------------------------------------------------

def fetch_user_id(session: SessionManager, username: str) -> str:
    data    = session.get_with_retry(PROFILE_INFO_URL, params={"username": username})
    user_id = (data.get("data") or {}).get("user", {}).get("id")
    if not user_id:
        raise AccountNotFoundError(f"無法取得 @{username} 的 user_id")
    return str(user_id)


# ---------------------------------------------------------------------------
# Step 2: user_id → 最新 Reel 列表
# ---------------------------------------------------------------------------

def fetch_recent_reels(session: SessionManager, user_id: str, cutoff_ts: float) -> List[Dict]:
    """
    用 /api/v1/feed/user/{user_id}/ 取貼文。
    只回傳 media_type=2 (video) + product_type=clips (Reel)，
    且 taken_at >= cutoff_ts 的項目。
    支援翻頁（最多 5 頁），遇到比 cutoff 舊的貼文立即停止。
    """
    results:  List[Dict] = []
    max_id:   Optional[str] = None

    for _page in range(5):
        params: Dict[str, Any] = {"count": 12}
        if max_id:
            params["max_id"] = max_id

        data  = session.get_with_retry(FEED_URL.format(user_id=user_id), params=params)
        items = data.get("items") or []
        stop  = False

        for item in items:
            taken_at = item.get("taken_at")

            # 比截止時間舊 → 不用繼續翻頁
            if taken_at and taken_at < cutoff_ts:
                stop = True
                break

            # 只要 Reel（media_type=2 + product_type=clips）
            if item.get("media_type") != REEL_MEDIA_TYPE:
                continue
            if item.get("product_type") != REEL_PRODUCT_TYPE:
                continue

            sc = item.get("code")   # feed API 用 "code" 不是 "shortcode"
            if sc and taken_at:
                results.append({"shortcode": sc, "taken_at": taken_at})

        if stop or not data.get("more_available"):
            break

        max_id = data.get("next_max_id")
        if not max_id:
            break

        time.sleep(random.uniform(1.0, 2.0))

    return results


# ---------------------------------------------------------------------------
# Step 3: shortcode → 詳細資訊（Endpoint A）
# ---------------------------------------------------------------------------

def fetch_reel_detail(session: SessionManager, shortcode: str) -> Dict[str, Any]:
    payload = urlencode({
        "variables": json.dumps({"shortcode": shortcode}, separators=(",", ":")),
        "doc_id":    DOC_REEL_DETAIL,
        "lsd":       LSD_TOKEN,
    })
    extra = {
        "X-FB-LSD":       LSD_TOKEN,
        "X-ASBD-ID":      "129477",
        "Sec-Fetch-Site": "same-origin",
    }
    resp = session.post_with_retry(GRAPHQL_URL_A, payload, extra_headers=extra)
    item = (resp.get("data") or {}).get("xdt_shortcode_media")
    if not isinstance(item, dict):
        raise ScraperError(f"shortcode={shortcode} 回傳結構異常")
    return item


def extract_static_row(username: str, shortcode: str, item: Dict[str, Any]) -> Dict[str, Any]:
    caption = (
        ((item.get("edge_media_to_caption") or {})
         .get("edges") or [{}])[0]
        .get("node", {})
        .get("text", "")
    )
    taken_at  = item.get("taken_at_timestamp") or item.get("taken_at")
    post_time = (
        datetime.fromtimestamp(taken_at, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        if taken_at else ""
    )
    return {
        "kol_account":     username,
        "reels_shortcode": shortcode,
        "post_time":       post_time,
        "duration":        item.get("video_duration"),
        "caption":         caption,
    }


# ---------------------------------------------------------------------------
# CSV helpers
# ---------------------------------------------------------------------------

def load_existing_shortcodes(static_csv: str) -> Set[str]:
    if not os.path.isfile(static_csv):
        return set()
    shortcodes: Set[str] = set()
    with open(static_csv, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            sc = row.get("reels_shortcode", "").strip()
            if sc:
                shortcodes.add(sc)
    return shortcodes


def append_static_rows(static_csv: str, rows: List[Dict[str, Any]]) -> None:
    if not rows:
        return
    file_exists = os.path.isfile(static_csv)
    with open(static_csv, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=STATIC_FIELDNAMES)
        if not file_exists:
            writer.writeheader()
        writer.writerows(rows)


def read_kol_list(kol_csv: str) -> List[str]:
    accounts: List[str] = []
    with open(kol_csv, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            acc = row.get("kol_account", "").strip().lstrip("@")
            if acc:
                accounts.append(acc)
    return accounts


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def scan_kol_new_posts(
    kol_csv:     str = KOL_CSV,
    static_csv:  str = STATIC_CSV,
    days_back:   int = DAYS_BACK,
    config_file: str = CONFIG_FILE,
) -> None:
    try:
        cookie = load_cookie(config_file)
    except AuthError as e:
        print(f"[ERROR] {e}"); sys.exit(1)

    accounts = read_kol_list(kol_csv)
    if not accounts:
        print(f"[WARN] {kol_csv} 中找不到任何帳號，結束。")
        return

    cutoff_ts  = (datetime.now(tz=timezone.utc) - timedelta(days=days_back)).timestamp()
    cutoff_str = datetime.fromtimestamp(cutoff_ts, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

    print(f"[INFO] 共 {len(accounts)} 個帳號")
    print(f"[INFO] 掃描範圍：過去 {days_back} 天（{cutoff_str} UTC 之後）")
    print(f"[INFO] 請求間隔：{DELAY_MIN}–{DELAY_MAX} 秒，每 {BATCH_SIZE} 帳號暫停 {BATCH_PAUSE_MIN}–{BATCH_PAUSE_MAX} 秒\n")

    existing  = load_existing_shortcodes(static_csv)
    session   = SessionManager(cookie)
    total_new = 0

    for i, username in enumerate(accounts, start=1):
        if i > 1 and (i - 1) % BATCH_SIZE == 0:
            sleep_batch_pause()

        print(f"[{i}/{len(accounts)}] @{username}", end=" … ", flush=True)

        # ── Step 1: user_id ──────────────────────────────────────────────
        try:
            user_id = fetch_user_id(session, username)
        except AuthError as e:
            print(f"\n[FATAL] Cookie 失效：{e}")
            sys.exit(1)
        except (AccountNotFoundError, ScraperError) as e:
            print(f"✗  {e}")
            sleep_random(); continue
        except Exception as e:
            print(f"✗  {type(e).__name__}：{e}")
            sleep_random(); continue

        # ── Step 2: 最新貼文列表 ─────────────────────────────────────────
        try:
            recent = fetch_recent_reels(session, user_id, cutoff_ts)
        except AuthError as e:
            print(f"\n[FATAL] Cookie 失效：{e}")
            sys.exit(1)
        except RateLimitedError:
            print("✗  Rate limited → 等待 60 秒後重試")
            time.sleep(60)
            try:
                recent = fetch_recent_reels(session, user_id, cutoff_ts)
            except Exception as e:
                print(f"✗  重試失敗：{e}"); sleep_random(); continue
        except Exception as e:
            print(f"✗  {type(e).__name__}：{e}")
            sleep_random(); continue

        new_posts = [p for p in recent if p.get("shortcode") not in existing]

        if not new_posts:
            print("（無新貼文）")
            sleep_random(); continue

        print(f"發現 {len(new_posts)} 則新 Reel，抓取詳細資訊…")

        # ── Step 3: 詳細資訊 ─────────────────────────────────────────────
        new_rows: List[Dict] = []
        for post in new_posts:
            sc = post["shortcode"]
            try:
                item = fetch_reel_detail(session, sc)
                row  = extract_static_row(username, sc, item)
                new_rows.append(row)
                existing.add(sc)
                print(f"    ✓  {sc}  post_time={row['post_time']}  duration={row['duration']}")
            except AuthError as e:
                print(f"\n[FATAL] Cookie 失效：{e}"); sys.exit(1)
            except RateLimitedError:
                print(f"    ✗  {sc}  Rate limited → 等待 60 秒")
                time.sleep(60)
            except Exception as e:
                print(f"    ✗  {sc}  {type(e).__name__}：{e}")
            sleep_random()

        if new_rows:
            append_static_rows(static_csv, new_rows)
            total_new += len(new_rows)

        sleep_random()

    print(f"\n[DONE] 掃描完成，共寫入 {total_new} 筆新 Reel 到 {static_csv}")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main() -> None:
    args        = sys.argv[1:]
    kol_csv     = KOL_CSV
    static_csv  = STATIC_CSV
    days_back   = DAYS_BACK
    config_file = CONFIG_FILE

    for idx, arg in enumerate(args):
        if arg == "--kol"    and idx + 1 < len(args): kol_csv     = args[idx + 1]
        if arg == "--static" and idx + 1 < len(args): static_csv  = args[idx + 1]
        if arg == "--config" and idx + 1 < len(args): config_file = args[idx + 1]
        if arg == "--days"   and idx + 1 < len(args):
            try:
                days_back = int(args[idx + 1])
            except ValueError:
                print("[ERROR] --days 必須是整數"); sys.exit(1)

    scan_kol_new_posts(
        kol_csv=kol_csv, static_csv=static_csv,
        days_back=days_back, config_file=config_file,
    )


if __name__ == "__main__":
    main()