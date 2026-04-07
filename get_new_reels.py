import os
import re
import time
import json
import random
import shutil
import datetime as dt
from typing import Optional

import pandas as pd
from curl_cffi import requests
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC


# ========= Basic config =========
DATA_DIR = "data"
KOL_INFO_FILE = os.path.join(DATA_DIR, "kol_info.csv")
STATE_FILE = os.path.join(DATA_DIR, "profile_post_state.csv")
STATIC_FILE = os.path.join(DATA_DIR, "reels_static_info.csv")
DYNAMIC_FILE = os.path.join(DATA_DIR, "reels_dynamic_info.csv")

PROFILE_API = "https://www.instagram.com/api/v1/users/web_profile_info/?username={username}"
DETAIL_API = "https://www.instagram.com/graphql/query/"
DETAIL_DOC_ID = "8845758582119845"
IG_APP_ID = "936619743392459"

REELS_WINDOW_DAYS = 30
PROFILE_SLEEP_RANGE = (0.1, 0.3)  # Minimal delay between profiles
DETAIL_SLEEP_RANGE = (0.1, 0.2)  # Minimal delay between details

BASE_HEADERS = {
    "user-agent": (
        "Mozilla/5.0 (X11; Linux x86_64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/123.0.0.0 Safari/537.36"
    ),
    "x-ig-app-id": IG_APP_ID,
}

STATIC_COLUMNS = [
    "kol_account",
    "reels_shortcode",
    "post_time",
    "duration",
    "caption",
]

DYNAMIC_COLUMNS = [
    "reels_shortcode",
    "views",
    "plays",
    "likes",
    "comments",
    "timestamp",
]

STATE_COLUMNS = [
    "kol_account",
    "profile_post_count",
    "last_checked_at",
    "last_changed_at",
    "check_status",
]


# ========= Helpers =========
def now_local() -> dt.datetime:
    return dt.datetime.now()


def now_str() -> str:
    return now_local().strftime("%Y-%m-%d %H:%M:%S")


def sleep_random(sec_range: tuple) -> None:
    time.sleep(random.uniform(*sec_range))


def ensure_parent_dir(filepath: str) -> None:
    parent = os.path.dirname(filepath)
    if parent:
        os.makedirs(parent, exist_ok=True)


def read_or_init_csv(filepath: str, columns: list) -> pd.DataFrame:
    ensure_parent_dir(filepath)

    if os.path.exists(filepath):
        try:
            df = pd.read_csv(filepath)
            print(f"✅ Read {filepath}")
            for col in columns:
                if col not in df.columns:
                    df[col] = None
            return df[columns]
        except Exception as exc:
            print(f"⚠️ Failed to read {filepath}, using empty table: {exc}")

    return pd.DataFrame(columns=columns)


def save_csv(df: pd.DataFrame, filepath: str, columns: list) -> None:
    ensure_parent_dir(filepath)

    for col in columns:
        if col not in df.columns:
            df[col] = None

    df = df[columns]
    df.to_csv(filepath, index=False, encoding="utf-8-sig")
    print(f"✅ Saved {filepath} ({len(df)} rows)")


def dedupe_and_sort_static(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df.copy()

    out = df.copy()
    out["_post_time_dt"] = pd.to_datetime(out["post_time"], errors="coerce")
    out = out.sort_values(["_post_time_dt", "reels_shortcode"], ascending=[False, True])
    out = out.drop_duplicates(subset=["reels_shortcode"], keep="first")

    cutoff = now_local() - dt.timedelta(days=REELS_WINDOW_DAYS)
    out = out[out["_post_time_dt"].notna()]
    out = out[out["_post_time_dt"] >= cutoff]
    out = out.drop(columns=["_post_time_dt"], errors="ignore")
    return out


def dedupe_and_sort_dynamic(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df.copy()

    out = df.copy()
    out["_ts_dt"] = pd.to_datetime(out["timestamp"], errors="coerce")
    out = out.sort_values(["_ts_dt", "reels_shortcode"], ascending=[False, True])
    out = out.drop_duplicates(subset=["reels_shortcode", "timestamp"], keep="last")
    out = out.drop(columns=["_ts_dt"], errors="ignore")
    return out


def dedupe_and_sort_state(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df.copy()

    out = df.copy()
    out["_checked_dt"] = pd.to_datetime(out["last_checked_at"], errors="coerce")
    out = out.sort_values(["_checked_dt", "kol_account"], ascending=[False, True])
    out = out.drop_duplicates(subset=["kol_account"], keep="first")
    out = out.drop(columns=["_checked_dt"], errors="ignore")
    return out


def safe_json_get(url: str, headers: dict, referer: Optional[str] = None, timeout: int = 15, max_retries: int = 2):
    req_headers = headers.copy()
    if referer:
        req_headers["referer"] = referer

    for attempt in range(max_retries):
        try:
            resp = requests.get(
                url,
                headers=req_headers,
                impersonate="chrome120",
                timeout=timeout,
            )
            print(f"DEBUG GET {url} status={resp.status_code}")

            if resp.status_code == 200:
                return resp.json()
            elif resp.status_code in [429, 403, 401]:
                print(f"❌ GET rate limited/blocked status={resp.status_code}")
                if attempt < max_retries - 1:
                    sleep_time = 2 * (attempt + 1)
                    print(f"⏳ Retrying in {sleep_time} seconds...")
                    time.sleep(sleep_time)
                    continue
                return None
            else:
                print(f"❌ GET failed status={resp.status_code}")
                print(resp.text[:300])
                return None
        except Exception as exc:
            print(f"❌ GET error: {exc}")
            if attempt < max_retries - 1:
                print(f"⏳ Retrying...")
                time.sleep(1)
                continue
            return None
    
    return None


def normalize_count_text(text: str) -> Optional[int]:
    if text is None:
        return None

    raw = str(text).strip()
    if not raw:
        return None

    raw = raw.replace(",", "").replace(" ", "")
    match = re.search(r"(\d+(?:\.\d+)?)([KMBkmb]?)", raw)
    if not match:
        return None

    value = float(match.group(1))
    unit = match.group(2).lower()

    if unit == "k":
        value *= 1_000
    elif unit == "m":
        value *= 1_000_000
    elif unit == "b":
        value *= 1_000_000_000

    return int(round(value))


# ========= Selenium profile gate =========


def parse_cookie_string(cookie_str: str) -> list[dict]:
    """把 cookie header 字串轉成 selenium 可用的格式"""
    cookies = []
    for part in cookie_str.split(";"):
        part = part.strip()
        if "=" in part:
            name, _, value = part.partition("=")
            cookies.append({
                "name": name.strip(),
                "value": value.strip(),
                "domain": ".instagram.com",
                "path": "/",
            })
    return cookies


def load_cookies_from_string(driver: webdriver.Chrome, cookie_str: str) -> None:
    """先訪問 instagram 建立 session，再注入 cookie"""
    driver.get("https://www.instagram.com/")
    time.sleep(2)

    for cookie in parse_cookie_string(cookie_str):
        try:
            driver.add_cookie(cookie)
        except Exception as e:
            print(f"⚠️ Failed to add cookie {cookie['name']}: {e}")

    driver.refresh()
    time.sleep(3)
    print("✅ Cookies injected")


def build_driver() -> webdriver.Chrome:
    options = webdriver.ChromeOptions()
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--window-size=1440,2200")
    options.add_argument("--lang=en-US")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option("useAutomationExtension", False)
    options.add_experimental_option("prefs", {"intl.accept_languages": "en,en_US"})


    chrome_bin = os.environ.get("CHROME_BIN")
    if not chrome_bin:
        # Check macOS first
        macos_chrome = "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"
        if os.path.exists(macos_chrome):
            chrome_bin = macos_chrome
        else:
            # Then check Linux/Unix alternatives
            for candidate in ["google-chrome", "chromium", "chromium-browser", "chrome"]:
                resolved = shutil.which(candidate)
                if resolved:
                    chrome_bin = resolved
                    break

    if chrome_bin:
        options.binary_location = chrome_bin
        print(f"ℹ️ Using browser binary: {chrome_bin}")
    else:
        print(f"⚠️ Could not locate Chrome binary, using system default")

    return webdriver.Chrome(options=options)


def extract_post_count_from_page_source(driver: webdriver.Chrome) -> Optional[int]:
    html = driver.page_source or ""
    patterns = [
        r'edge_owner_to_timeline_media\":\{\"count\":(\d+)',
        r'edge_owner_to_timeline_media"\s*:\s*\{\s*"count"\s*:\s*(\d+)',
    ]

    for pattern in patterns:
        match = re.search(pattern, html)
        if match:
            try:
                return int(match.group(1))
            except Exception:
                continue

    return None


def extract_post_count_from_meta(driver: webdriver.Chrome) -> Optional[int]:
    metas = driver.find_elements(By.XPATH, "//meta[@property='og:description']")
    for meta in metas:
        content = (meta.get_attribute("content") or "").strip()
        if not content:
            continue

        # Most robust case: English og:description like "123 posts, 4,567 followers..."
        match = re.search(r"([0-9][0-9,\.KMBkmb]*)\s+posts?\b", content, flags=re.I)
        if match:
            count = normalize_count_text(match.group(1))
            if count is not None:
                return count

        # Locale fallback: parse the first numeric token in og:description
        match = re.search(r"^\s*([0-9][0-9,\.KMBkmb]*)\b", content)
        if match:
            count = normalize_count_text(match.group(1))
            if count is not None:
                return count

    return None


def extract_post_count_from_xpath(driver: webdriver.Chrome) -> Optional[int]:
    xpaths = [
        "//header//ul/li[1]//span[@title]",
        "//header//ul/li[1]//span/span",
        "//main//header//section//ul/li[1]//span[@title]",
        "//main//header//section//ul/li[1]//span/span",
    ]

    for xpath in xpaths:
        try:
            elements = driver.find_elements(By.XPATH, xpath)
            for el in elements:
                candidate = (el.get_attribute("title") or el.text or "").strip()
                count = normalize_count_text(candidate)
                if count is not None:
                    return count
        except Exception:
            continue
    return None


def get_profile_post_count(driver: webdriver.Chrome, username: str) -> Optional[int]:
    """Get post count using Selenium to establish proper session, then extract from page"""
    try:
        url = f"https://www.instagram.com/{username}/"
        driver.get(url)
        
        # Wait only for body tag with short timeout
        try:
            WebDriverWait(driver, 5).until(
                EC.presence_of_element_located((By.TAG_NAME, "body"))
            )
        except:
            pass
        
        # No sleep - extract immediately
        
        # Try page_source first (fastest)
        html = driver.page_source or ""
        patterns = [
            r'edge_owner_to_timeline_media\":\{\"count\":(\d+)',
            r'"edge_owner_to_timeline_media":\s*\{\s*"count":\s*(\d+)',
        ]
        for pattern in patterns:
            match = re.search(pattern, html)
            if match:
                try:
                    count = int(match.group(1))
                    print(f"ℹ️ {username} current profile post count (page_source): {count}")
                    return count
                except Exception:
                    continue
        
        # Try meta tag
        try:
            metas = driver.find_elements(By.XPATH, "//meta[@property='og:description']")
            for meta in metas:
                content = (meta.get_attribute("content") or "").strip()
                if not content:
                    continue
                match = re.search(r"([0-9][0-9,\.KMBkmb]*)\s+posts?\b", content, flags=re.I)
                if match:
                    count = normalize_count_text(match.group(1))
                    if count is not None:
                        print(f"ℹ️ {username} current profile post count (meta): {count}")
                        return count
        except:
            pass
        
        print(f"⚠️ Could not read post count for {username}")
        return None
    except Exception as exc:
        print(f"❌ Error getting post count: {exc}")
        return None


# ========= API fetch for changed accounts only =========
def get_profile_info(driver: webdriver.Chrome, username: str):
    """直接從目前已載入的 profile 頁面抽 timeline 資料，不重新開頁"""
    if not driver:
        return None

    try:
        html = driver.page_source or ""
        print(f"DEBUG profile html preview: {html[:1200]}")

        script_texts = re.findall(r"<script[^>]*>(.*?)</script>", html, flags=re.S | re.I)

        for raw in script_texts:
            if "edge_owner_to_timeline_media" not in raw:
                continue

            cleaned = raw.strip()
            candidates = [cleaned]

            brace_match = re.search(r"(\{.*\})", cleaned, flags=re.S)
            if brace_match:
                candidates.append(brace_match.group(1))

            for candidate in candidates:
                try:
                    blob = json.loads(candidate)
                    result = find_timeline_data(blob)
                    if result:
                        print(f"✅ {username} extracted profile JSON from current page")
                        return result
                except Exception:
                    continue

        # fallback: 直接從 HTML regex 抽
        edges = []
        seen = set()

        pattern = re.compile(
            r'"shortcode":"(?P<shortcode>[^"]+)".{0,2000}?"is_video":(?P<is_video>true|false).{0,4000}?"taken_at_timestamp":(?P<ts>\d+)',
            flags=re.S
        )

        for m in pattern.finditer(html):
            shortcode = m.group("shortcode")
            if shortcode in seen:
                continue
            seen.add(shortcode)

            is_video = m.group("is_video") == "true"
            ts = int(m.group("ts"))

            caption = ""
            snippet_start = max(0, m.start() - 500)
            snippet_end = min(len(html), m.end() + 3000)
            snippet = html[snippet_start:snippet_end]

            cap_match = re.search(
                r'"edge_media_to_caption":\{"edges":\[\{"node":\{"text":"(.*?)"\}\}\]\}',
                snippet,
                flags=re.S
            )
            if cap_match:
                caption = bytes(cap_match.group(1), "utf-8").decode("unicode_escape", errors="ignore")

            duration = None
            dur_match = re.search(r'"video_duration":([0-9.]+)', snippet)
            if dur_match:
                try:
                    duration = float(dur_match.group(1))
                except Exception:
                    duration = None

            edges.append({
                "node": {
                    "shortcode": shortcode,
                    "is_video": is_video,
                    "taken_at_timestamp": ts,
                    "video_duration": duration,
                    "edge_media_to_caption": {
                        "edges": [{"node": {"text": caption}}] if caption else []
                    },
                }
            })

        if edges:
            print(f"✅ {username} extracted {len(edges)} timeline nodes from current page fallback")
            return {
                "data": {
                    "user": {
                        "edge_owner_to_timeline_media": {
                            "edges": edges
                        }
                    }
                }
            }

        print(f"⚠️ {username} could not extract profile JSON from current page")
        return None

    except Exception as exc:
        print(f"❌ get_profile_info error: {exc}")
        return None


def find_timeline_data(obj, depth=0) -> Optional[dict]:
    """遞迴搜尋包含 edge_owner_to_timeline_media 的結構"""
    if depth > 10:
        return None

    if isinstance(obj, dict):
        if "edge_owner_to_timeline_media" in obj:
            # 包成 extract_reels_within_days 期望的格式
            return {"data": {"user": obj}}
        for v in obj.values():
            result = find_timeline_data(v, depth + 1)
            if result:
                return result

    elif isinstance(obj, list):
        for item in obj:
            result = find_timeline_data(item, depth + 1)
            if result:
                return result

    return None


def extract_reels_within_days(username: str, profile_json: dict, existing_shortcodes: set) -> list:
    results: list = []

    try:
        user = profile_json["data"]["user"]
        edges = user.get("edge_owner_to_timeline_media", {}).get("edges", [])
    except Exception as exc:
        print(f"⚠️ {username} profile JSON structure error: {exc}, skipping reel extraction")
        return results
    
    # If no edges found, that's OK - just return empty
    if not edges:
        print(f"ℹ️ {username} no timeline data available, skipping reel extraction")
        return results

    cutoff = now_local() - dt.timedelta(days=REELS_WINDOW_DAYS)

    for edge in edges:
        node = edge.get("node", {})
        shortcode = str(node.get("shortcode") or "").strip()
        is_video = node.get("is_video", False)
        timestamp = node.get("taken_at_timestamp")

        if not shortcode or not is_video or not timestamp:
            continue

        try:
            post_dt = dt.datetime.fromtimestamp(timestamp)
        except Exception:
            continue

        if post_dt < cutoff:
            continue

        if shortcode in existing_shortcodes:
            continue

        caption_edges = node.get("edge_media_to_caption", {}).get("edges", [])
        caption_text = caption_edges[0].get("node", {}).get("text", "") if caption_edges else ""

        results.append({
            "kol_account": username,
            "reels_shortcode": shortcode,
            "post_time": post_dt.strftime("%Y-%m-%d %H:%M:%S"),
            "duration": node.get("video_duration"),
            "caption": caption_text,
        })

    return results


def get_reel_detail_by_shortcode(shortcode: str, driver: Optional[webdriver.Chrome] = None):
    """Extract reel detail from page HTML using XPath - no API calls"""
    if not driver:
        return None
    
    try:
        driver.get(f"https://www.instagram.com/reel/{shortcode}/")
        time.sleep(0.3)
        
        html = driver.page_source or ""
        
        # Look for the media data in page source
        # Instagram stores it as JSON in the page
        match = re.search(r'"shortcode":"' + shortcode + r'".*?"__typename":"GraphImage".*?}', html)
        if not match:
            match = re.search(r'"shortcode":"' + shortcode + r'".*?"edge_media_to_caption".*?}', html)
        
        if match:
            try:
                json_str = match.group(0)
                # Extract key metrics from page
                node = {
                    "video_view_count": 0,
                    "video_play_count": 0,
                    "edge_liked_by": {"count": 0},
                    "edge_media_to_comment": {"count": 0},
                }
                
                # Try to extract view count
                view_match = re.search(r'"video_view_count":(\d+)', html)
                if view_match:
                    node["video_view_count"] = int(view_match.group(1))
                
                # Try to extract play count
                play_match = re.search(r'"video_play_count":(\d+)', html)
                if play_match:
                    node["video_play_count"] = int(play_match.group(1))
                
                # Try to extract likes
                like_match = re.search(r'"edge_liked_by":\s*\{\s*"count":(\d+)', html)
                if like_match:
                    node["edge_liked_by"]["count"] = int(like_match.group(1))
                
                # Try to extract comments
                comment_match = re.search(r'"edge_media_to_comment":\s*\{\s*"count":(\d+)', html)
                if comment_match:
                    node["edge_media_to_comment"]["count"] = int(comment_match.group(1))
                
                return node
            except Exception as e:
                print(f"⚠️ Parse error for {shortcode}: {e}")
        
        return None
        
    except Exception as exc:
        print(f"❌ Error getting reel detail {shortcode}: {exc}")
        return None


def parse_likes(node: dict) -> int:
    return (
        node.get("edge_liked_by", {}).get("count")
        or node.get("edge_media_preview_like", {}).get("count")
        or 0
    )


def parse_comments_count(node: dict) -> int:
    return (
        node.get("edge_media_to_comment", {}).get("count")
        or node.get("edge_media_to_parent_comment", {}).get("count")
        or 0
    )


def build_dynamic_snapshot(shortcode: str, node: dict) -> dict:
    return {
        "reels_shortcode": shortcode,
        "views": node.get("video_view_count", 0),
        "plays": node.get("video_play_count", 0),
        "likes": parse_likes(node),
        "comments": parse_comments_count(node),
        "timestamp": now_str(),
    }


def upsert_state_row(state_df: pd.DataFrame, username: str, current_count: Optional[int], status: str, changed: bool) -> pd.DataFrame:
    checked_at = now_str()
    changed_at = checked_at if changed else None

    existing_changed_at = None
    if not state_df.empty:
        match = state_df[state_df["kol_account"].astype(str) == username]
        if not match.empty:
            existing_changed_at = match.iloc[0].get("last_changed_at")

    row = {
        "kol_account": username,
        "profile_post_count": current_count,
        "last_checked_at": checked_at,
        "last_changed_at": changed_at or existing_changed_at,
        "check_status": status,
    }

    out = pd.concat([state_df, pd.DataFrame([row])], ignore_index=True)
    return dedupe_and_sort_state(out)


# ========= Main =========
def main() -> None:
    start_ts = time.time()

    # Ensure data directory exists
    ensure_parent_dir(KOL_INFO_FILE)

    # Initialize sample kol_info.csv if missing
    if not os.path.exists(KOL_INFO_FILE):
        print(f"⚠️ {KOL_INFO_FILE} not found, creating sample file")
        sample_df = pd.DataFrame({"kol_account": ["instagram", "nasa", "cristiano"]})
        sample_df.to_csv(KOL_INFO_FILE, index=False, encoding="utf-8-sig")
        print(f"✅ Created sample {KOL_INFO_FILE} - please edit with real accounts")
        print(f"⏹️ Exiting. Please add Instagram accounts to {KOL_INFO_FILE} and run again.")
        raise SystemExit(0)

    try:
        kol_df = pd.read_csv(KOL_INFO_FILE)
        print(f"✅ Read {KOL_INFO_FILE}")
    except Exception as exc:
        raise SystemExit(f"❌ Failed to read {KOL_INFO_FILE}: {exc}")

    if "kol_account" not in kol_df.columns:
        raise SystemExit("❌ kol_info.csv must contain kol_account column")

    if kol_df.empty or len(kol_df[kol_df["kol_account"].notna()]) == 0:
        raise SystemExit(f"❌ No valid accounts in {KOL_INFO_FILE}")

    state_df = read_or_init_csv(STATE_FILE, STATE_COLUMNS)
    static_df = read_or_init_csv(STATIC_FILE, STATIC_COLUMNS)
    dynamic_df = read_or_init_csv(DYNAMIC_FILE, DYNAMIC_COLUMNS)

    existing_shortcodes = set(static_df["reels_shortcode"].dropna().astype(str).tolist())
    new_static_rows: list = []
    new_dynamic_rows: list = []

    processed = 0
    changed_accounts = 0
    skipped_accounts = 0

    driver = build_driver()

    # 注入 cookie
    cookie_str = os.environ.get("IG_COOKIE", "")
    if cookie_str:
        load_cookies_from_string(driver, cookie_str)
    else:
        print("⚠️ IG_COOKIE not set, may be blocked")

    try:
        for _, row in kol_df.iterrows():
            username = str(row["kol_account"]).strip()
            if not username or username.lower() == "nan":
                continue

            print(f"\n=== Checking account: {username} ===")

            previous_count = None
            if not state_df.empty:
                matched = state_df[state_df["kol_account"].astype(str) == username]
                if not matched.empty:
                    try:
                        previous_count = int(float(matched.iloc[0]["profile_post_count"]))
                    except Exception:
                        previous_count = None

            current_count = get_profile_post_count(driver, username)
            processed += 1

            if current_count is None:
                state_df = upsert_state_row(state_df, username, previous_count, "count_read_failed", False)
                sleep_random(PROFILE_SLEEP_RANGE)
                continue

            if previous_count is not None and current_count == previous_count:
                print(f"⏭️ {username} unchanged ({current_count}), skipped")
                state_df = upsert_state_row(state_df, username, current_count, "skipped_same_count", False)
                skipped_accounts += 1
                sleep_random(PROFILE_SLEEP_RANGE)
                continue

            print(f"🔄 {username} changed: previous={previous_count}, current={current_count}")
            state_df = upsert_state_row(state_df, username, current_count, "changed_fetching", True)
            changed_accounts += 1

            profile_json = get_profile_info(username, driver)
            if not profile_json:
                print(f"⏭️ {username} profile data unavailable, skipping reel extraction")
                state_df = upsert_state_row(state_df, username, current_count, "changed_no_reel_data", True)
                sleep_random(PROFILE_SLEEP_RANGE)
                continue

            recent_new_reels = extract_reels_within_days(username, profile_json, existing_shortcodes)
            print(f"ℹ️ {username} new reels to append: {len(recent_new_reels)}")

            for static_row in recent_new_reels:
                shortcode = static_row["reels_shortcode"]
                detail_node = get_reel_detail_by_shortcode(shortcode, driver)

                if detail_node:
                    if static_row["duration"] is None:
                        static_row["duration"] = detail_node.get("video_duration")
                    new_dynamic_rows.append(build_dynamic_snapshot(shortcode, detail_node))
                else:
                    print(f"⚠️ Detail fetch failed for shortcode={shortcode}; static row will still be saved")

                new_static_rows.append(static_row)
                existing_shortcodes.add(shortcode)
                sleep_random(DETAIL_SLEEP_RANGE)

            state_df = upsert_state_row(state_df, username, current_count, "changed_saved", True)
            sleep_random(PROFILE_SLEEP_RANGE)

    except Exception as exc:
        print(f"❌ Unexpected error: {exc}")
        raise
    finally:
        driver.quit()

    static_df = dedupe_and_sort_static(pd.concat([static_df, pd.DataFrame(new_static_rows)], ignore_index=True))
    dynamic_df = dedupe_and_sort_dynamic(pd.concat([dynamic_df, pd.DataFrame(new_dynamic_rows)], ignore_index=True))
    state_df = dedupe_and_sort_state(state_df)

    save_csv(static_df, STATIC_FILE, STATIC_COLUMNS)
    save_csv(dynamic_df, DYNAMIC_FILE, DYNAMIC_COLUMNS)
    save_csv(state_df, STATE_FILE, STATE_COLUMNS)

    elapsed = round(time.time() - start_ts, 2)
    print("\n✅ Done")
    print(f"✅ Processed accounts: {processed}")
    print(f"✅ Changed accounts: {changed_accounts}")
    print(f"✅ Skipped accounts: {skipped_accounts}")
    print(f"✅ Total runtime: {elapsed} seconds")


if __name__ == "__main__":
    main()
