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
PROFILE_SLEEP_RANGE = (0.5, 1.2)   # slightly longer to let JS hydrate
DETAIL_SLEEP_RANGE  = (0.3, 0.6)

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
    driver.get("https://www.instagram.com/")
    time.sleep(3)

    for cookie in parse_cookie_string(cookie_str):
        try:
            driver.add_cookie(cookie)
        except Exception as e:
            print(f"⚠️ Failed to add cookie {cookie['name']}: {e}")

    driver.refresh()
    time.sleep(4)
    print("✅ Cookies injected")


# [FIX #4] Verify login succeeded before proceeding
def verify_logged_in(driver: webdriver.Chrome) -> bool:
    try:
        driver.get("https://www.instagram.com/accounts/activity/")
        time.sleep(3)

        if "login" in driver.current_url:
            print("❌ Cookie login verification failed — current URL indicates login wall")
            return False

        body_text = (driver.find_element(By.TAG_NAME, "body").text or "").lower()
        if "log in" in body_text or "login" in body_text:
            print("❌ Login wall text detected")
            return False

        print("✅ Login verified successfully")
        return True
    except Exception as exc:
        print(f"⚠️ Could not verify login status: {exc}")
        return False


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
        macos_chrome = "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"
        if os.path.exists(macos_chrome):
            chrome_bin = macos_chrome
        else:
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


# [FIX #3] Extract post count from in-memory JS store via execute_script
def extract_count_from_js(driver: webdriver.Chrome) -> Optional[int]:
    """Query Instagram's in-memory React/JS data store for timeline count."""
    scripts = [
        # Try window.__additionalDataLoaded (older IG)
        """
        try {
            const data = window.__additionalDataLoaded || {};
            for (const key of Object.keys(data)) {
                const u = (data[key]?.graphql?.user) || (data[key]?.data?.user);
                if (u && u.edge_owner_to_timeline_media) {
                    return u.edge_owner_to_timeline_media.count;
                }
            }
        } catch(e) {}
        return null;
        """,
        # Try window._sharedData (legacy IG)
        """
        try {
            const u = window._sharedData?.entry_data?.ProfilePage?.[0]?.graphql?.user;
            if (u && u.edge_owner_to_timeline_media) {
                return u.edge_owner_to_timeline_media.count;
            }
        } catch(e) {}
        return null;
        """,
        # Try __reactFiber / __reactProps traversal (modern IG)
        """
        try {
            const root = document.getElementById('react-root') || document.body;
            const fiberKey = Object.keys(root).find(k => k.startsWith('__reactFiber') || k.startsWith('__reactInternalInstance'));
            if (!fiberKey) return null;
            let fiber = root[fiberKey];
            let depth = 0;
            while (fiber && depth < 200) {
                const props = fiber.memoizedProps || {};
                const data = props.data || props.initialData || props.serverData;
                if (data?.user?.edge_owner_to_timeline_media?.count !== undefined) {
                    return data.user.edge_owner_to_timeline_media.count;
                }
                fiber = fiber.child || fiber.sibling || (fiber.return && fiber.return.sibling);
                depth++;
            }
        } catch(e) {}
        return null;
        """,
    ]
    for script in scripts:
        try:
            result = driver.execute_script(script)
            if result is not None:
                return int(result)
        except Exception:
            continue
    return None


# [FIX #3] Extract full timeline edges from in-memory JS store
def extract_timeline_from_js(driver: webdriver.Chrome) -> Optional[dict]:
    """Pull the full edge_owner_to_timeline_media structure from JS memory."""
    scripts = [
        """
        try {
            const data = window.__additionalDataLoaded || {};
            for (const key of Object.keys(data)) {
                const u = (data[key]?.graphql?.user) || (data[key]?.data?.user);
                if (u && u.edge_owner_to_timeline_media && u.edge_owner_to_timeline_media.edges) {
                    return JSON.stringify({data: {user: u}});
                }
            }
        } catch(e) {}
        return null;
        """,
        """
        try {
            const u = window._sharedData?.entry_data?.ProfilePage?.[0]?.graphql?.user;
            if (u && u.edge_owner_to_timeline_media && u.edge_owner_to_timeline_media.edges) {
                return JSON.stringify({data: {user: u}});
            }
        } catch(e) {}
        return null;
        """,
    ]
    for script in scripts:
        try:
            result = driver.execute_script(script)
            if result:
                return json.loads(result)
        except Exception:
            continue
    return None


# [FIX #1] Wait for JS hydration before reading page source
def wait_for_profile_ready(driver: webdriver.Chrome, timeout: int = 20) -> bool:
    try:
        WebDriverWait(driver, timeout).until(
            lambda d: d.execute_script("return document.readyState") == "complete"
        )

        for _ in range(8):
            # 1) JS memory 有資料
            js_ok = extract_timeline_from_js(driver) is not None
            if js_ok:
                return True

            # 2) DOM 已經有貼文/reel 連結
            anchors = driver.find_elements(
                By.XPATH,
                '//a[@href and (contains(@href, "/p/") or contains(@href, "/reel/") or starts-with(@href, "/p/") or starts-with(@href, "/reel/"))]'
            )
            if anchors:
                return True

            # 3) 幫助 lazy load / hydration
            driver.execute_script("window.scrollTo(0, 500);")
            time.sleep(1.2)
            driver.execute_script("window.scrollTo(0, 0);")
            time.sleep(1.2)

        return False
    except Exception:
        return False


def get_profile_post_count(driver: webdriver.Chrome, username: str) -> Optional[int]:
    """
    Navigate to profile page, wait for JS hydration, then extract post count
    via (in priority order):
      1. Regex on page source (edge_owner_to_timeline_media count)
      2. execute_script querying JS memory
      3. og:description meta tag
      4. XPath header stats
    """
    try:
        url = f"https://www.instagram.com/{username}/"
        driver.get(url)

        # [FIX #1] Wait for JS-hydrated data rather than just <body>
        hydrated = wait_for_profile_ready(driver, timeout=20)
        if not hydrated:
            print(f"⚠️ {username}: page did not fully hydrate within timeout")

        html = driver.page_source or ""

        # 1. Regex on page source
        patterns = [
            r'edge_owner_to_timeline_media\":\{\"count\":(\d+)',
            r'"edge_owner_to_timeline_media":\s*\{\s*"count":\s*(\d+)',
        ]
        for pattern in patterns:
            match = re.search(pattern, html)
            if match:
                try:
                    count = int(match.group(1))
                    print(f"ℹ️ {username} post count (page_source regex): {count}")
                    return count
                except Exception:
                    continue

        # [FIX #3] 2. execute_script JS memory query
        count = extract_count_from_js(driver)
        if count is not None:
            print(f"ℹ️ {username} post count (JS memory): {count}")
            return count

        # 3. og:description meta tag
        try:
            metas = driver.find_elements(By.XPATH, "//meta[@property='og:description']")
            for meta in metas:
                content = (meta.get_attribute("content") or "").strip()
                if not content:
                    continue
                m = re.search(r"([0-9][0-9,\.KMBkmb]*)\s+posts?\b", content, flags=re.I)
                if m:
                    count = normalize_count_text(m.group(1))
                    if count is not None:
                        print(f"ℹ️ {username} post count (og:description): {count}")
                        return count
        except Exception:
            pass

        # 4. XPath header stats
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
                        print(f"ℹ️ {username} post count (XPath): {count}")
                        return count
            except Exception:
                continue

        print(f"⚠️ Could not read post count for {username}")
        return None

    except Exception as exc:
        print(f"❌ Error getting post count for {username}: {exc}")
        return None


# ========= API fetch for changed accounts only =========
def get_profile_info(username: str, driver: webdriver.Chrome) -> Optional[dict]:
    if not driver:
        return None

    try:
        ready = wait_for_profile_ready(driver, timeout=20)
        if not ready:
            print(f"⚠️ {username} profile page not fully ready")

        html = driver.page_source or ""

        # 1. JS memory first
        js_data = extract_timeline_from_js(driver)
        if js_data:
            print(f"✅ {username} extracted timeline from JS memory")
            return js_data

        # 2. DOM fallback: 從頁面上已經 render 的連結建 edges
        anchors = anchors[:3]

        seen = set()
        edges = []

        for a in anchors:
            href = a.get_attribute("href") or ""
            if not href:
                continue

            shortcode = None
            is_video = "/reel/" in href

            parts = [p for p in href.split("/") if p]
            if "p" in parts:
                idx = parts.index("p")
                if idx + 1 < len(parts):
                    shortcode = parts[idx + 1]
            elif "reel" in parts:
                idx = parts.index("reel")
                if idx + 1 < len(parts):
                    shortcode = parts[idx + 1]

            if not shortcode or shortcode in seen:
                continue
            seen.add(shortcode)

            edges.append({
                "node": {
                    "shortcode": shortcode,
                    "is_video": is_video,
                    "taken_at_timestamp": None,
                    "video_duration": None,
                    "edge_media_to_caption": {"edges": []},
                }
            })
         
        if edges:
            print(f"✅ {username} extracted {len(edges)} timeline nodes from DOM anchors")
            return {
                "data": {
                    "user": {
                        "edge_owner_to_timeline_media": {
                            "edges": edges
                        }
                    }
                }
            }

        # 3. 原本 script / regex fallback 保留
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
                        print(f"✅ {username} extracted timeline from <script> JSON")
                        return result
                except Exception:
                    continue

        print(f"⚠️ {username} could not extract timeline data from current page")
        return None

    except Exception as exc:
        print(f"❌ get_profile_info error: {exc}")
        return None


def find_timeline_data(obj, depth: int = 0) -> Optional[dict]:
    """Recursively search a parsed JSON blob for edge_owner_to_timeline_media."""
    if depth > 10:
        return None
    if isinstance(obj, dict):
        if "edge_owner_to_timeline_media" in obj:
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

    if not edges:
        print(f"ℹ️ {username} no timeline edges available, skipping reel extraction")
        return results

    cutoff = now_local() - dt.timedelta(days=REELS_WINDOW_DAYS)

    skipped_no_shortcode = 0
    skipped_not_video = 0
    skipped_no_timestamp = 0
    skipped_old = 0
    skipped_existing = 0
    accepted = 0

    for i, edge in enumerate(edges, start=1):
        node = edge.get("node", {})
        shortcode = str(node.get("shortcode") or "").strip()
        is_video = node.get("is_video", False)
        timestamp = node.get("taken_at_timestamp")

        if not shortcode:
            skipped_no_shortcode += 1
            print(f"SKIP #{i}: no shortcode")
            continue

        if not is_video:
            skipped_not_video += 1
            print(f"SKIP #{i}: shortcode={shortcode} is not video")
            continue

        if shortcode in existing_shortcodes:
            skipped_existing += 1
            print(f"SKIP #{i}: shortcode={shortcode} already exists in static_df")
            continue

        post_dt = None
        if timestamp:
            try:
                post_dt = dt.datetime.fromtimestamp(timestamp)
            except Exception as exc:
                skipped_no_timestamp += 1
                print(f"SKIP #{i}: shortcode={shortcode} timestamp parse failed: {exc}")
                continue
        else:
            skipped_no_timestamp += 1
            print(f"WARN #{i}: shortcode={shortcode} is video but no taken_at_timestamp, will try detail page later")

        if post_dt is not None and post_dt < cutoff:
            skipped_old += 1
            print(
                f"SKIP #{i}: shortcode={shortcode} too old "
                f"post_dt={post_dt.strftime('%Y-%m-%d %H:%M:%S')} cutoff={cutoff.strftime('%Y-%m-%d %H:%M:%S')}"
            )
            continue

        caption_edges = node.get("edge_media_to_caption", {}).get("edges", [])
        caption_text = caption_edges[0].get("node", {}).get("text", "") if caption_edges else ""

        results.append({
            "kol_account": username,
            "reels_shortcode": shortcode,
            "post_time": post_dt.strftime("%Y-%m-%d %H:%M:%S") if post_dt else None,
            "duration": node.get("video_duration"),
            "caption": caption_text,
        })
        accepted += 1
        print(
            f"KEEP #{i}: shortcode={shortcode} "
            f"post_dt={post_dt.strftime('%Y-%m-%d %H:%M:%S') if post_dt else 'None'}"
        )

    print(
        f"ℹ️ {username} reel filter summary | "
        f"total_edges={len(edges)} | accepted={accepted} | "
        f"no_shortcode={skipped_no_shortcode} | "
        f"not_video={skipped_not_video} | "
        f"no_timestamp={skipped_no_timestamp} | "
        f"too_old={skipped_old} | "
        f"already_exists={skipped_existing}"
    )

    return results


def get_post_timestamp(driver):
    try:
        time_el = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.TAG_NAME, "time"))
        )
        dt_str = time_el.get_attribute("datetime")
        return dt.datetime.fromisoformat(dt_str.replace("Z", "+00:00"))
    except Exception:
        return None


def wait_for_profile_ready(driver: webdriver.Chrome, timeout: int = 20) -> bool:
    try:
        WebDriverWait(driver, timeout).until(
            lambda d: d.execute_script("return document.readyState") == "complete"
        )

        for _ in range(8):
            # 1) JS memory 有資料
            js_ok = extract_timeline_from_js(driver) is not None
            if js_ok:
                return True

            # 2) DOM 已經有貼文/reel 連結
            anchors = driver.find_elements(
                By.XPATH,
                '//a[@href and (contains(@href, "/p/") or contains(@href, "/reel/") or starts-with(@href, "/p/") or starts-with(@href, "/reel/"))]'
            )
            if anchors:
                return True

            # 3) 幫助 lazy load / hydration
            driver.execute_script("window.scrollTo(0, 500);")
            time.sleep(1.2)
            driver.execute_script("window.scrollTo(0, 0);")
            time.sleep(1.2)

        return False
    except Exception:
        return False


def get_reel_detail_by_shortcode(shortcode: str, driver: Optional[webdriver.Chrome] = None) -> Optional[dict]:
    if not driver:
        return None

    try:
        driver.get(f"https://www.instagram.com/reel/{shortcode}/")
        wait_for_profile_ready(driver, timeout=10)

        html = driver.page_source or ""

        node: dict = {
            "video_view_count": 0,
            "video_play_count": 0,
            "edge_liked_by": {"count": 0},
            "edge_media_to_comment": {"count": 0},
        }
        found_any = False

        post_dt = get_post_timestamp(driver)
        if post_dt is not None:
            node["post_time"] = post_dt.strftime("%Y-%m-%d %H:%M:%S")
            node["taken_at_timestamp"] = int(post_dt.timestamp())

        try:
            result = driver.execute_script(f"""
                try {{
                    const data = window.__additionalDataLoaded || {{}};
                    for (const key of Object.keys(data)) {{
                        const items = data[key]?.items || [];
                        for (const item of items) {{
                            if (item.code === '{shortcode}' || item.shortcode === '{shortcode}') {{
                                return JSON.stringify({{
                                    view_count: item.view_count || item.play_count || 0,
                                    play_count: item.play_count || 0,
                                    like_count: item.like_count || 0,
                                    comment_count: item.comment_count || 0
                                }});
                            }}
                        }}
                    }}
                }} catch(e) {{}}
                return null;
            """)
            if result:
                d = json.loads(result)
                node["video_view_count"] = d.get("view_count", 0)
                node["video_play_count"] = d.get("play_count", 0)
                node["edge_liked_by"]["count"] = d.get("like_count", 0)
                node["edge_media_to_comment"]["count"] = d.get("comment_count", 0)
                found_any = True
        except Exception:
            pass

        if not found_any:
            view_match = re.search(r'"video_view_count":(\d+)', html)
            if view_match:
                node["video_view_count"] = int(view_match.group(1))
                found_any = True

            play_match = re.search(r'"video_play_count":(\d+)', html)
            if play_match:
                node["video_play_count"] = int(play_match.group(1))
                found_any = True

            like_match = re.search(r'"edge_liked_by":\s*\{\s*"count":(\d+)', html)
            if like_match:
                node["edge_liked_by"]["count"] = int(like_match.group(1))
                found_any = True

            comment_match = re.search(r'"edge_media_to_comment":\s*\{\s*"count":(\d+)', html)
            if comment_match:
                node["edge_media_to_comment"]["count"] = int(comment_match.group(1))
                found_any = True

        if found_any or post_dt is not None:
            return node

        print(f"⚠️ No metrics found for reel {shortcode}")
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


def upsert_state_row(
    state_df: pd.DataFrame,
    username: str,
    current_count: Optional[int],
    status: str,
    changed: bool,
) -> pd.DataFrame:
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

    ensure_parent_dir(KOL_INFO_FILE)

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

    # Inject cookies
    cookie_str = os.environ.get("IG_COOKIE", "")
    if cookie_str:
        load_cookies_from_string(driver, cookie_str)

        # [FIX #4] Verify login before proceeding
        if not verify_logged_in(driver):
            print("⚠️ Login verification failed. Timeline data may be unavailable.")
            print("⚠️ Check that IG_COOKIE is valid and not expired.")
    else:
        print("⚠️ IG_COOKIE not set — authenticated data will not be available")

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

            # [FIX #1+#2] get_profile_post_count now waits for JS hydration.
            # get_profile_info MUST be called immediately after, before any
            # other driver.get() call, so it can reuse the already-loaded page.
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

            # [FIX #2] Call get_profile_info immediately — driver is still on the profile page
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
                print(f"➡️ Fetching detail for shortcode={shortcode}")

                detail_node = get_reel_detail_by_shortcode(shortcode, driver)

                if detail_node:
                    if static_row["duration"] is None:
                        static_row["duration"] = detail_node.get("video_duration")

                    if not static_row.get("post_time"):
                        static_row["post_time"] = detail_node.get("post_time")

                    snapshot = build_dynamic_snapshot(shortcode, detail_node)
                    new_dynamic_rows.append(snapshot)

                    print(
                        f"✅ Detail fetched: {shortcode} | "
                        f"post_time={static_row.get('post_time')} | "
                        f"views={snapshot['views']} plays={snapshot['plays']} "
                        f"likes={snapshot['likes']} comments={snapshot['comments']}"
                    )
                else:
                    print(f"⚠️ Detail fetch failed for shortcode={shortcode}; static row will still be saved")

                new_static_rows.append(static_row)
                print(f"✅ Static row appended: {shortcode}")

                if not static_row.get("post_time"):
                    print(f"⚠️ {shortcode} still has no post_time after detail fetch")
                else:
                    print(f"✅ {shortcode} post_time resolved: {static_row['post_time']}")

                existing_shortcodes.add(shortcode)
                sleep_random(DETAIL_SLEEP_RANGE)

            state_df = upsert_state_row(state_df, username, current_count, "changed_saved", True)
            sleep_random(PROFILE_SLEEP_RANGE)

    except Exception as exc:
        print(f"❌ Unexpected error: {exc}")
        raise
    finally:
        driver.quit()

    static_df = dedupe_and_sort_static(
        pd.concat([static_df, pd.DataFrame(new_static_rows)], ignore_index=True)
    )
    dynamic_df = dedupe_and_sort_dynamic(
        pd.concat([dynamic_df, pd.DataFrame(new_dynamic_rows)], ignore_index=True)
    )
    state_df = dedupe_and_sort_state(state_df)

    save_csv(static_df, STATIC_FILE, STATIC_COLUMNS)
    save_csv(dynamic_df, DYNAMIC_FILE, DYNAMIC_COLUMNS)
    save_csv(state_df, STATE_FILE, STATE_COLUMNS)

    elapsed = round(time.time() - start_ts, 2)
    print("\n✅ Done")
    print(f"✅ Processed accounts : {processed}")
    print(f"✅ Changed accounts   : {changed_accounts}")
    print(f"✅ Skipped accounts   : {skipped_accounts}")
    print(f"✅ Total runtime      : {elapsed} seconds")


if __name__ == "__main__":
    main()