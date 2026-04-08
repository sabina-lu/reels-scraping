import os
import re
import time
import json
import random
import shutil
import datetime as dt
from typing import Optional, List, Dict

import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait


# ========= Basic config =========
DATA_DIR = "data"
KOL_INFO_FILE = os.path.join(DATA_DIR, "kol_info.csv")
STATE_FILE = os.path.join(DATA_DIR, "profile_post_state.csv")
STATIC_FILE = os.path.join(DATA_DIR, "reels_static_info.csv")

IG_APP_ID = "936619743392459"

PROFILE_SLEEP_RANGE = (0.5, 1.2)

STATIC_COLUMNS = [
    "kol_account",
    "reels_shortcode",
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


def dedupe_and_sort_state(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df.copy()

    out = df.copy()
    out["_checked_dt"] = pd.to_datetime(out["last_checked_at"], errors="coerce")
    out = out.sort_values(["_checked_dt", "kol_account"], ascending=[False, True])
    out = out.drop_duplicates(subset=["kol_account"], keep="first")
    out = out.drop(columns=["_checked_dt"], errors="ignore")
    return out


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
        except Exception as exc:
            print(f"⚠️ Failed to add cookie {cookie['name']}: {exc}")

    driver.refresh()
    time.sleep(4)
    print("✅ Cookies injected")


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
        print("⚠️ Could not locate Chrome binary, using system default")

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


def extract_count_from_js(driver: webdriver.Chrome) -> Optional[int]:
    scripts = [
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
        """
        try {
            const u = window._sharedData?.entry_data?.ProfilePage?.[0]?.graphql?.user;
            if (u && u.edge_owner_to_timeline_media) {
                return u.edge_owner_to_timeline_media.count;
            }
        } catch(e) {}
        return null;
        """,
        """
        try {
            const root = document.getElementById('react-root') || document.body;
            const fiberKey = Object.keys(root).find(
                k => k.startsWith('__reactFiber') || k.startsWith('__reactInternalInstance')
            );
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


def extract_timeline_from_js(driver: webdriver.Chrome) -> Optional[dict]:
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


def wait_for_profile_ready(driver: webdriver.Chrome, timeout: int = 20) -> bool:
    try:
        WebDriverWait(driver, timeout).until(
            lambda d: d.execute_script("return document.readyState") == "complete"
        )

        for _ in range(8):
            js_ok = extract_timeline_from_js(driver) is not None
            if js_ok:
                return True

            anchors = driver.find_elements(
                By.XPATH,
                '//a[@href and (contains(@href, "/p/") or contains(@href, "/reel/") or starts-with(@href, "/p/") or starts-with(@href, "/reel/"))]'
            )
            if anchors:
                return True

            driver.execute_script("window.scrollTo(0, 500);")
            time.sleep(1.2)
            driver.execute_script("window.scrollTo(0, 0);")
            time.sleep(1.2)

        return False
    except Exception:
        return False


def get_profile_post_count(driver: webdriver.Chrome, username: str) -> Optional[int]:
    try:
        url = f"https://www.instagram.com/{username}/"
        driver.get(url)

        hydrated = wait_for_profile_ready(driver, timeout=20)
        if not hydrated:
            print(f"⚠️ {username}: page did not fully hydrate within timeout")

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
                    print(f"ℹ️ {username} post count (page_source regex): {count}")
                    return count
                except Exception:
                    continue

        count = extract_count_from_js(driver)
        if count is not None:
            print(f"ℹ️ {username} post count (JS memory): {count}")
            return count

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


# ========= Shortcode extraction =========
def build_result(username: str, shortcode: str) -> Dict[str, str]:
    return {
        "kol_account": username,
        "shortcode": shortcode,
    }


def extract_shortcode_from_href(href: str) -> Optional[str]:
    if not href:
        return None

    m = re.search(r"/(?:p|reel)/([^/?#]+)/?", href)
    return m.group(1) if m else None


def extract_results_from_edges(
    username: str,
    edges: list,
    max_items: int = 3
) -> Optional[List[Dict[str, str]]]:
    seen = set()
    results = []

    for edge in edges:
        shortcode = edge.get("node", {}).get("shortcode")
        if not shortcode or shortcode in seen:
            continue

        seen.add(shortcode)
        results.append(build_result(username, shortcode))

        if len(results) >= max_items:
            break

    return results or None


def extract_results_from_js_data(
    username: str,
    js_data: dict,
    max_items: int = 3
) -> Optional[List[Dict[str, str]]]:
    try:
        edges = js_data["data"]["user"]["edge_owner_to_timeline_media"]["edges"]
        return extract_results_from_edges(username, edges, max_items=max_items)
    except Exception:
        return None


def extract_results_from_dom(
    username: str,
    driver,
    max_items: int = 3
) -> Optional[List[Dict[str, str]]]:
    anchors = driver.find_elements(
        By.XPATH,
        '//a[@href and (contains(@href, "/p/") or contains(@href, "/reel/") or starts-with(@href, "/p/") or starts-with(@href, "/reel/"))]'
    )

    print(f"ℹ️ {username} raw anchors found: {len(anchors)}")

    seen = set()
    results = []

    for a in anchors:
        href = a.get_attribute("href") or ""
        shortcode = extract_shortcode_from_href(href)

        if not shortcode or shortcode in seen:
            continue

        seen.add(shortcode)
        results.append(build_result(username, shortcode))
        print(f"  + (dom) kol_account={username}, shortcode={shortcode}")

        if len(results) >= max_items:
            break

    return results or None


def get_profile_info(
    username: str,
    driver
) -> Optional[List[Dict[str, str]]]:
    if not driver:
        return None

    max_items = 3

    try:
        ready = wait_for_profile_ready(driver, timeout=20)
        if not ready:
            print(f"⚠️ {username} profile page not fully ready")

        js_data = extract_timeline_from_js(driver)
        if js_data:
            results = extract_results_from_js_data(username, js_data, max_items=max_items)
            if results:
                print(f"✅ {username} extracted {len(results)} items from JS memory")
                return results

        results = extract_results_from_dom(username, driver, max_items=max_items)
        if results:
            print(f"✅ {username} extracted {len(results)} items from DOM")
            return results

        print(f"⚠️ {username} could not extract shortcode data from current page")
        return None

    except Exception as exc:
        print(f"❌ get_profile_info error: {exc}")
        return None


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

    existing_pairs = set(
        zip(
            static_df["kol_account"].fillna("").astype(str),
            static_df["reels_shortcode"].fillna("").astype(str),
        )
    )
    new_static_rows: List[Dict[str, str]] = []

    processed = 0
    changed_accounts = 0
    skipped_accounts = 0

    driver = build_driver()

    cookie_str = os.environ.get("IG_COOKIE", "")
    if cookie_str:
        load_cookies_from_string(driver, cookie_str)
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

            current_count = get_profile_post_count(driver, username)
            processed += 1

            if current_count is None:
                state_df = upsert_state_row(
                    state_df,
                    username,
                    previous_count,
                    "count_read_failed",
                    False,
                )
                sleep_random(PROFILE_SLEEP_RANGE)
                continue

            if previous_count is not None and current_count == previous_count:
                print(f"⏭️ {username} unchanged ({current_count}), skipped")
                state_df = upsert_state_row(
                    state_df,
                    username,
                    current_count,
                    "skipped_same_count",
                    False,
                )
                skipped_accounts += 1
                sleep_random(PROFILE_SLEEP_RANGE)
                continue

            print(f"🔄 {username} changed: previous={previous_count}, current={current_count}")
            state_df = upsert_state_row(
                state_df,
                username,
                current_count,
                "changed_fetching",
                True,
            )
            changed_accounts += 1

            profile_rows = get_profile_info(username, driver)
            if not profile_rows:
                print(f"⏭️ {username} profile data unavailable, skipping shortcode extraction")
                state_df = upsert_state_row(
                    state_df,
                    username,
                    previous_count,   # ← 改成 previous_count，不寫入新數字
                    "changed_no_reel_data",
                    False,            # ← changed 也改 False，保留舊的 last_changed_at
                )
                sleep_random(PROFILE_SLEEP_RANGE)
                continue

            new_count = 0
            for item in profile_rows:
                shortcode = item.get("shortcode")
                if not shortcode:
                    continue

                pair = (username, shortcode)
                if pair in existing_pairs:
                    continue

                static_row = {
                    "kol_account": username,
                    "reels_shortcode": shortcode,
                }
                new_static_rows.append(static_row)
                existing_pairs.add(pair)
                new_count += 1
                print(f"✅ Static row appended: {shortcode}")

            print(f"ℹ️ {username} new shortcodes to append: {new_count}")

            state_df = upsert_state_row(
                state_df,
                username,
                current_count,
                "changed_saved",
                True,
            )
            sleep_random(PROFILE_SLEEP_RANGE)

    except Exception as exc:
        print(f"❌ Unexpected error: {exc}")
        raise
    finally:
        driver.quit()

    state_df = dedupe_and_sort_state(state_df)

    if new_static_rows:
        static_df = pd.concat([static_df, pd.DataFrame(new_static_rows)], ignore_index=True)
        static_df = static_df.drop_duplicates(subset=["kol_account", "reels_shortcode"], keep="first")

    save_csv(static_df, STATIC_FILE, STATIC_COLUMNS)
    save_csv(state_df, STATE_FILE, STATE_COLUMNS)

    elapsed = round(time.time() - start_ts, 2)
    print("\n✅ Done")
    print(f"✅ Processed accounts : {processed}")
    print(f"✅ Changed accounts   : {changed_accounts}")
    print(f"✅ Skipped accounts   : {skipped_accounts}")
    print(f"✅ Total runtime      : {elapsed} seconds")


if __name__ == "__main__":
    main()