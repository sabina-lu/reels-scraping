# reels-scraping

A Python project for tracking Instagram posts and Reels from a configured list of accounts and collecting engagement metrics over time. It stores account scan state, post metadata, and engagement snapshots in CSV files, with GitHub Actions automating data collection and repository updates.

## How it works

The collection pipeline has two stages:

1. **Discover post shortcodes.** `get_new_reels.py` opens each account profile in headless Chrome using Selenium. It reads the profile's post count and compares it with the previous scan. For new accounts or accounts whose post count has changed, it extracts up to three shortcodes from JavaScript page data or links in the DOM. New entries are deduplicated by account and shortcode.
2. **Collect metadata and metrics.** `update_reels.py` reads the stored shortcodes and uses a requests session to query Instagram's internal GraphQL endpoints. It tries a primary endpoint, with a secondary endpoint used for supported fallback cases. Successful results fill missing post metadata and append engagement snapshots.

Discovery includes both `/reel/` and `/p/` links. The project collects metadata and metrics; it does not download media files.

## Project structure

```text
get_new_reels.py                 # Profile scanning and shortcode discovery
update_reels.py                  # Post metadata and engagement collection
requirements.txt                # Python dependencies
data/
  kol_info.csv                  # Accounts to track
  profile_post_state.csv        # Latest scan state for each account
  reels_static_info.csv         # Account, shortcode, and post metadata
  reels_dynamic_info.csv        # Historical engagement snapshots
.github/workflows/
  scrape-instagram-scan.yml     # Scheduled profile scanning
  scrape-instagram-update.yml   # Metric collection after a successful scan
```

## Setup

Use Python 3.11 to match the GitHub Actions environment. Google Chrome is required for profile scanning. The metadata and metrics script uses HTTP requests directly.

From the repository root:

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt
```

On Windows, activate the virtual environment with `.venv\Scripts\activate`.

### Account list

Edit `data/kol_info.csv` with one Instagram username per row. Use usernames without `@` or a profile URL:

```csv
kol_account
instagram
nasa
```

If the file does not exist, the scan script creates a sample account list and exits. Edit the list before running it again.

### Environment variables

| Variable | Description |
| --- | --- |
| `IG_COOKIE` | Instagram Cookie string used by the profile scanner, in the format `name=value; name2=value2`. When supplied, the scanner injects the cookies into Chrome and checks the login state. |
| `CHROME_BIN` | Optional path to the Chrome executable. Otherwise, the scanner checks the default macOS Chrome location and browser executables on `PATH`. |
| `TZ` | Timezone for local timestamps. The workflows use `Asia/Taipei`. |

For macOS or Linux, read the Cookie value from the terminal and export it:

```bash
read -r -s IG_COOKIE
export IG_COOKIE
export TZ=Asia/Taipei
```

After running `read`, paste the Cookie string and press Enter. Store the Cookie in the `IG_COOKIE` repository secret when using GitHub Actions. Keep login credentials out of version control.

The scripts do not automatically load `.env` files. `IG_COOKIE` is used by `get_new_reels.py`; `update_reels.py` creates a separate requests session without importing that Cookie.

## Usage

Run commands from the repository root so the default `data/` paths resolve correctly.

### Scan accounts

```bash
python get_new_reels.py
```

The script reads the account list and saved scan state, checks each profile's post count, extracts shortcodes when the count changes, and writes the shortcode list and updated account state. Accounts with unchanged post counts are skipped.

### Update metadata and engagement metrics

```bash
python update_reels.py
```

The script processes all entries in `data/reels_static_info.csv`. For each successful query, it fills empty publication time, duration, and caption fields and appends a row to `data/reels_dynamic_info.csv`.

To use different input and output files:

```bash
python update_reels.py --static /path/to/static.csv --dynamic /path/to/dynamic.csv
```

### Query a single post

```bash
python update_reels.py 'https://www.instagram.com/reel/SHORTCODE/'
```

This mode prints the result as JSON without writing to the CSV files. Instagram post URLs using `/p/SHORTCODE/` are also supported.

## Data files

### `data/kol_info.csv`

| Field | Description |
| --- | --- |
| `kol_account` | Instagram username to track. |

### `data/profile_post_state.csv`

| Field | Description |
| --- | --- |
| `kol_account` | Instagram username. |
| `profile_post_count` | Most recently stored profile post count. |
| `last_checked_at` | Time of the latest account check. |
| `last_changed_at` | Most recently recorded post-count change time. |
| `check_status` | Scan result or processing state. |

Scan states include `count_read_failed`, `skipped_same_count`, `changed_fetching`, `changed_no_reel_data`, and `changed_saved`. When shortcode extraction fails after a count change, the scanner retains the previous post count for a later attempt.

### `data/reels_static_info.csv`

| Field | Description |
| --- | --- |
| `kol_account` | Account associated with the post. |
| `reels_shortcode` | Instagram post identifier. |
| `post_time` | Publication time. |
| `duration` | Video duration in seconds. |
| `caption` | Post caption. |

The scanner reads and writes the account and shortcode columns. The update script writes the full five-column metadata table and fills empty metadata fields from query results.

### `data/reels_dynamic_info.csv`

| Field | Description |
| --- | --- |
| `reels_shortcode` | Instagram post identifier. |
| `views` | View count derived from the endpoint response. |
| `plays` | Play count derived from the endpoint response. |
| `likes` | Like count. |
| `comments` | Comment count. |
| `timestamp` | Start time of the collection batch. |

Each successful batch query appends a snapshot, allowing the same shortcode to appear at multiple collection times. Missing metrics are stored as empty values. View and play counts are mapped from the fields available in the endpoint response, with fallback fields used by the secondary endpoint.

Generated CSV files use UTF-8 with a byte order mark (BOM). Timestamps use `YYYY-MM-DD HH:MM:SS` in the process's local timezone, without an embedded timezone offset.

## GitHub Actions

### Profile scan

`.github/workflows/scrape-instagram-scan.yml` runs on the hourly schedule `0 * * * *` and supports manual dispatch. It installs Python dependencies and Chrome, runs `get_new_reels.py`, and commits changed CSV files.

Configure a repository secret named `IG_COOKIE` for authenticated profile scanning.

### Metadata and metrics update

`.github/workflows/scrape-instagram-update.yml` is triggered when the scan workflow completes. Its job runs when the scan conclusion is `success`, executes `update_reels.py`, and commits changed CSV files.

Both jobs use Python 3.11, set `TZ=Asia/Taipei`, have a 30-minute timeout, and request `contents: write` permission to push data updates. The synchronization commands reference the `main` branch.
