#!/usr/bin/env python3
import os
import logging
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any

import pycountry
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ============================================================
# SETTINGS
# ============================================================
TIMEOUT = 7.0
MAX_THREADS = 50
OUTPUT_FOLDER = Path("active_m3u")
EXTRA_FILE_MYANMAR = "extra_myanmar.m3u"
USER_AGENT = "IPTV-Playlist-Builder/2.0"

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
log = logging.getLogger("iptv_playlist_builder")

GITHUB_APIS = [
    "https://api.github.com/repos/iptv-org/iptv/contents/streams",
    "https://api.github.com/repos/YueChan/Live/contents",
    "https://api.github.com/repos/qiangi/m3u/contents",
    "https://api.github.com/repos/youshandefeiyang/IPTV-URL-1/contents",
    "https://api.github.com/repos/youshandefeiyang/IPTV-URL/contents",
    "https://api.github.com/repos/mursor1985/LIVE/contents"
]

CUSTOM_ONLINE_PLAYLISTS = {
    "Migu_Playlist.m3u": "https://migu.yang-1989.xyz/playlist.m3u",
    "Waastudios.m3u": "http://events.waastudios.dpdns.org:8877/sub/wDIWWCqpBc4hvIBFOXCX8ZrhPEOPCyjV/playlist.m3u",
    "MyIPTV.m3u": "https://github.chenc.dev/raw.githubusercontent.com/CKL1211/eric/refs/heads/master/MyIPTV.m3u"
}

_THREAD_LOCAL = threading.local()

# ============================================================
# EXPERT'S NETWORK SESSION MANAGER
# ============================================================
def get_session() -> requests.Session:
    session = getattr(_THREAD_LOCAL, "session", None)
    if session is None:
        session = requests.Session()
        session.headers.update({
            "User-Agent": USER_AGENT,
            "Accept": "*/*",
            "Connection": "keep-alive",
        })
        retries = Retry(
            total=2, connect=2, read=1, status=2,
            backoff_factor=0.35,
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=frozenset({"GET", "HEAD"}),
            respect_retry_after_header=True,
            raise_on_status=False,
        )
        adapter = HTTPAdapter(max_retries=retries, pool_connections=8, pool_maxsize=8)
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        _THREAD_LOCAL.session = session
    return session

# ============================================================
# HELPERS
# ============================================================
def get_country_name(filename: str) -> str:
    name = os.path.splitext(filename)[0]
    lower_name = name.lower()
    parts = lower_name.split("_", 1)
    country_code = parts[0]
    country = pycountry.countries.get(alpha_2=country_code.upper())
    
    if country:
        country_name = country.name.replace(" ", "_")
        if len(parts) > 1:
            return f"{country_name}_{parts[1]}.m3u"
        return f"{country_name}.m3u"
    return f"{name.title()}.m3u"

def _url_key(url: str) -> str:
    return url.strip().lower()

def parse_m3u_text(text: str) -> list[dict[str, Any]]:
    channels = []
    current_extinf = "#EXTINF:-1,Unknown Channel"
    
    for line in text.splitlines():
        line = line.strip()
        if not line: continue
        if line.startswith("#EXTINF"):
            current_extinf = line
        elif line.startswith("http"):
            channels.append({
                "extinf": current_extinf,
                "url": line
            })
            current_extinf = "#EXTINF:-1,Unknown Channel"
    return channels

# ============================================================
# DATA COLLECTION (MERGING ALL SOURCES)
# ============================================================
def collect_all_playlists() -> dict[str, list[dict[str, Any]]]:
    playlists = {}

    def add_to_playlist(fname, chs):
        if fname not in playlists:
            playlists[fname] = []
        playlists[fname].extend(chs)

    # 1. Custom Online Playlists
    for filename, url in CUSTOM_ONLINE_PLAYLISTS.items():
        log.info("Fetching custom playlist: %s", filename)
        try:
            res = get_session().get(url, timeout=15)
            if res.status_code == 200:
                add_to_playlist(filename, parse_m3u_text(res.text))
        except Exception as e:
            log.error("Failed custom %s: %s", filename, e)

    # 2. Local Extra Categories (e.g. extra_adult.m3u)
    for local_file in os.listdir('.'):
        if local_file.startswith('extra_') and local_file.endswith('.m3u') and local_file != EXTRA_FILE_MYANMAR:
            cat_name = local_file.replace('extra_', '')
            log.info("Fetching local extra: %s", local_file)
            try:
                with open(local_file, 'r', encoding='utf-8', errors='ignore') as f:
                    add_to_playlist(cat_name, parse_m3u_text(f.read()))
            except Exception as e:
                log.error("Failed local %s: %s", local_file, e)

    # 3. GitHub API Folders
    for api_url in GITHUB_APIS:
        log.info("Fetching GitHub API: %s", api_url)
        try:
            res = get_session().get(api_url, timeout=15)
            files = res.json()
            if isinstance(files, list):
                for f_info in files:
                    if isinstance(f_info, dict) and f_info.get('name', '').lower().endswith(('.m3u', '.m3u8')):
                        dl_url = f_info['download_url']
                        fname = f_info['name']
                        try:
                            r = get_session().get(dl_url, timeout=15)
                            add_to_playlist(fname, parse_m3u_text(r.text))
                        except Exception:
                            pass
        except Exception as e:
            log.error("Failed API %s: %s", api_url, e)

    # 4. Merge extra_myanmar.m3u into Myanmar Playlist
    if os.path.exists(EXTRA_FILE_MYANMAR):
        log.info("Merging %s into Myanmar list...", EXTRA_FILE_MYANMAR)
        try:
            with open(EXTRA_FILE_MYANMAR, 'r', encoding='utf-8', errors='ignore') as f:
                mm_extras = parse_m3u_text(f.read())
                # Find if mm.m3u exists, if not create Myanmar.m3u
                mm_key = next((k for k in playlists.keys() if "mm" in k.lower() or "myanmar" in k.lower()), "Myanmar.m3u")
                add_to_playlist(mm_key, mm_extras)
        except Exception as e:
            log.error("Failed to merge %s: %s", EXTRA_FILE_MYANMAR, e)

    return playlists

# ============================================================
# EXPERT'S LINK CHECKING & SAVING LOGIC
# ============================================================
def check_link(url: str) -> bool:
    try:
        session = get_session()
        resp = session.get(url, timeout=TIMEOUT, stream=True, allow_redirects=True)
        return resp.status_code == 200
    except Exception:
        return False

def check_all_unique_links(playlists: dict[str, list[dict[str, Any]]]) -> dict[str, bool]:
    representatives = {}
    for channels in playlists.values():
        for channel in channels:
            key = _url_key(channel["url"])
            representatives.setdefault(key, channel["url"])

    total = len(representatives)
    log.info("Checking %d unique HTTP(S) link(s) with %d worker(s)...", total, MAX_THREADS)
    results = {}
    if not total:
        return results

    completed = 0
    with ThreadPoolExecutor(max_workers=MAX_THREADS) as pool:
        future_to_key = {
            pool.submit(check_link, url): key
            for key, url in representatives.items()
        }
        for future in as_completed(future_to_key):
            key = future_to_key[future]
            try:
                results[key] = bool(future.result())
            except Exception as exc:
                log.debug("Link check raised an error: %s", exc)
                results[key] = False
            completed += 1
            if completed % 50 == 0 or completed == total:
                log.info("Link checks: %d/%d complete", completed, total)

    active_count = sum(results.values())
    log.info("Link check complete: %d active / %d total.", active_count, total)
    return results

def write_playlist(output_path: Path, channels: list[dict[str, Any]]) -> None:
    """Write atomically to avoid leaving a partially-written playlist."""
    temporary_path = output_path.with_suffix(output_path.suffix + ".tmp")
    with temporary_path.open("w", encoding="utf-8", newline="\n") as output:
        output.write("#EXTM3U\n")
        for channel in channels:
            extinf = str(channel.get("extinf") or "#EXTINF:-1,Unknown Channel").strip()
            output.write(extinf + "\n")
            output.write(str(channel["url"]).strip() + "\n")
    os.replace(temporary_path, output_path)

def save_active_playlists(playlists: dict[str, list[dict[str, Any]]], active_by_url: dict[str, bool]) -> None:
    OUTPUT_FOLDER.mkdir(parents=True, exist_ok=True)
    total_saved = 0

    for filename, channels in playlists.items():
        active_channels = [
            channel for channel in channels
            if active_by_url.get(_url_key(channel["url"]), False)
        ]
        output_name = get_country_name(filename)
        output_path = OUTPUT_FOLDER / output_name

        if not active_channels:
            try:
                output_path.unlink(missing_ok=True)
            except OSError as exc:
                pass
            continue

        try:
            write_playlist(output_path, active_channels)
            total_saved += len(active_channels)
            log.info("Saved %-32s active=%d / source=%d", str(output_path), len(active_channels), len(channels))
        except OSError as exc:
            log.error("Could not write %s: %s", output_path, exc)
            try:
                output_path.with_suffix(output_path.suffix + ".tmp").unlink(missing_ok=True)
            except OSError:
                pass

    log.info("Finished. Saved %d active channel entry/entries across output playlists.", total_saved)

def main() -> int:
    log.info("=" * 68)
    log.info("SAHARA IPTV - SUPER M3U PLAYLIST COLLECTOR + LINK CHECKER")
    log.info("Workers: %d | Output: %s", MAX_THREADS, OUTPUT_FOLDER)

    playlists = collect_all_playlists()
    if not playlists:
        log.error("No playlists could be loaded. Check your network and source URLs.")
        return 1

    channel_count = sum(len(channels) for channels in playlists.values())
    log.info("Collected %d URL entries across %d output categories.", channel_count, len(playlists))

    active_by_url = check_all_unique_links(playlists)
    save_active_playlists(playlists, active_by_url)
    return 0

if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        log.warning("Stopped by user.")
        raise SystemExit(130)
