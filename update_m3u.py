#!/usr/bin/env python3
"""SAHARA IPTV Playlist Builder v4.1 (Fast Non-Blocking Edition).

Collect M3U/M3U8 playlists from configured public GitHub repositories,
configured remote playlist URLs, and local extra_*.m3u files. Optionally
check HTTP(S) stream endpoints, de-duplicate entries, validate generated
playlists, and publish the complete output directory atomically.
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import re
import shutil
import tempfile
import threading
import time
from collections import deque
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Iterator
from urllib.parse import quote, unquote, urlsplit, urlunsplit

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

try:
    import pycountry
except ImportError:
    pycountry = None  # type: ignore[assignment]


# ---------------------------------------------------------------------------
# CONFIGURATION
# ---------------------------------------------------------------------------
APP_NAME = "SAHARA IPTV Playlist Builder"
APP_VERSION = "4.1.0"


def _env_int(name: str, default: int, minimum: int, maximum: int) -> int:
    try:
        value = int(os.getenv(name, str(default)).strip())
    except (TypeError, ValueError):
        value = default
    return max(minimum, min(maximum, value))


def _env_float(name: str, default: float, minimum: float, maximum: float) -> float:
    try:
        value = float(os.getenv(name, str(default)).strip())
    except (TypeError, ValueError):
        value = default
    if value != value:
        value = default
    return max(minimum, min(maximum, value))


DEFAULT_TIMEOUT = _env_float("IPTV_TIMEOUT", 5.0, 1.0, 120.0)
DEFAULT_WORKERS = _env_int("IPTV_WORKERS", 64, 1, 128)
DEFAULT_OUTPUT = Path(os.getenv("IPTV_OUTPUT_DIR", "active_m3u").strip() or "active_m3u")
DEFAULT_MAX_GITHUB_FILES = 2500
DEFAULT_MAX_PLAYLIST_BYTES = 32 * 1024 * 1024
DEFAULT_RETRIES = 2
MAX_WORKERS = 128
USER_AGENT = f"SAHARA-IPTV-Playlist-Builder/{APP_VERSION}"
EXTRA_MYANMAR_FILE = "extra_myanmar.m3u"
PLAYLIST_EXTENSIONS = (".m3u", ".m3u8")

GITHUB_CONTENTS_URLS = [
    "https://api.github.com/repos/iptv-org/iptv/contents/streams",
    "https://api.github.com/repos/YueChan/Live/contents",
    "https://api.github.com/repos/qiangi/m3u/contents",
    "https://api.github.com/repos/youshandefeiyang/IPTV-URL-1/contents",
    "https://api.github.com/repos/youshandefeiyang/IPTV-URL/contents",
    "https://api.github.com/repos/mursor1985/LIVE/contents",
]

_DEFAULT_MIGU_URL = "https://migu.yang-1989.xyz/playlist.m3u"
_DEFAULT_WAASTUDIOS_URL = "http://events.waastudios.dpdns.org:8877/sub/wDIWWCqpBc4hvIBFOXCX8ZrhPEOPCyjV/playlist.m3u"
_DEFAULT_MYIPTV_URL = "https://github.chenc.dev/raw.githubusercontent.com/CKL1211/eric/refs/heads/master/MyIPTV.m3u"


def get_custom_playlist_urls() -> dict[str, str]:
    sources = {
        "Migu_Playlist.m3u": os.getenv("IPTV_MIGU_URL", _DEFAULT_MIGU_URL).strip(),
        "Waastudios.m3u": os.getenv("IPTV_WAASTUDIOS_URL", _DEFAULT_WAASTUDIOS_URL).strip(),
        "MyIPTV.m3u": os.getenv("IPTV_MYIPTV_URL", _DEFAULT_MYIPTV_URL).strip(),
    }
    return {name: url for name, url in sources.items() if is_http_url(url)}


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(message)s",
    datefmt="%H:%M:%S",
)
# Silence noisy urllib3 retry warnings in GitHub Actions logs
logging.getLogger("urllib3").setLevel(logging.ERROR)
log = logging.getLogger("sahara_iptv_builder")
_THREAD_LOCAL = threading.local()


# ---------------------------------------------------------------------------
# DATA MODELS
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class LinkCheckResult:
    active: bool
    status_code: int | None = None
    error: str | None = None
    checked: bool = True
    elapsed_ms: int = 0


@dataclass
class RunStats:
    github_repositories_checked: int = 0
    github_repositories_failed: int = 0
    source_files_discovered: int = 0
    source_files_downloaded: int = 0
    source_files_failed: int = 0
    local_extra_files_loaded: int = 0
    source_files_found: int = 0
    playlist_categories: int = 0
    channel_entries_collected: int = 0
    unique_urls_checked: int = 0
    active_urls: int = 0
    inactive_urls: int = 0
    channel_entries_saved: int = 0
    playlists_saved: int = 0
    source_errors: int = 0
    elapsed_seconds: float = 0.0


# ---------------------------------------------------------------------------
# LOGGING / NETWORK HELPERS
# ---------------------------------------------------------------------------
def safe_url_label(url: str) -> str:
    try:
        parsed = urlsplit(url)
        host = parsed.hostname or "unknown-host"
        if ":" in host and not host.startswith("["):
            host = f"[{host}]"
        port = parsed.port
        if port:
            host = f"{host}:{port}"
        scheme = parsed.scheme.lower() or "https"
        return f"{scheme}://{host}/…"
    except (TypeError, ValueError):
        return "[redacted URL]"


def safe_error(error: BaseException | str, limit: int = 500) -> str:
    value = str(error)
    value = re.sub(
        r"https?://[^\s\]\[\)\}\>'\"]+",
        lambda match: safe_url_label(match.group(0).rstrip(".,;")),
        value,
        flags=re.IGNORECASE,
    )
    return value[:limit]


def get_session() -> requests.Session:
    """Retry-enabled Session for downloading source M3U playlists and GitHub API."""
    session = getattr(_THREAD_LOCAL, "session", None)
    if session is None:
        session = requests.Session()
        session.headers.update({
            "User-Agent": USER_AGENT,
            "Accept": "*/*",
            "Connection": "keep-alive",
        })
        retries = Retry(
            total=DEFAULT_RETRIES,
            connect=DEFAULT_RETRIES,
            read=1,
            status=DEFAULT_RETRIES,
            backoff_factor=0.35,
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=frozenset({"GET", "HEAD"}),
            respect_retry_after_header=True,
            raise_on_status=False,
        )
        adapter = HTTPAdapter(
            max_retries=retries,
            pool_connections=8,
            pool_maxsize=8,
            pool_block=False,
        )
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        _THREAD_LOCAL.session = session
    return session


def get_check_session() -> requests.Session:
    """Fast fail-fast Session (0 connect/read retries) dedicated to stream link checking."""
    session = getattr(_THREAD_LOCAL, "check_session", None)
    if session is None:
        session = requests.Session()
        session.headers.update({
            "User-Agent": USER_AGENT,
            "Accept": "*/*",
            "Connection": "keep-alive",
        })
        # Do not retry dead stream servers; fail fast so 30,000+ links finish quickly.
        adapter = HTTPAdapter(
            max_retries=Retry(total=0, connect=0, read=0, redirect=2, status=0),
            pool_connections=16,
            pool_maxsize=16,
            pool_block=False,
        )
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        _THREAD_LOCAL.check_session = session
    return session


def _github_headers() -> dict[str, str]:
    headers = {"Accept": "application/vnd.github+json"}
    token = os.getenv("GITHUB_TOKEN", "").strip()
    if token:
        headers["Authorization"] = f"Bearer {token}"
    return headers


def fetch_text(url: str, timeout: float, max_bytes: int = DEFAULT_MAX_PLAYLIST_BYTES) -> str:
    with get_session().get(
        url,
        timeout=(min(timeout, 10.0), max(timeout, 15.0)),
        stream=True,
        allow_redirects=True,
    ) as response:
        response.raise_for_status()
        content_length = response.headers.get("Content-Length")
        if content_length:
            try:
                if int(content_length) > max_bytes:
                    raise ValueError(f"response exceeds {max_bytes // (1024 * 1024)} MiB limit")
            except ValueError as exc:
                if "response exceeds" in str(exc):
                    raise
        chunks: list[bytes] = []
        size = 0
        for chunk in response.iter_content(chunk_size=64 * 1024):
            if not chunk:
                continue
            size += len(chunk)
            if size > max_bytes:
                raise ValueError(f"response exceeds {max_bytes // (1024 * 1024)} MiB limit")
            chunks.append(chunk)
        return b"".join(chunks).decode("utf-8-sig", errors="replace")


def github_json(url: str, timeout: float) -> Any:
    with get_session().get(
        url,
        headers=_github_headers(),
        timeout=(min(timeout, 10.0), max(timeout, 15.0)),
        allow_redirects=True,
    ) as response:
        response.raise_for_status()
        return response.json()


# ---------------------------------------------------------------------------
# URL / COUNTRY / M3U HELPERS
# ---------------------------------------------------------------------------
def is_http_url(value: str) -> bool:
    try:
        parsed = urlsplit(value.strip())
        if parsed.scheme.lower() not in {"http", "https"} or not parsed.hostname:
            return False
        _ = parsed.port
        return not any(ord(char) < 32 for char in value)
    except (AttributeError, TypeError, ValueError):
        return False


def normalize_url(url: str) -> str:
    value = url.strip()
    try:
        parsed = urlsplit(value)
        scheme = parsed.scheme.lower()
        hostname = (parsed.hostname or "").lower()
        if not scheme or not hostname:
            return value
        try:
            port = parsed.port
        except ValueError:
            return value
        if ":" in hostname and not hostname.startswith("["):
            hostname = f"[{hostname}]"
        if port and not ((scheme == "http" and port == 80) or (scheme == "https" and port == 443)):
            netloc = f"{hostname}:{port}"
        else:
            netloc = hostname
        if parsed.username is not None:
            user_info = parsed.username
            if parsed.password is not None:
                user_info += f":{parsed.password}"
            netloc = f"{user_info}@{netloc}"
        return urlunsplit((scheme, netloc, parsed.path, parsed.query, ""))
    except (TypeError, ValueError):
        return value


def url_key(url: str) -> str:
    return normalize_url(url)


def get_country_name(filename: str) -> str:
    safe_name = Path(filename).name
    stem = Path(safe_name).stem
    parts = stem.split("_", 1)
    code = parts[0].lower()
    aliases = {
        "uk": "United_Kingdom",
        "us": "United_States",
        "kr": "South_Korea",
        "kp": "North_Korea",
        "ru": "Russia",
        "vn": "Vietnam",
        "la": "Laos",
        "sy": "Syria",
        "tz": "Tanzania",
        "bo": "Bolivia",
        "ve": "Venezuela",
        "md": "Moldova",
        "ir": "Iran",
        "bn": "Brunei",
        "cd": "DR_Congo",
        "cg": "Republic_of_the_Congo",
    }
    if code in aliases:
        country_name = aliases[code]
    elif pycountry is not None:
        country = pycountry.countries.get(alpha_2=code.upper())
        country_name = country.name.replace(" ", "_") if country else ""
    else:
        country_name = ""

    if country_name:
        suffix = f"_{parts[1]}" if len(parts) > 1 else ""
        result = f"{country_name}{suffix}.m3u"
    else:
        result = f"{stem}.m3u"

    result = re.sub(r"[^A-Za-z0-9._-]+", "_", result).strip("._")
    result = result[:180]
    return result or "Unknown.m3u"


def parse_m3u_text(content: str) -> list[dict[str, Any]]:
    channels: list[dict[str, Any]] = []
    pending: list[str] = []

    for raw_line in content.lstrip("\ufeff").splitlines():
        line = raw_line.strip().lstrip("\ufeff")
        if not line or line.upper() == "#EXTM3U":
            continue

        if is_http_url(line):
            metadata = list(pending)
            if not any(item.upper().startswith("#EXTINF") for item in metadata):
                metadata.insert(0, "#EXTINF:-1,Unknown Channel")
            extinf = next(
                (item for item in metadata if item.upper().startswith("#EXTINF")),
                "#EXTINF:-1,Unknown Channel",
            )
            channels.append({"extinf": extinf, "metadata": metadata, "url": line})
            pending.clear()
            continue

        if line.startswith("#"):
            if line.upper().startswith("#EXTINF") and any(
                item.upper().startswith("#EXTINF") for item in pending
            ):
                pending.clear()
            pending.append(line)

    return channels


def channel_urls(playlists: dict[str, list[dict[str, Any]]]) -> dict[str, str]:
    representatives: dict[str, str] = {}
    for channels in playlists.values():
        for channel in channels:
            url = str(channel.get("url", "")).strip()
            if is_http_url(url):
                representatives.setdefault(url_key(url), url)
    return representatives


def is_myanmar_playlist_name(filename: str) -> bool:
    stem = Path(filename).stem.casefold()
    return stem in {"mm", "myanmar"} or stem.startswith(("mm_", "myanmar_"))


# ---------------------------------------------------------------------------
# TRUE SLIDING-WINDOW CONCURRENCY (NO BATCH STALLS)
# ---------------------------------------------------------------------------
def completed_in_batches(
    executor: ThreadPoolExecutor,
    function: Any,
    items: list[Any],
    max_in_flight: int,
) -> Iterator[tuple[Any, Any]]:
    """Keep up to max_in_flight tasks running continuously without waiting for slow stragglers."""
    limit = max(1, max_in_flight)
    iterator = iter(items)
    future_map: dict[Any, Any] = {}

    for _ in range(limit):
        try:
            item = next(iterator)
        except StopIteration:
            break
        future_map[executor.submit(function, item)] = item

    while future_map:
        done, _ = wait(future_map.keys(), return_when=FIRST_COMPLETED)
        for future in done:
            item = future_map.pop(future)
            yield future, item
            try:
                next_item = next(iterator)
                future_map[executor.submit(function, next_item)] = next_item
            except StopIteration:
                pass


# ---------------------------------------------------------------------------
# GITHUB PLAYLIST DISCOVERY
# ---------------------------------------------------------------------------
def parse_github_contents_url(api_url: str) -> tuple[str, str, str] | None:
    try:
        parsed = urlsplit(api_url)
    except ValueError:
        return None
    match = re.match(r"^/repos/([^/]+)/([^/]+)/contents(?:/(.*))?$", parsed.path)
    if not match:
        return None
    owner, repo, path = match.groups()
    return owner, repo, unquote(path or "").strip("/")


def _discover_via_contents_api(
    owner: str,
    repo: str,
    branch: str,
    requested_path: str,
    timeout: float,
    max_files: int,
) -> list[dict[str, str]]:
    queue: deque[str] = deque([requested_path])
    visited: set[str] = set()
    found: list[dict[str, str]] = []
    max_directories = min(max(max_files, 100), 5000)

    while queue and len(found) < max_files and len(visited) < max_directories:
        current_path = queue.popleft()
        if current_path in visited:
            continue
        visited.add(current_path)
        encoded_path = quote(current_path, safe="/")
        endpoint = f"https://api.github.com/repos/{owner}/{repo}/contents/{encoded_path}?ref={quote(branch, safe='')}"
        payload = github_json(endpoint, timeout)
        if not isinstance(payload, list):
            continue

        for entry in payload:
            if not isinstance(entry, dict):
                continue
            entry_type = entry.get("type")
            path = str(entry.get("path", "")).strip("/")
            if entry_type == "dir":
                queue.append(path)
            elif entry_type == "file" and path.lower().endswith(PLAYLIST_EXTENSIONS):
                size = entry.get("size")
                if isinstance(size, int) and size > DEFAULT_MAX_PLAYLIST_BYTES:
                    continue
                raw_url = str(entry.get("download_url") or "").strip()
                if not is_http_url(raw_url):
                    raw_url = (
                        f"https://raw.githubusercontent.com/{owner}/{repo}/"
                        f"{quote(branch, safe='')}/{quote(path, safe='/')}"
                    )
                found.append({
                    "filename": Path(path).name,
                    "url": raw_url,
                    "repo_path": path,
                })
                if len(found) >= max_files:
                    break

    if queue:
        log.warning("Contents API fallback reached its directory/file safety cap for %s/%s", owner, repo)
    return found


def discover_github_playlists(
    api_url: str,
    timeout: float,
    max_files: int,
) -> list[dict[str, str]]:
    parsed = parse_github_contents_url(api_url)
    if not parsed:
        log.warning("Skipping unsupported GitHub API source: %s", safe_url_label(api_url))
        return []

    owner, repo, requested_path = parsed
    repo_api = f"https://api.github.com/repos/{owner}/{repo}"
    repo_info = github_json(repo_api, timeout)
    default_branch = str(repo_info.get("default_branch") or "main")
    branch = quote(default_branch, safe="")
    tree_url = f"https://api.github.com/repos/{owner}/{repo}/git/trees/{branch}?recursive=1"
    tree_data = github_json(tree_url, timeout)
    tree = tree_data.get("tree", []) if isinstance(tree_data, dict) else []
    if not isinstance(tree, list):
        raise ValueError("GitHub tree response did not contain a valid tree list")

    if isinstance(tree_data, dict) and tree_data.get("truncated"):
        log.warning("GitHub tree for %s/%s was truncated; using Contents API fallback", owner, repo)
        found = _discover_via_contents_api(
            owner, repo, default_branch, requested_path, timeout, max_files
        )
    else:
        prefix = requested_path.rstrip("/")
        found = []
        for entry in tree:
            if not isinstance(entry, dict) or entry.get("type") != "blob":
                continue
            path = str(entry.get("path", ""))
            if prefix and not (path == prefix or path.startswith(prefix + "/")):
                continue
            if not path.lower().endswith(PLAYLIST_EXTENSIONS):
                continue
            size = entry.get("size")
            if isinstance(size, int) and size > DEFAULT_MAX_PLAYLIST_BYTES:
                continue
            raw_path = quote(path, safe="/")
            raw_url = f"https://raw.githubusercontent.com/{owner}/{repo}/{branch}/{raw_path}"
            found.append({"filename": Path(path).name, "url": raw_url, "repo_path": path})
            if len(found) >= max_files:
                log.warning("Capped %s/%s at %d playlist files", owner, repo, max_files)
                break

    log.info("GitHub discovery: %s/%s -> %d playlist file(s)", owner, repo, len(found))
    return found


# ---------------------------------------------------------------------------
# SOURCE COLLECTION
# ---------------------------------------------------------------------------
def download_source_playlist(
    item: dict[str, str],
    timeout: float,
    max_bytes: int,
) -> tuple[str, list[dict[str, Any]], str | None]:
    filename = item["filename"]
    try:
        content = fetch_text(item["url"], timeout=timeout, max_bytes=max_bytes)
        channels = parse_m3u_text(content)
        if not channels:
            return filename, [], "no valid HTTP(S) channel entries found"
        return filename, channels, None
    except Exception as exc:
        return filename, [], safe_error(exc)


def add_channels(
    playlists: dict[str, list[dict[str, Any]]],
    filename: str,
    channels: Iterable[dict[str, Any]],
) -> None:
    safe_filename = Path(filename).name
    playlists.setdefault(safe_filename, []).extend(channels)


def collect_all_playlists(
    args: argparse.Namespace,
    stats: RunStats,
) -> tuple[dict[str, list[dict[str, Any]]], list[str]]:
    playlists: dict[str, list[dict[str, Any]]] = {}
    source_errors: list[str] = []
    source_jobs: list[dict[str, str]] = []

    if not args.no_custom:
        for filename, url in get_custom_playlist_urls().items():
            source_jobs.append({"filename": filename, "url": url, "source": "custom"})

    if not args.no_github:
        stats.github_repositories_checked = len(GITHUB_CONTENTS_URLS)
        with ThreadPoolExecutor(max_workers=min(8, args.workers)) as pool:
            for future, api_url in completed_in_batches(
                pool,
                lambda url: discover_github_playlists(url, args.timeout, args.max_github_files),
                GITHUB_CONTENTS_URLS,
                max_in_flight=min(8, args.workers) * 2,
            ):
                try:
                    items = future.result()
                    stats.source_files_discovered += len(items)
                    for item in items:
                        source_jobs.append({
                            "filename": item["filename"],
                            "url": item["url"],
                            "source": "github",
                        })
                except Exception as exc:
                    stats.github_repositories_failed += 1
                    source_errors.append(f"GitHub {safe_url_label(api_url)}: {safe_error(exc)}")
                    log.warning("GitHub repository discovery failed (%s): %s", safe_url_label(api_url), safe_error(exc))

    stats.source_files_found = len(source_jobs)
    if source_jobs:
        log.info("Downloading %d remote playlist source(s)...", len(source_jobs))
        with ThreadPoolExecutor(max_workers=args.workers) as pool:
            completed = 0
            for future, item in completed_in_batches(
                pool,
                lambda job: download_source_playlist(job, args.timeout, args.max_playlist_bytes),
                source_jobs,
                max_in_flight=max(args.workers * 2, args.workers),
            ):
                completed += 1
                try:
                    filename, channels, error = future.result()
                except Exception as exc:
                    filename, channels, error = item["filename"], [], safe_error(exc)
                if error:
                    stats.source_files_failed += 1
                    source_errors.append(f"{item['source']} {Path(filename).name}: {error}")
                    log.debug("Source failed (%s / %s): %s", item["source"], Path(filename).name, error)
                elif channels:
                    stats.source_files_downloaded += 1
                    add_channels(playlists, filename, channels)
                if completed % 50 == 0 or completed == len(source_jobs):
                    log.info("Remote playlist downloads: %d/%d", completed, len(source_jobs))

    if not args.no_local_extras:
        extras_dir = args.extras_dir.resolve()
        if extras_dir.exists() and extras_dir.is_dir():
            for local_file in sorted(extras_dir.glob("extra_*.m3u*")):
                if not local_file.is_file() or local_file.name.casefold() == EXTRA_MYANMAR_FILE.casefold():
                    continue
                category_name = local_file.name[len("extra_"):]
                try:
                    if local_file.stat().st_size > args.max_playlist_bytes:
                        raise ValueError("file exceeds configured playlist size limit")
                    text = local_file.read_text(encoding="utf-8-sig", errors="replace")
                    channels = parse_m3u_text(text)
                    if channels:
                        add_channels(playlists, category_name, channels)
                        stats.local_extra_files_loaded += 1
                        log.info("Loaded local extra: %s (%d channels)", local_file.name, len(channels))
                except (OSError, ValueError) as exc:
                    stats.source_files_failed += 1
                    source_errors.append(f"Local {local_file.name}: {safe_error(exc)}")
                    log.warning("Could not load local extra %s: %s", local_file.name, safe_error(exc))

            myanmar_file = extras_dir / EXTRA_MYANMAR_FILE
            if myanmar_file.is_file():
                try:
                    if myanmar_file.stat().st_size > args.max_playlist_bytes:
                        raise ValueError("file exceeds configured playlist size limit")
                    mm_channels = parse_m3u_text(
                        myanmar_file.read_text(encoding="utf-8-sig", errors="replace")
                    )
                    target = next(
                        (name for name in playlists if is_myanmar_playlist_name(name)),
                        "Myanmar.m3u",
                    )
                    if mm_channels:
                        add_channels(playlists, target, mm_channels)
                        stats.local_extra_files_loaded += 1
                        log.info("Merged %d Myanmar extra channel(s) into %s", len(mm_channels), target)
                except (OSError, ValueError) as exc:
                    stats.source_files_failed += 1
                    source_errors.append(f"Local {EXTRA_MYANMAR_FILE}: {safe_error(exc)}")
                    log.warning("Could not load %s: %s", myanmar_file.name, safe_error(exc))

    stats.source_errors = len(source_errors)
    return playlists, source_errors


# ---------------------------------------------------------------------------
# LINK VALIDATION (FAST FAIL-FAST SESSION)
# ---------------------------------------------------------------------------
def check_link(url: str, timeout: float) -> LinkCheckResult:
    """Check an HTTP(S) endpoint quickly without retrying dead stream hosts."""
    started = time.monotonic()
    connect_timeout = min(timeout, 4.0)
    read_timeout = min(timeout, 5.0)
    session = get_check_session()
    try:
        with session.get(
            url,
            headers={"Range": "bytes=0-0"},
            timeout=(connect_timeout, read_timeout),
            stream=True,
            allow_redirects=True,
        ) as response:
            code = response.status_code
            if code in {405, 416, 501}:
                with session.get(
                    url,
                    timeout=(connect_timeout, read_timeout),
                    stream=True,
                    allow_redirects=True,
                ) as fallback:
                    code = fallback.status_code
            elapsed = int((time.monotonic() - started) * 1000)
            return LinkCheckResult(
                active=200 <= code < 300,
                status_code=code,
                error=None if 200 <= code < 300 else f"HTTP {code}",
                checked=True,
                elapsed_ms=elapsed,
            )
    except requests.RequestException as exc:
        return LinkCheckResult(
            active=False,
            error=safe_error(exc),
            checked=True,
            elapsed_ms=int((time.monotonic() - started) * 1000),
        )
    except Exception as exc:
        return LinkCheckResult(
            active=False,
            error=safe_error(exc),
            checked=True,
            elapsed_ms=int((time.monotonic() - started) * 1000),
        )


def check_all_unique_links(
    playlists: dict[str, list[dict[str, Any]]],
    workers: int,
    timeout: float,
    skip_check: bool,
) -> dict[str, LinkCheckResult]:
    representatives = channel_urls(playlists)
    total = len(representatives)
    if skip_check:
        log.warning("Link validation skipped; all collected HTTP(S) URLs will be included.")
        return {
            key: LinkCheckResult(active=True, checked=False, error="link check skipped")
            for key in representatives
        }

    log.info("Checking %d unique HTTP(S) URL(s) with %d worker(s)...", total, workers)
    if not total:
        return {}

    results: dict[str, LinkCheckResult] = {}
    items = list(representatives.items())
    with ThreadPoolExecutor(max_workers=workers) as pool:
        for completed, (future, item) in enumerate(
            completed_in_batches(
                pool,
                lambda pair: check_link(pair[1], timeout),
                items,
                max_in_flight=max(workers * 3, workers),
            ),
            start=1,
        ):
            key, _url = item
            try:
                results[key] = future.result()
            except Exception as exc:
                results[key] = LinkCheckResult(active=False, error=safe_error(exc))
            if completed % 250 == 0 or completed == total:
                log.info("Link checks: %d/%d complete", completed, total)

    active_count = sum(1 for result in results.values() if result.active)
    log.info("Link check result: %d reachable / %d checked", active_count, total)
    return results


# ---------------------------------------------------------------------------
# OUTPUT GENERATION / VALIDATION / ATOMIC PUBLISH
# ---------------------------------------------------------------------------
def write_playlist(output_path: Path, channels: list[dict[str, Any]]) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path = output_path.with_suffix(output_path.suffix + ".tmp")
    try:
        with temporary_path.open("w", encoding="utf-8", newline="\n") as output:
            output.write("#EXTM3U\n")
            for channel in channels:
                url = str(channel.get("url", "")).strip()
                if not is_http_url(url):
                    continue
                metadata = channel.get("metadata")
                if not isinstance(metadata, list) or not metadata:
                    metadata = [str(channel.get("extinf") or "#EXTINF:-1,Unknown Channel")]
                for item in metadata:
                    value = str(item).strip().replace("\r", "").replace("\n", "")
                    if value and value.upper() != "#EXTM3U":
                        output.write(value + "\n")
                output.write(url + "\n")
        os.replace(temporary_path, output_path)
    finally:
        temporary_path.unlink(missing_ok=True)


def group_active_playlists(
    playlists: dict[str, list[dict[str, Any]]],
    checks: dict[str, LinkCheckResult],
) -> dict[str, list[dict[str, Any]]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    casefold_names: dict[str, str] = {}
    for filename, channels in playlists.items():
        output_name = get_country_name(filename)
        canonical = casefold_names.setdefault(output_name.casefold(), output_name)
        grouped.setdefault(canonical, []).extend(channels)

    output: dict[str, list[dict[str, Any]]] = {}
    for output_name, channels in sorted(grouped.items(), key=lambda pair: pair[0].casefold()):
        active_channels: list[dict[str, Any]] = []
        seen: set[str] = set()
        for channel in channels:
            url = str(channel.get("url", "")).strip()
            if not is_http_url(url):
                continue
            key = url_key(url)
            result = checks.get(key)
            if not result or not result.active or key in seen:
                continue
            seen.add(key)
            active_channels.append(channel)
        if active_channels:
            output[output_name] = active_channels
    return output


def validate_output_directory(output_folder: Path, expected_channels: int) -> tuple[int, int]:
    playlist_files = sorted(output_folder.glob("*.m3u")) + sorted(output_folder.glob("*.m3u8"))
    if not playlist_files:
        raise RuntimeError("no .m3u/.m3u8 playlists were generated")

    actual_channels = 0
    for path in playlist_files:
        content = path.read_text(encoding="utf-8-sig", errors="strict")
        lines = content.splitlines()
        if not lines or lines[0].strip().upper() != "#EXTM3U":
            raise RuntimeError(f"invalid M3U header in {path.name}")
        parsed = parse_m3u_text(content)
        if not parsed:
            raise RuntimeError(f"playlist contains no valid channel URLs: {path.name}")
        if any(not is_http_url(str(channel.get("url", ""))) for channel in parsed):
            raise RuntimeError(f"invalid URL found in {path.name}")
        actual_channels += len(parsed)

    if actual_channels != expected_channels:
        raise RuntimeError(
            f"output validation count mismatch: expected {expected_channels}, found {actual_channels}"
        )
    return len(playlist_files), actual_channels


def write_json_report(
    output_folder: Path,
    stats: RunStats,
    checks: dict[str, LinkCheckResult],
    source_errors: list[str],
) -> None:
    report = {
        "application": APP_NAME,
        "version": APP_VERSION,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "stats": asdict(stats),
        "link_checks": {
            "total": len(checks),
            "reachable": sum(1 for result in checks.values() if result.active),
            "unreachable": sum(1 for result in checks.values() if not result.active),
            "skipped": sum(1 for result in checks.values() if not result.checked),
            "average_response_ms": round(
                sum(result.elapsed_ms for result in checks.values() if result.checked)
                / max(1, sum(1 for result in checks.values() if result.checked)),
                2,
            ),
        },
        "source_errors": source_errors[:200],
        "source_errors_truncated": max(0, len(source_errors) - 200),
        "note": (
            "HTTP reachability does not guarantee that a stream will play in every IPTV app, "
            "device, network, or region."
        ),
    }
    temp_path = output_folder / "report.json.tmp"
    final_path = output_folder / "report.json"
    temp_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    os.replace(temp_path, final_path)


def _remove_path(path: Path) -> None:
    if path.is_symlink() or path.is_file():
        path.unlink(missing_ok=True)
    elif path.exists():
        shutil.rmtree(path)


def atomic_publish_directory(staging_dir: Path, output_dir: Path) -> None:
    output_dir.parent.mkdir(parents=True, exist_ok=True)
    backup_dir = output_dir.parent / f".{output_dir.name}.backup-{os.getpid()}-{time.time_ns()}"
    moved_old = False
    try:
        if output_dir.exists() or output_dir.is_symlink():
            os.replace(output_dir, backup_dir)
            moved_old = True
        os.replace(staging_dir, output_dir)
    except Exception:
        if moved_old and backup_dir.exists() and not output_dir.exists():
            os.replace(backup_dir, output_dir)
        raise
    else:
        if moved_old:
            _remove_path(backup_dir)


def save_active_playlists(
    playlists: dict[str, list[dict[str, Any]]],
    checks: dict[str, LinkCheckResult],
    output_folder: Path,
    stats: RunStats,
    source_errors: list[str],
) -> tuple[int, int]:
    grouped = group_active_playlists(playlists, checks)
    if not grouped:
        raise RuntimeError("no active playlist entries remain; existing output was preserved")

    output_folder = output_folder.expanduser().resolve()
    output_folder.parent.mkdir(parents=True, exist_ok=True)
    staging_dir = Path(tempfile.mkdtemp(prefix=f".{output_folder.name}.staging-", dir=output_folder.parent))
    try:
        expected_channels = 0
        for output_name, channels in grouped.items():
            write_playlist(staging_dir / output_name, channels)
            expected_channels += len(channels)

        stats.channel_entries_saved = expected_channels
        playlist_count, validated_channels = validate_output_directory(staging_dir, expected_channels)
        stats.playlists_saved = playlist_count
        write_json_report(staging_dir, stats, checks, source_errors)

        with (staging_dir / "report.json").open("r", encoding="utf-8") as report_file:
            json.load(report_file)

        atomic_publish_directory(staging_dir, output_folder)
        return playlist_count, validated_channels
    finally:
        if staging_dir.exists():
            _remove_path(staging_dir)


# ---------------------------------------------------------------------------
# CLI / MAIN
# ---------------------------------------------------------------------------
def _positive_int(value: str) -> int:
    number = int(value)
    if number < 1:
        raise argparse.ArgumentTypeError("must be at least 1")
    return number


def _positive_float(value: str) -> float:
    number = float(value)
    if number <= 0 or number != number:
        raise argparse.ArgumentTypeError("must be a positive number")
    return number


def _ratio(value: str) -> float:
    number = float(value)
    if not 0.0 <= number <= 1.0:
        raise argparse.ArgumentTypeError("must be between 0.0 and 1.0")
    return number


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=f"{APP_NAME} v{APP_VERSION} - collect, validate, merge, and safely publish M3U playlists."
    )
    parser.add_argument("--workers", type=_positive_int, default=DEFAULT_WORKERS,
                        help=f"Concurrent link/source workers (default: {DEFAULT_WORKERS}, max: {MAX_WORKERS})")
    parser.add_argument("--timeout", type=_positive_float, default=DEFAULT_TIMEOUT,
                        help=f"Network timeout in seconds (default: {DEFAULT_TIMEOUT:g}, max: 120)")
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT,
                        help=f"Output directory (default: {DEFAULT_OUTPUT})")
    parser.add_argument("--extras-dir", type=Path, default=Path.cwd(),
                        help="Directory containing extra_*.m3u files (default: current directory)")
    parser.add_argument("--max-github-files", type=_positive_int, default=DEFAULT_MAX_GITHUB_FILES,
                        help=f"Maximum playlist files per GitHub repository (default: {DEFAULT_MAX_GITHUB_FILES})")
    parser.add_argument("--max-playlist-bytes", type=_positive_int, default=DEFAULT_MAX_PLAYLIST_BYTES,
                        help="Maximum bytes accepted per downloaded/local playlist")
    parser.add_argument("--min-active-urls", type=int, default=1,
                        help="Minimum reachable URLs required before replacing existing output (default: 1)")
    parser.add_argument("--min-active-ratio", type=_ratio, default=0.0,
                        help="Minimum reachable/unique URL ratio (0.0..1.0; default: 0.0)")
    parser.add_argument("--skip-link-check", action="store_true",
                        help="Skip URL validation and keep every collected HTTP(S) URL")
    parser.add_argument("--no-github", action="store_true", help="Do not collect GitHub repository playlists")
    parser.add_argument("--no-custom", action="store_true", help="Do not fetch configured custom playlist URLs")
    parser.add_argument("--no-local-extras", action="store_true", help="Do not load local extra_*.m3u files")
    parser.add_argument("--log-level", choices=("DEBUG", "INFO", "WARNING", "ERROR"), default="INFO",
                        help="Logging verbosity")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.workers > MAX_WORKERS:
        parser.error(f"--workers cannot exceed {MAX_WORKERS}")
    if args.timeout > 120:
        parser.error("--timeout cannot exceed 120 seconds")
    if args.max_github_files > 20_000:
        parser.error("--max-github-files cannot exceed 20000")
    if args.max_playlist_bytes > 256 * 1024 * 1024:
        parser.error("--max-playlist-bytes cannot exceed 256 MiB")
    if args.min_active_urls < 1:
        parser.error("--min-active-urls must be at least 1")

    logging.getLogger().setLevel(getattr(logging, args.log_level))
    logging.getLogger("urllib3").setLevel(logging.ERROR)
    started = time.monotonic()
    stats = RunStats()

    log.info("=" * 78)
    log.info("%s v%s", APP_NAME, APP_VERSION)
    log.info("Workers: %d | Timeout: %.1fs | Output: %s", args.workers, args.timeout, args.output)
    log.info("Maximum playlist size: %.1f MiB | Link check skipped: %s",
             args.max_playlist_bytes / (1024 * 1024), args.skip_link_check)
    if os.getenv("GITHUB_TOKEN"):
        log.info("GitHub token detected; authenticated API rate limits may apply")

    try:
        playlists, source_errors = collect_all_playlists(args, stats)
        if not playlists:
            log.error("No playlists were loaded. Existing output was not changed.")
            return 1

        for filename in list(playlists):
            cleaned: list[dict[str, Any]] = []
            seen_in_category: set[str] = set()
            for channel in playlists[filename]:
                url = str(channel.get("url", "")).strip()
                if not is_http_url(url):
                    continue
                key = url_key(url)
                if key in seen_in_category:
                    continue
                seen_in_category.add(key)
                cleaned.append(channel)
            if cleaned:
                playlists[filename] = cleaned
            else:
                del playlists[filename]

        stats.playlist_categories = len(playlists)
        stats.channel_entries_collected = sum(len(channels) for channels in playlists.values())
        log.info("Collected %d channel entries across %d source categories",
                 stats.channel_entries_collected, stats.playlist_categories)
        if not playlists:
            log.error("No valid HTTP(S) channel URLs remained after parsing. Existing output was not changed.")
            return 1

        representatives = channel_urls(playlists)
        checks = check_all_unique_links(
            playlists,
            workers=args.workers,
            timeout=args.timeout,
            skip_check=args.skip_link_check,
        )
        stats.unique_urls_checked = len(representatives)
        stats.active_urls = sum(1 for item in checks.values() if item.active)
        stats.inactive_urls = sum(1 for item in checks.values() if not item.active)

        active_ratio = stats.active_urls / max(1, stats.unique_urls_checked)
        log.info("Reachability: %d/%d (%.1f%%)",
                 stats.active_urls, stats.unique_urls_checked, active_ratio * 100)
        if stats.active_urls < args.min_active_urls:
            log.error(
                "Safety gate: only %d active URL(s); at least %d required. Existing output was preserved.",
                stats.active_urls, args.min_active_urls,
            )
            return 2
        if active_ratio < args.min_active_ratio:
            log.error(
                "Safety gate: active ratio %.3f is below required %.3f. Existing output was preserved.",
                active_ratio, args.min_active_ratio,
            )
            return 2

        playlist_count, channel_count = save_active_playlists(
            playlists, checks, args.output, stats, source_errors
        )
        stats.elapsed_seconds = round(time.monotonic() - started, 2)

        report_path = args.output.expanduser().resolve() / "report.json"
        try:
            report = json.loads(report_path.read_text(encoding="utf-8"))
            report["stats"] = asdict(stats)
            report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
        except (OSError, ValueError) as exc:
            log.warning("Could not refresh final report timing: %s", safe_error(exc))

        log.info("=" * 78)
        log.info("SUCCESS | playlists saved: %d | channel entries saved: %d", playlist_count, channel_count)
        log.info("URLs: %d reachable / %d unreachable / %d unique",
                 stats.active_urls, stats.inactive_urls, stats.unique_urls_checked)
        log.info("Source errors: %d | Duration: %.2fs", stats.source_errors, stats.elapsed_seconds)
        log.info("Output published safely: %s", args.output)
        return 0

    except KeyboardInterrupt:
        log.warning("Interrupted by user; existing output was preserved unless publish had already completed.")
        return 130
    except Exception as exc:
        log.exception("Fatal updater error: %s", safe_error(exc))
        log.error("The previous output directory is preserved when generation fails before publish.")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
