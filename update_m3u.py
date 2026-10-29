#!/usr/bin/env python3
"""SAHARA IPTV Playlist Builder v3 (update_m3u.py)

Collects M3U/M3U8 playlists from configured public GitHub repositories,
configured playlist URLs, and local extra_*.m3u files; optionally checks
HTTP(S) endpoints; then writes merged playlists to an output directory.

Install:
    python -m pip install -r requirements.txt

Run:
    python update_m3u.py
    python update_m3u.py --workers 32 --timeout 8
    python update_m3u.py --skip-link-check

Optional GitHub API token (helps with GitHub API rate limits):
    Windows PowerShell:  $env:GITHUB_TOKEN="YOUR_TOKEN"
    Linux/macOS:         export GITHUB_TOKEN="YOUR_TOKEN"

Only use playlist sources and streams you are authorized to access.
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import re
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Iterable
from urllib.parse import quote, unquote, urlsplit, urlunsplit

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

try:
    import pycountry
except ImportError:  # Country-name conversion is optional.
    pycountry = None  # type: ignore[assignment]


# ---------------------------------------------------------------------------
# DEFAULT SETTINGS
# ---------------------------------------------------------------------------
APP_NAME = "SAHARA IPTV Playlist Builder"
APP_VERSION = "3.0.0"
DEFAULT_TIMEOUT = float(os.getenv("IPTV_TIMEOUT", "8"))
DEFAULT_WORKERS = max(1, min(128, int(os.getenv("IPTV_WORKERS", "40"))))
DEFAULT_OUTPUT = Path(os.getenv("IPTV_OUTPUT_DIR", "active_m3u"))
DEFAULT_MAX_GITHUB_FILES = 2500
MAX_PLAYLIST_BYTES = 32 * 1024 * 1024
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

CUSTOM_PLAYLIST_URLS = {
    "Migu_Playlist.m3u": "https://migu.yang-1989.xyz/playlist.m3u",
    "Waastudios.m3u": "http://events.waastudios.dpdns.org:8877/sub/wDIWWCqpBc4hvIBFOXCX8ZrhPEOPCyjV/playlist.m3u",
    "MyIPTV.m3u": "https://github.chenc.dev/raw.githubusercontent.com/CKL1211/eric/refs/heads/master/MyIPTV.m3u",
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("sahara_iptv_builder")
_THREAD_LOCAL = threading.local()


@dataclass(frozen=True)
class LinkCheckResult:
    active: bool
    status_code: int | None = None
    error: str | None = None


@dataclass
class RunStats:
    source_files_found: int = 0
    playlist_categories: int = 0
    channel_entries_collected: int = 0
    unique_urls_checked: int = 0
    active_urls: int = 0
    inactive_urls: int = 0
    channel_entries_saved: int = 0
    source_errors: int = 0


# ---------------------------------------------------------------------------
# HTTP SESSION / NETWORK HELPERS
# ---------------------------------------------------------------------------
def get_session() -> requests.Session:
    """Return a retry-enabled Session dedicated to the current worker thread."""
    session = getattr(_THREAD_LOCAL, "session", None)
    if session is None:
        session = requests.Session()
        session.headers.update({
            "User-Agent": USER_AGENT,
            "Accept": "*/*",
            "Connection": "keep-alive",
        })
        retries = Retry(
            total=2,
            connect=2,
            read=1,
            status=2,
            backoff_factor=0.4,
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=frozenset({"GET", "HEAD"}),
            respect_retry_after_header=True,
            raise_on_status=False,
        )
        adapter = HTTPAdapter(
            max_retries=retries,
            pool_connections=8,
            pool_maxsize=8,
        )
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        _THREAD_LOCAL.session = session
    return session


def _github_headers() -> dict[str, str]:
    """Keep GitHub credentials scoped to GitHub API requests only."""
    headers = {"Accept": "application/vnd.github+json"}
    token = os.getenv("GITHUB_TOKEN", "").strip()
    if token:
        headers["Authorization"] = f"Bearer {token}"
    return headers


def fetch_text(url: str, timeout: float, max_bytes: int = MAX_PLAYLIST_BYTES) -> str:
    """Fetch text with a size limit and guaranteed response cleanup."""
    with get_session().get(url, timeout=timeout, stream=True, allow_redirects=True) as response:
        response.raise_for_status()
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
        timeout=timeout,
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
        return parsed.scheme.lower() in {"http", "https"} and bool(parsed.hostname)
    except ValueError:
        return False


def normalize_url(url: str) -> str:
    """Canonicalize host/scheme without lower-casing case-sensitive paths."""
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
    """Convert country-code playlist names (e.g. us_news.m3u) to readable names."""
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
    return result or "Unknown.m3u"


def parse_m3u_text(content: str) -> list[dict[str, Any]]:
    """Parse M3U entries while preserving per-channel directives and metadata."""
    channels: list[dict[str, Any]] = []
    pending: list[str] = []

    for raw_line in content.lstrip("\ufeff").splitlines():
        line = raw_line.strip()
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
    """Return one representative URL per normalized URL key."""
    representatives: dict[str, str] = {}
    for channels in playlists.values():
        for channel in channels:
            url = str(channel.get("url", "")).strip()
            if is_http_url(url):
                representatives.setdefault(url_key(url), url)
    return representatives


# ---------------------------------------------------------------------------
# GITHUB PLAYLIST DISCOVERY
# ---------------------------------------------------------------------------
def parse_github_contents_url(api_url: str) -> tuple[str, str, str] | None:
    parsed = urlsplit(api_url)
    match = re.match(r"^/repos/([^/]+)/([^/]+)/contents(?:/(.*))?$", parsed.path)
    if not match:
        return None
    owner, repo, path = match.groups()
    return owner, repo, unquote(path or "").strip("/")


def discover_github_playlists(api_url: str, timeout: float, max_files: int) -> list[dict[str, str]]:
    """Use Git Trees API to find playlist files in nested directories."""
    parsed = parse_github_contents_url(api_url)
    if not parsed:
        log.warning("Skipping unsupported GitHub API URL: %s", api_url)
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
        return []

    prefix = requested_path.rstrip("/")
    found: list[dict[str, str]] = []
    for entry in tree:
        if not isinstance(entry, dict) or entry.get("type") != "blob":
            continue
        path = str(entry.get("path", ""))
        if prefix and not (path == prefix or path.startswith(prefix + "/")):
            continue
        if not path.lower().endswith(PLAYLIST_EXTENSIONS):
            continue
        raw_path = quote(path, safe="/")
        raw_url = f"https://raw.githubusercontent.com/{owner}/{repo}/{branch}/{raw_path}"
        found.append({"filename": Path(path).name, "url": raw_url, "repo_path": path})
        if len(found) >= max_files:
            log.warning("Capped %s at %d playlist files", f"{owner}/{repo}", max_files)
            break

    if tree_data.get("truncated"):
        log.warning("GitHub tree for %s/%s was truncated by GitHub", owner, repo)
    log.info("GitHub discovery: %s/%s -> %d playlist file(s)", owner, repo, len(found))
    return found


def download_source_playlist(item: dict[str, str], timeout: float) -> tuple[str, list[dict[str, Any]], str | None]:
    filename = item["filename"]
    url = item["url"]
    try:
        content = fetch_text(url, timeout=timeout)
        channels = parse_m3u_text(content)
        if not channels:
            return filename, [], "no valid HTTP(S) channel entries found"
        return filename, channels, None
    except Exception as exc:
        return filename, [], f"{type(exc).__name__}: {exc}"


# ---------------------------------------------------------------------------
# COLLECTION / LOCAL EXTRAS
# ---------------------------------------------------------------------------
def add_channels(
    playlists: dict[str, list[dict[str, Any]]],
    filename: str,
    channels: Iterable[dict[str, Any]],
) -> None:
    playlists.setdefault(Path(filename).name, []).extend(channels)


def collect_all_playlists(args: argparse.Namespace, stats: RunStats) -> dict[str, list[dict[str, Any]]]:
    playlists: dict[str, list[dict[str, Any]]] = {}
    source_jobs: list[dict[str, str]] = []
    errors = 0

    if not args.no_custom:
        for filename, url in CUSTOM_PLAYLIST_URLS.items():
            source_jobs.append({"filename": filename, "url": url, "source": "custom"})

    if not args.no_github:
        with ThreadPoolExecutor(max_workers=min(8, args.workers)) as pool:
            future_map = {
                pool.submit(discover_github_playlists, url, args.timeout, args.max_github_files): url
                for url in GITHUB_CONTENTS_URLS
            }
            for future in as_completed(future_map):
                api_url = future_map[future]
                try:
                    items = future.result()
                    for item in items:
                        source_jobs.append({
                            "filename": item["filename"],
                            "url": item["url"],
                            "source": "github",
                        })
                except Exception as exc:
                    errors += 1
                    log.warning("GitHub source failed (%s): %s", api_url, exc)

    if source_jobs:
        log.info("Downloading %d remote playlist source(s)...", len(source_jobs))
        with ThreadPoolExecutor(max_workers=args.workers) as pool:
            future_map = {
                pool.submit(download_source_playlist, item, args.timeout): item
                for item in source_jobs
            }
            completed = 0
            for future in as_completed(future_map):
                item = future_map[future]
                completed += 1
                try:
                    filename, channels, error = future.result()
                except Exception as exc:
                    filename, channels, error = item["filename"], [], str(exc)
                if error:
                    errors += 1
                    log.debug("Source failed (%s): %s", item["url"], error)
                elif channels:
                    add_channels(playlists, filename, channels)
                if completed % 25 == 0 or completed == len(source_jobs):
                    log.info("Remote playlist downloads: %d/%d", completed, len(source_jobs))

    if not args.no_local_extras:
        extras_dir = args.extras_dir.resolve()
        if extras_dir.exists():
            for local_file in sorted(extras_dir.glob("extra_*.m3u*")):
                if local_file.name.casefold() == EXTRA_MYANMAR_FILE.casefold():
                    continue
                category_name = local_file.name[len("extra_"):]
                try:
                    text = local_file.read_text(encoding="utf-8-sig", errors="replace")
                    channels = parse_m3u_text(text)
                    if channels:
                        add_channels(playlists, category_name, channels)
                        log.info("Loaded local extra: %s (%d channels)", local_file.name, len(channels))
                except OSError as exc:
                    errors += 1
                    log.warning("Could not read local extra %s: %s", local_file, exc)

        myanmar_file = extras_dir / EXTRA_MYANMAR_FILE
        if myanmar_file.is_file():
            try:
                mm_channels = parse_m3u_text(
                    myanmar_file.read_text(encoding="utf-8-sig", errors="replace")
                )
                target = next(
                    (name for name in playlists if is_myanmar_playlist_name(name)),
                    "Myanmar.m3u",
                )
                add_channels(playlists, target, mm_channels)
                log.info("Merged %d Myanmar extra channel(s) into %s", len(mm_channels), target)
            except OSError as exc:
                errors += 1
                log.warning("Could not read %s: %s", myanmar_file, exc)

    stats.source_files_found = len(source_jobs)
    stats.source_errors = errors
    return playlists


def is_myanmar_playlist_name(filename: str) -> bool:
    stem = Path(filename).stem.casefold()
    return stem == "mm" or stem == "myanmar" or stem.startswith("mm_") or stem.startswith("myanmar_")


# ---------------------------------------------------------------------------
# LINK VALIDATION
# ---------------------------------------------------------------------------
def check_link(url: str, timeout: float) -> LinkCheckResult:
    """Check endpoint response quickly; close streamed responses immediately."""
    try:
        with get_session().get(
            url,
            headers={"Range": "bytes=0-0"},
            timeout=(min(timeout, 5.0), timeout),
            stream=True,
            allow_redirects=True,
        ) as response:
            code = response.status_code
            return LinkCheckResult(active=(200 <= code < 300), status_code=code)
    except requests.RequestException as exc:
        return LinkCheckResult(active=False, error=f"{type(exc).__name__}: {exc}")
    except Exception as exc:
        return LinkCheckResult(active=False, error=f"{type(exc).__name__}: {exc}")


def check_all_unique_links(
    playlists: dict[str, list[dict[str, Any]]],
    workers: int,
    timeout: float,
    skip_check: bool,
) -> dict[str, LinkCheckResult]:
    representatives = channel_urls(playlists)
    total = len(representatives)
    if skip_check:
        log.warning("Link validation skipped; all collected URLs will be included.")
        return {key: LinkCheckResult(active=True) for key in representatives}

    log.info("Checking %d unique HTTP(S) URL(s) with %d worker(s)...", total, workers)
    if not total:
        return {}

    results: dict[str, LinkCheckResult] = {}
    with ThreadPoolExecutor(max_workers=workers) as pool:
        future_map = {
            pool.submit(check_link, url, timeout): key
            for key, url in representatives.items()
        }
        for completed, future in enumerate(as_completed(future_map), start=1):
            key = future_map[future]
            try:
                results[key] = future.result()
            except Exception as exc:
                results[key] = LinkCheckResult(active=False, error=str(exc))
            if completed % 50 == 0 or completed == total:
                log.info("Link checks: %d/%d complete", completed, total)

    active_count = sum(1 for item in results.values() if item.active)
    log.info("Link check result: %d reachable / %d checked", active_count, total)
    return results


# ---------------------------------------------------------------------------
# PLAYLIST SAVING / REPORTING
# ---------------------------------------------------------------------------
def write_playlist(output_path: Path, channels: list[dict[str, Any]]) -> None:
    """Write atomically; preserve all known per-channel metadata lines."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path = output_path.with_suffix(output_path.suffix + ".tmp")
    try:
        with temporary_path.open("w", encoding="utf-8", newline="\n") as output:
            output.write("#EXTM3U\n")
            for channel in channels:
                metadata = channel.get("metadata")
                if not isinstance(metadata, list) or not metadata:
                    metadata = [str(channel.get("extinf") or "#EXTINF:-1,Unknown Channel")]
                for item in metadata:
                    value = str(item).strip()
                    if value and value.upper() != "#EXTM3U":
                        output.write(value + "\n")
                output.write(str(channel.get("url", "")).strip() + "\n")
        os.replace(temporary_path, output_path)
    finally:
        try:
            temporary_path.unlink(missing_ok=True)
        except OSError:
            pass


def save_active_playlists(
    playlists: dict[str, list[dict[str, Any]]],
    checks: dict[str, LinkCheckResult],
    output_folder: Path,
) -> tuple[int, int]:
    """Group filename aliases, remove duplicate URLs, and save active entries."""
    grouped: dict[str, list[dict[str, Any]]] = {}
    source_counts: dict[str, int] = {}

    for filename, channels in playlists.items():
        output_name = get_country_name(filename)
        grouped.setdefault(output_name, [])
        source_counts[output_name] = source_counts.get(output_name, 0) + len(channels)
        grouped[output_name].extend(channels)

    total_saved = 0
    files_saved = 0
    output_folder.mkdir(parents=True, exist_ok=True)

    for output_name, channels in sorted(grouped.items()):
        active_channels: list[dict[str, Any]] = []
        seen: set[str] = set()
        for channel in channels:
            url = str(channel.get("url", "")).strip()
            key = url_key(url)
            result = checks.get(key, LinkCheckResult(active=False))
            if not result.active or key in seen:
                continue
            seen.add(key)
            active_channels.append(channel)

        output_path = output_folder / output_name
        if not active_channels:
            try:
                output_path.unlink(missing_ok=True)
            except OSError as exc:
                log.warning("Could not remove empty playlist %s: %s", output_path, exc)
            log.info("No reachable channels for %-32s source entries=%d", output_name, source_counts[output_name])
            continue

        try:
            write_playlist(output_path, active_channels)
            total_saved += len(active_channels)
            files_saved += 1
            log.info(
                "Saved %-32s active=%d / source=%d",
                output_name,
                len(active_channels),
                source_counts[output_name],
            )
        except OSError as exc:
            log.error("Could not write %s: %s", output_path, exc)

    return total_saved, files_saved


def write_json_report(output_folder: Path, stats: RunStats, checks: dict[str, LinkCheckResult]) -> None:
    try:
        output_folder.mkdir(parents=True, exist_ok=True)
        report = {
            "application": APP_NAME,
            "version": APP_VERSION,
            "stats": asdict(stats),
            "link_checks": {
                "reachable": sum(1 for result in checks.values() if result.active),
                "unreachable": sum(1 for result in checks.values() if not result.active),
            },
            "note": "HTTP reachability does not guarantee that a stream will play in every IPTV app or region.",
        }
        temp_path = output_folder / "report.json.tmp"
        final_path = output_folder / "report.json"
        temp_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
        os.replace(temp_path, final_path)
    except OSError as exc:
        log.warning("Could not write JSON report: %s", exc)


# ---------------------------------------------------------------------------
# CLI / MAIN
# ---------------------------------------------------------------------------
def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=f"{APP_NAME} v{APP_VERSION} - collect, validate, merge, and save M3U playlists."
    )
    parser.add_argument("--workers", type=int, default=DEFAULT_WORKERS, help=f"Concurrent workers (default: {DEFAULT_WORKERS}, max: 128)")
    parser.add_argument("--timeout", type=float, default=DEFAULT_TIMEOUT, help=f"Network timeout in seconds (default: {DEFAULT_TIMEOUT:g})")
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT, help=f"Output directory (default: {DEFAULT_OUTPUT})")
    parser.add_argument("--extras-dir", type=Path, default=Path.cwd(), help="Directory containing extra_*.m3u files (default: current directory)")
    parser.add_argument("--max-github-files", type=int, default=DEFAULT_MAX_GITHUB_FILES, help=f"Maximum playlist files per GitHub repository (default: {DEFAULT_MAX_GITHUB_FILES})")
    parser.add_argument("--skip-link-check", action="store_true", help="Skip URL validation and keep every collected HTTP(S) URL")
    parser.add_argument("--no-github", action="store_true", help="Do not collect GitHub repository playlists")
    parser.add_argument("--no-custom", action="store_true", help="Do not fetch configured custom playlist URLs")
    parser.add_argument("--no-local-extras", action="store_true", help="Do not load local extra_*.m3u files")
    parser.add_argument("--log-level", choices=("DEBUG", "INFO", "WARNING", "ERROR"), default="INFO", help="Logging verbosity")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    args.workers = max(1, min(128, args.workers))
    args.timeout = max(1.0, min(120.0, args.timeout))
    args.max_github_files = max(1, min(20_000, args.max_github_files))
    logging.getLogger().setLevel(getattr(logging, args.log_level))

    log.info("=" * 76)
    log.info("%s v%s", APP_NAME, APP_VERSION)
    log.info("Workers: %d | Timeout: %.1fs | Output: %s", args.workers, args.timeout, args.output)
    if os.getenv("GITHUB_TOKEN"):
        log.info("GitHub token detected; authenticated API limits may apply")

    stats = RunStats()
    playlists = collect_all_playlists(args, stats)
    if not playlists:
        log.error("No playlists were loaded. Check source URLs, internet access, and GitHub API limits.")
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
    log.info(
        "Collected %d channel entries across %d source categories",
        stats.channel_entries_collected,
        stats.playlist_categories,
    )
    if not playlists:
        log.error("No valid HTTP(S) channel URLs remained after parsing.")
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
    stats.channel_entries_saved, files_saved = save_active_playlists(playlists, checks, args.output)
    write_json_report(args.output, stats, checks)

    log.info("=" * 76)
    log.info("DONE | playlists saved: %d | channel entries saved: %d", files_saved, stats.channel_entries_saved)
    log.info("URL results: %d reachable / %d unreachable / %d unique", stats.active_urls, stats.inactive_urls, stats.unique_urls_checked)
    log.info("Source errors: %d | Report: %s", stats.source_errors, args.output / "report.json")
    log.info("Note: an HTTP-reachable URL may still fail to play in a specific app or region.")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        log.warning("Interrupted by user.")
        raise SystemExit(130)
