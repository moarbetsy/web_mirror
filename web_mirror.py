#!/usr/bin/env python3
import argparse
import dbm
import hashlib
import json
import logging
import mimetypes
import os
import random
import re
import sys
import tempfile
import time
import xml.etree.ElementTree as ET
from collections import deque
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from http.cookiejar import MozillaCookieJar
from io import BytesIO
from pathlib import Path
from threading import Lock
from typing import Dict, Iterable, Iterator, List, Mapping, Optional, Set, Tuple, Union
from urllib import robotparser
from urllib.parse import parse_qsl, urlencode, urljoin, urlparse, urlunparse

import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# -------------------- Config --------------------

DEFAULT_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.7",
}

CSS_URL_RE = re.compile(r"url\(\s*([\"']?)([^)\"']+)\1\s*\)", re.IGNORECASE)
CSS_IMPORT_RE = re.compile(
    r"@import\s+(?:url\()?\s*([\"']?)([^\)\"';]+)\1\s*\)?\s*;",
    re.IGNORECASE,
)
SRCSET_SPLIT_RE = re.compile(r"\s*,\s*")
WS_RE = re.compile(r"\s+")
INVALID_FILENAME_CHARS_RE = re.compile(r'[<>:"/\\|?*]')

TRACKING_PARAM_PREFIXES = (
    "utm_",
    "gclid",
    "fbclid",
    "mc_",
    "yclid",
    "icid",
    "ref",
    "cmpid",
)
HTML_LIKE_EXTS = {".html", ".htm", ".php", ".asp", ".aspx", ".jsp", ".jspx", ".cfm"}

# Heuristic for regex‑based JS URL literal rewriting (fallback)
JS_STRING_URL_RE = re.compile(
    r'(?P<q>["\'])'
    r"(?P<u>("
    r'(?:https?:)?//[^"\']+'
    r'|/[^\s"\']+'
    r'|[^"\':\s?#][^"\']+\.(?:png|jpe?g|gif|webp|svg|ico|woff2?|ttf|otf|mp4|webm|mp3|ogg|wav|json|webmanifest|css|js|mjs)'
    r"))"
    r"(?P=q)"
)

# -------------------- Settings --------------------


@dataclass
class Settings:
    timeout: float = 15.0
    workers: int = 16
    delay: float = 0.0
    same_origin_only: bool = True
    respect_robots: bool = False
    max_bytes: int = 50_000_000
    skip_js: bool = False

    # Crawl
    crawl: bool = False
    max_pages: int = 100
    max_depth: int = 1
    include: Optional[str] = None
    exclude: Optional[str] = None
    strip_params: bool = True
    allow_params: Set[str] = field(default_factory=set)
    sitemap_seed: bool = False

    # Rendering
    render_js: bool = False
    render_timeout_ms: int = 10000
    wait_until: str = "networkidle"
    render_block_media: bool = True

    # Throttle
    global_rps: float = 2.0
    per_host_rps: float = 1.0
    burst: int = 2
    jitter: float = 0.3
    max_backoff: float = 60.0
    auto_throttle: bool = True

    # Checkpoint / resume
    resume: bool = False
    checkpoint_file: Optional[str] = None
    checkpoint_pages: int = 50
    checkpoint_seconds: float = 120.0
    checkpoint_include_seen_when_mem: bool = True

    # Persistent KV
    kv_backend: str = "none"  # none | dbm
    kv_dir: Optional[str] = None

    # Rewriting
    rewrite_js_urls: bool = False  # regex fallback
    rewrite_js_ast: bool = False  # AST‑based (esprima)

    # WARC
    warc: bool = False
    warc_path: Optional[str] = None
    warc_gzip: bool = True
    warc_append: bool = True

    # Auth / session
    cookies_file: Optional[str] = None
    extra_headers: List[str] = field(default_factory=list)  # "Name: value"
    auth_basic: Optional[str] = None  # "user:pass"
    auth_bearer: Optional[str] = None


# -------------------- Throttle --------------------


class TokenBucket:
    def __init__(self, rate: float, capacity: float):
        self.rate = max(rate, 0.001)
        self.capacity = max(capacity, 1.0)
        self.tokens = self.capacity
        self.ts = time.monotonic()
        self.lock = Lock()

    def _refill(self) -> None:
        now = time.monotonic()
        delta = now - self.ts
        if delta > 0:
            self.tokens = min(self.capacity, self.tokens + delta * self.rate)
            self.ts = now

    def consume_wait(self, tokens: float = 1.0) -> float:
        with self.lock:
            self._refill()
            if self.tokens >= tokens:
                self.tokens -= tokens
                return 0.0
            need = tokens - self.tokens
            wait = need / self.rate
            self.tokens = 0.0
            self.ts = time.monotonic()
            return max(0.0, wait)


def parse_retry_after(value: Optional[str]) -> Optional[float]:
    if not value:
        return None
    v = value.strip()
    if v.isdigit():
        return float(v)
    try:
        dt = parsedate_to_datetime(v)
        if not dt.tzinfo:
            dt = dt.replace(tzinfo=timezone.utc)
        now = datetime.now(tz=timezone.utc)
        return max(0.0, (dt - now).total_seconds())
    except Exception:
        return None


class Throttle:
    def __init__(self, settings: Settings):
        self.s = settings
        self.global_bucket = TokenBucket(settings.global_rps, settings.burst)
        self.host_buckets: Dict[str, TokenBucket] = {}
        self.host_block_until: Dict[str, float] = {}
        self.host_429_count: Dict[str, int] = {}
        self.lock = Lock()

    def _host_bucket(self, host: str) -> TokenBucket:
        with self.lock:
            b = self.host_buckets.get(host)
            if b is None:
                b = TokenBucket(self.s.per_host_rps, self.s.burst)
                self.host_buckets[host] = b
            return b

    def acquire(self, url: str) -> None:
        host = urlparse(url).netloc
        base = self.s.delay
        g = self.global_bucket.consume_wait(1.0)
        h = self._host_bucket(host).consume_wait(1.0)
        with self.lock:
            until = self.host_block_until.get(host, 0.0)
        now = time.monotonic()
        b = max(0.0, until - now)
        wait = max(base, g, h, b)
        if self.s.jitter > 0:
            wait += random.uniform(0, self.s.jitter * max(wait, base, 0.01))
        if wait > 0:
            time.sleep(wait)

    def on_result(self, url: str, status: int, headers: Mapping[str, str]) -> None:
        if not self.s.auto_throttle:
            return
        host = urlparse(url).netloc
        if status in (429, 503):
            ra = parse_retry_after(headers.get("Retry-After"))
            if ra is None:
                with self.lock:
                    n = self.host_429_count.get(host, 0) + 1
                    self.host_429_count[host] = n
                backoff = min(self.s.max_backoff, 2 ** min(n, 6))
            else:
                backoff = min(self.s.max_backoff, ra)
                with self.lock:
                    self.host_429_count[host] = max(
                        0, self.host_429_count.get(host, 0) - 1
                    )
            until = time.monotonic() + backoff + random.uniform(0, 0.1 * backoff)
            with self.lock:
                self.host_block_until[host] = max(
                    self.host_block_until.get(host, 0.0), until
                )
        elif 200 <= status < 300:
            with self.lock:
                self.host_429_count[host] = 0


# -------------------- Utils --------------------


def sanitize_filename(name: str) -> str:
    name = INVALID_FILENAME_CHARS_RE.sub("_", name)
    name = name or "file"
    if name.startswith("."):
        name = "_" + name[1:]
    return name[:200]


def can_fetch_url(u: str) -> bool:
    if not u:
        return False
    u = u.strip()
    if u.startswith(("#", "mailto:", "tel:", "javascript:", "data:", "blob:")):
        return False
    return True


def is_same_origin(base: str, other: str) -> bool:
    b, o = urlparse(base), urlparse(other)
    return (b.scheme, b.netloc) == (o.scheme, o.netloc)


def build_session(headers: Optional[Dict[str, str]] = None) -> requests.Session:
    s = requests.Session()
    retry = Retry(
        total=5,
        backoff_factor=0.5,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods={"GET", "HEAD"},
        raise_on_status=False,
        respect_retry_after_header=True,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=128, pool_maxsize=128)
    s.mount("http://", adapter)
    s.mount("https://", adapter)
    s.headers.update(DEFAULT_HEADERS if headers is None else headers)
    return s


def apply_auth_to_session(session: requests.Session, settings: Settings) -> None:
    for h in settings.extra_headers:
        if ":" not in h:
            logging.warning("invalid header (no colon): %s", h)
            continue
        k, v = h.split(":", 1)
        session.headers[k.strip()] = v.strip()
    if settings.auth_bearer:
        session.headers["Authorization"] = f"Bearer {settings.auth_bearer}"
    if settings.auth_basic:
        if ":" not in settings.auth_basic:
            logging.error("--auth-basic requires user:pass")
        else:
            u, p = settings.auth_basic.split(":", 1)
            session.auth = (u, p)
    if settings.cookies_file:
        try:
            jar = MozillaCookieJar()
            jar.load(settings.cookies_file, ignore_discard=True, ignore_expires=True)
            session.cookies.update(jar)
            logging.info("loaded cookies: %s", settings.cookies_file)
        except Exception as e:
            logging.error("failed to load cookies: %s", e)


def ensure_parent_dir(p: Path) -> None:
    p.parent.mkdir(parents=True, exist_ok=True)


def guess_ext_from_type(content_type: Optional[str]) -> Optional[str]:
    if not content_type:
        return None
    ct = content_type.split(";")[0].strip().lower()
    if ct in ("application/javascript", "text/javascript"):
        return ".js"
    if ct == "application/json":
        return ".json"
    if ct == "image/svg+xml":
        return ".svg"
    if ct == "font/woff2":
        return ".woff2"
    if ct == "font/woff":
        return ".woff"
    if ct == "application/manifest+json":
        return ".webmanifest"
    return mimetypes.guess_extension(ct)


def category_for(path: str, content_type: Optional[str]) -> str:
    ext = (os.path.splitext(path)[1] or "").lower()
    ct = (content_type or "").lower()
    if ct.startswith("image/") or ext in {
        ".png",
        ".jpg",
        ".jpeg",
        ".gif",
        ".webp",
        ".svg",
        ".ico",
        ".bmp",
        ".avif",
    }:
        return "img"
    if ct.startswith("text/css") or ext == ".css":
        return "css"
    if "javascript" in ct or ext in {".js", ".mjs"}:
        return "js"
    if ct.startswith("font/") or ext in {".woff", ".woff2", ".ttf", ".otf", ".eot"}:
        return "font"
    if (
        ct.startswith("audio/")
        or ct.startswith("video/")
        or ext in {".mp4", ".webm", ".mp3", ".ogg", ".wav", ".m4a", ".mkv"}
    ):
        return "media"
    if "json" in ct or ext in {".json", ".webmanifest", ".map"}:
        return "data"
    return "other"


def short_h(url: str) -> str:
    return hashlib.md5(url.encode("utf-8")).hexdigest()[:8]


# -------------------- HTML utils --------------------


def bs4_parse(html: str) -> BeautifulSoup:
    try:
        return BeautifulSoup(html, "lxml")
    except Exception:
        return BeautifulSoup(html, "html.parser")


def effective_base_url(soup: BeautifulSoup, fallback: str) -> str:
    try:
        tag = soup.find("base", href=True)
        if tag and tag.get("href"):
            return urljoin(fallback, tag["href"])
    except Exception:
        pass
    return fallback


def serialize_html(soup: BeautifulSoup) -> str:
    try:
        return soup.decode(formatter="html")
    except Exception:
        return str(soup)


# -------------------- Output layout --------------------


def site_root(base_origin: str, output_root: Path) -> Path:
    host = sanitize_filename(urlparse(base_origin).netloc) or "host"
    return output_root / host


def pages_dir(base_origin: str, output_root: Path) -> Path:
    return site_root(base_origin, output_root) / "pages"


def assets_dir(base_origin: str, output_root: Path) -> Path:
    return site_root(base_origin, output_root) / "assets"


def vendor_dir(base_origin: str, output_root: Path) -> Path:
    return assets_dir(base_origin, output_root) / "vendor"


def meta_dir(base_origin: str, output_root: Path) -> Path:
    return site_root(base_origin, output_root) / "meta"


def local_html_path_for_url(page_url: str, base_origin: str, output_root: Path) -> Path:
    root = pages_dir(base_origin, output_root)
    p = urlparse(page_url)
    path = p.path or "/"
    segs = [seg for seg in path.split("/") if seg]
    if path.endswith("/"):
        return root.joinpath(*segs, "index.html")
    last = segs[-1] if segs else ""
    _, ext = os.path.splitext(last)
    if ext.lower() in HTML_LIKE_EXTS:
        return root.joinpath(*segs)
    if ext:
        return root.joinpath(*segs, "index.html")
    return root.joinpath(*segs, "index.html")


def local_asset_path_for_url(
    asset_url: str, base_origin: str, output_root: Path, content_type: Optional[str]
) -> Path:
    au = urlparse(asset_url)
    base_host = urlparse(base_origin).netloc
    same = au.netloc == base_host
    ctype = content_type or ""
    cat = category_for(au.path, ctype)
    name = os.path.basename(au.path.rstrip("/")) or "file"
    base, ext = os.path.splitext(name)
    if not ext:
        ext_guess = guess_ext_from_type(ctype)
        if ext_guess:
            ext = ext_guess
    base = sanitize_filename(base)
    fname = f"{base}_{short_h(asset_url)}{ext or ''}"
    if same:
        return assets_dir(base_origin, output_root) / cat / fname
    else:
        host = sanitize_filename(au.netloc) or "host"
        return vendor_dir(base_origin, output_root) / host / cat / fname


# -------------------- URL normalize --------------------


def normalize_url(u: str, *, strip_params: bool, allow_params: Set[str]) -> str:
    p = urlparse(u)
    p = p._replace(fragment="")
    if not strip_params or not p.query:
        return urlunparse((p.scheme, p.netloc, p.path, p.params, p.query, ""))
    qs = parse_qsl(p.query, keep_blank_values=True)
    allow_lc = {k.lower() for k in allow_params}
    keep = []
    for k, v in qs:
        kl = k.lower()
        if kl in allow_lc:
            keep.append((k, v))
            continue
        drop = any(kl.startswith(pref) for pref in TRACKING_PARAM_PREFIXES)
        if not drop:
            keep.append((k, v))
    new_q = urlencode(keep, doseq=True)
    return urlunparse((p.scheme, p.netloc, p.path, p.params, new_q, ""))


def url_allowed(u: str, base_url: str, settings: Settings) -> bool:
    if settings.same_origin_only and not is_same_origin(base_url, u):
        return False
    if settings.include and not re.search(settings.include, u):
        return False
    if settings.exclude and re.search(settings.exclude, u):
        return False
    return True


def filter_allowed(urls: Iterable[str], base_url: str, settings: Settings) -> Set[str]:
    return {u for u in urls if url_allowed(u, base_url, settings)}


# -------------------- Extraction --------------------


def parse_srcset(v: str) -> List[str]:
    urls: List[str] = []
    if not v:
        return urls
    for cand in SRCSET_SPLIT_RE.split(v.strip()):
        if not cand:
            continue
        parts = WS_RE.split(cand.strip())
        if parts:
            urls.append(parts[0])
    return urls


def parse_css_urls(text: str) -> Set[str]:
    urls: Set[str] = set()
    for m in CSS_URL_RE.finditer(text):
        u = m.group(2).strip()
        if can_fetch_url(u):
            urls.add(u)
    for m in CSS_IMPORT_RE.finditer(text):
        u = m.group(2).strip()
        if can_fetch_url(u):
            urls.add(u)
    return urls


def extract_asset_urls(
    soup: BeautifulSoup, base_url: str, *, skip_js: bool
) -> Set[str]:
    base = effective_base_url(soup, base_url)
    URLs: Set[str] = set()
    for link in soup.select("link[href]"):
        rels = {r.lower() for r in (link.get("rel") or [])}
        if {
            "stylesheet",
            "icon",
            "apple-touch-icon",
            "shortcut icon",
            "manifest",
        } & rels or "preload" in rels:
            if "preload" in rels:
                as_type = (link.get("as") or "").lower()
                if as_type not in {"style", "image", "font"}:
                    continue
            href = link.get("href")
            if can_fetch_url(href):
                URLs.add(urljoin(base, href))
    for tag in soup.select(
        "img[src], source[src], video[src], audio[src], track[src], input[type=image][src]"
    ):
        src = tag.get("src")
        if can_fetch_url(src):
            URLs.add(urljoin(base, src))
    for tag in soup.select("video[poster]"):
        poster = tag.get("poster")
        if can_fetch_url(poster):
            URLs.add(urljoin(base, poster))
    for tag in soup.select("img[srcset], source[srcset]"):
        for u in parse_srcset(tag.get("srcset", "")):
            if can_fetch_url(u):
                URLs.add(urljoin(base, u))
    for tag in soup.select("[style]"):
        css = tag.get("style") or ""
        for u in parse_css_urls(css):
            URLs.add(urljoin(base, u))
    for style in soup.find_all("style"):
        text = style.string or ""
        for u in parse_css_urls(text):
            URLs.add(urljoin(base, u))
    if not skip_js:
        for tag in soup.select("script[src]"):
            src = tag.get("src")
            if can_fetch_url(src):
                URLs.add(urljoin(base, src))
    return URLs


def extract_anchor_links(soup: BeautifulSoup, base_url: str) -> Set[str]:
    base = effective_base_url(soup, base_url)
    urls: Set[str] = set()
    for a in soup.select("a[href]"):
        href = a.get("href")
        if not can_fetch_url(href):
            continue
        urls.add(urljoin(base, href))
    return urls


# -------------------- HTTP + throttle --------------------


def throttled_get(
    session: requests.Session,
    throttle: Throttle,
    url: str,
    *,
    timeout: float,
    stream: bool = False,
) -> requests.Response:
    throttle.acquire(url)
    r = session.get(url, timeout=timeout, stream=stream)
    throttle.on_result(url, r.status_code, r.headers)
    return r


# -------------------- WARC archiver --------------------


class WarcArchiver:
    def __init__(self, path: Path, gzip: bool = True, append: bool = True):
        self.enabled = False
        try:
            from warcio.statusandheaders import StatusAndHeaders  # type: ignore
            from warcio.warcwriter import WARCWriter  # type: ignore

            self._WARCWriter = WARCWriter
            self._StatusAndHeaders = StatusAndHeaders
        except Exception:
            logging.error("warcio not installed. Disable WARC or: pip install warcio")
            return
        ensure_parent_dir(path)
        self._fh = open(path, "ab" if append else "wb")
        self._writer = self._WARCWriter(self._fh, gzip=gzip)
        self.enabled = True

    def close(self) -> None:
        try:
            if self.enabled:
                self._fh.close()
        except Exception:
            pass

    def _req_headers(self, req: requests.PreparedRequest) -> List[Tuple[str, str]]:
        headers = list(req.headers.items())
        host = urlparse(req.url).netloc if req.url else None
        if host and not any(k.lower() == "host" for k, _ in headers):
            headers.insert(0, ("Host", host))
        return headers

    def _resp_headers(self, resp: requests.Response) -> List[Tuple[str, str]]:
        return list(resp.headers.items())

    def record_buffered(self, resp: requests.Response, body: bytes) -> None:
        if not self.enabled:
            return
        url = resp.url or ""
        req = resp.request
        req_line = f"{req.method} {req.path_url} HTTP/1.1"
        req_sh = self._StatusAndHeaders(req_line, self._req_headers(req), protocol=None)
        req_body = (
            req.body.encode("utf-8") if isinstance(req.body, str) else (req.body or b"")
        )
        rq = self._writer.create_warc_record(
            url, "request", payload=BytesIO(req_body), http_headers=req_sh
        )
        self._writer.write_record(rq)
        status_line = f"{resp.status_code} {resp.reason or ''}".strip()
        resp_sh = self._StatusAndHeaders(
            status_line, self._resp_headers(resp), protocol="HTTP/1.1"
        )
        rs = self._writer.create_warc_record(
            url, "response", payload=BytesIO(body), http_headers=resp_sh
        )
        self._writer.write_record(rs)

    def record_stream_file(self, resp: requests.Response, file_path: Path) -> None:
        if not self.enabled:
            return
        url = resp.url or ""
        req = resp.request
        req_line = f"{req.method} {req.path_url} HTTP/1.1"
        req_sh = self._StatusAndHeaders(req_line, self._req_headers(req), protocol=None)
        req_body = (
            req.body.encode("utf-8") if isinstance(req.body, str) else (req.body or b"")
        )
        rq = self._writer.create_warc_record(
            url, "request", payload=BytesIO(req_body), http_headers=req_sh
        )
        self._writer.write_record(rq)
        status_line = f"{resp.status_code} {resp.reason or ''}".strip()
        resp_sh = self._StatusAndHeaders(
            status_line, self._resp_headers(resp), protocol="HTTP/1.1"
        )
        with open(file_path, "rb") as f:
            rs = self._writer.create_warc_record(
                url, "response", payload=f, http_headers=resp_sh
            )
            self._writer.write_record(rs)


# -------------------- Persistent stores --------------------


class SeenStore:
    def add(self, url: str) -> None:
        raise NotImplementedError

    def __contains__(self, url: str) -> bool:
        raise NotImplementedError

    def dump_all(self) -> Optional[List[str]]:
        return None

    def close(self) -> None:
        pass


class MemSeen(SeenStore):
    def __init__(self, init: Optional[Iterable[str]] = None):
        self._s: Set[str] = set(init or [])

    def add(self, url: str) -> None:
        self._s.add(url)

    def __contains__(self, url: str) -> bool:
        return url in self._s

    def dump_all(self) -> List[str]:
        return list(self._s)


class DBMSeen(SeenStore):
    def __init__(self, path: Path):
        ensure_parent_dir(path)
        self._db = dbm.open(str(path), "c")
        self._lock = Lock()

    def add(self, url: str) -> None:
        with self._lock:
            self._db[url.encode("utf-8")] = b"1"

    def __contains__(self, url: str) -> bool:
        with self._lock:
            return url.encode("utf-8") in self._db

    def close(self) -> None:
        try:
            self._db.close()
        except Exception:
            pass


class AssetStore:
    def get(self, url: str) -> Optional[Path]:
        raise NotImplementedError

    def set(self, url: str, p: Path, root: Path) -> None:
        raise NotImplementedError

    def contains(self, url: str) -> bool:
        return self.get(url) is not None

    def update_from_downloaded(
        self, downloaded: Mapping[str, Path], root: Path
    ) -> None:
        for u, p in downloaded.items():
            self.set(u, p, root)

    def iter_paths(self, root: Path) -> Iterator[Path]:
        raise NotImplementedError

    def iter_url_paths(self, root: Path, exts: Set[str]) -> Iterator[Tuple[str, Path]]:
        raise NotImplementedError

    def close(self) -> None:
        pass


class MemAsset(AssetStore):
    # store absolute paths to avoid relpath errors during rewriting
    def __init__(self, init: Optional[Mapping[str, str]] = None):
        self._m: Dict[str, str] = dict(init or {})

    def get(self, url: str) -> Optional[Path]:
        v = self._m.get(url)
        return None if v is None else Path(v).resolve()

    def set(self, url: str, p: Path, root: Path) -> None:
        self._m[url] = str(p.resolve())

    def iter_paths(self, root: Path) -> Iterator[Path]:
        for abs_path in self._m.values():
            yield Path(abs_path)

    def iter_url_paths(self, root: Path, exts: Set[str]) -> Iterator[Tuple[str, Path]]:
        for u, abs_path in self._m.items():
            pp = Path(abs_path)
            if pp.suffix.lower() in exts:
                yield u, pp


class DBMAsset(AssetStore):
    def __init__(self, path: Path):
        ensure_parent_dir(path)
        self._db = dbm.open(str(path), "c")
        self._lock = Lock()

    def get(self, url: str) -> Optional[Path]:
        k = url.encode("utf-8")
        with self._lock:
            v = self._db.get(k)
        if v is None:
            return None
        return Path(v.decode("utf-8")).resolve()

    def set(self, url: str, p: Path, root: Path) -> None:
        abs_path = str(p.resolve()).encode("utf-8")
        with self._lock:
            self._db[url.encode("utf-8")] = abs_path

    def iter_paths(self, root: Path) -> Iterator[Path]:
        with self._lock:
            for k in self._db.keys():
                v = self._db[k].decode("utf-8")
                yield Path(v)

    def iter_url_paths(self, root: Path, exts: Set[str]) -> Iterator[Tuple[str, Path]]:
        with self._lock:
            for k in self._db.keys():
                url = k.decode("utf-8")
                v = self._db[k].decode("utf-8")
                p = Path(v)
                if p.suffix.lower() in exts:
                    yield url, p

    def close(self) -> None:
        try:
            self._db.close()
        except Exception:
            pass


# -------------------- Downloaders --------------------


def download_one(
    session: requests.Session,
    throttle: Throttle,
    absolute_url: str,
    base_origin: str,
    output_root: Path,
    settings: Settings,
    robots: Optional[robotparser.RobotFileParser] = None,
    archiver: Optional[WarcArchiver] = None,
) -> Optional[Path]:
    if settings.respect_robots and robots is not None:
        if not robots.can_fetch(session.headers.get("User-Agent", "*"), absolute_url):
            logging.debug("robots disallow: %s", absolute_url)
            return None
    try:
        resp = throttled_get(
            session, throttle, absolute_url, timeout=settings.timeout, stream=True
        )
        if resp.status_code >= 400:
            logging.warning("failed %s -> HTTP %s", absolute_url, resp.status_code)
            return None
        cl = resp.headers.get("Content-Length")
        if cl:
            try:
                if int(cl) > settings.max_bytes:
                    logging.warning("skip large file %s (%s bytes)", absolute_url, cl)
                    return None
            except ValueError:
                pass
        content_type = resp.headers.get("Content-Type")
        local_path = local_asset_path_for_url(
            absolute_url, base_origin, output_root, content_type
        )
        ensure_parent_dir(local_path)

        tmp_fh = None
        tmp_path: Optional[Path] = None
        if settings.warc and archiver and archiver.enabled:
            tmp_fh = tempfile.NamedTemporaryFile(
                prefix="warc_", suffix=".tmp", delete=False
            )
            tmp_path = Path(tmp_fh.name)

        written = 0
        with open(local_path, "wb") as f:
            for chunk in resp.iter_content(chunk_size=64 * 1024):
                if not chunk:
                    continue
                written += len(chunk)
                if written > settings.max_bytes:
                    logging.warning("truncated %s at %d bytes", absolute_url, written)
                    break
                f.write(chunk)
                if tmp_fh is not None:
                    tmp_fh.write(chunk)

        if tmp_fh is not None:
            try:
                tmp_fh.flush()
                tmp_fh.close()
                if tmp_path is not None:
                    archiver.record_stream_file(resp, tmp_path)
            finally:
                if tmp_path is not None:
                    try:
                        tmp_path.unlink(missing_ok=True)
                    except Exception:
                        pass

        if written == 0:
            logging.warning("empty response %s", absolute_url)
            return None

        logging.info("downloaded asset: %s -> %s", absolute_url, local_path)
        return local_path
    except requests.RequestException as e:
        logging.warning("error downloading %s: %s", absolute_url, e)
        return None


def download_all(
    session: requests.Session,
    throttle: Throttle,
    urls: Iterable[str],
    base_origin: str,
    output_root: Path,
    settings: Settings,
    robots: Optional[robotparser.RobotFileParser] = None,
    archiver: Optional[WarcArchiver] = None,
) -> Dict[str, Path]:
    url_set: Set[str] = set(urls)
    result: Dict[str, Path] = {}
    if not url_set:
        return result
    with ThreadPoolExecutor(max_workers=settings.workers) as pool:
        future_map = {
            pool.submit(
                download_one,
                session,
                throttle,
                u,
                base_origin,
                output_root,
                settings,
                robots,
                archiver,
            ): u
            for u in url_set
        }
        for fut in as_completed(future_map):
            u = future_map[fut]
            p = fut.result()
            if p is not None:
                result[u] = p
    return result


# -------------------- Rewriters --------------------


def rewrite_html_assets_and_srcset(
    soup: BeautifulSoup,
    page_url: str,
    mapping: AssetStore,
    html_dir: Path,
) -> None:
    base = effective_base_url(soup, page_url)

    def to_rel(p: Path) -> str:
        return Path(os.path.relpath(p, html_dir)).as_posix()

    attr_map = {
        "img": ["src"],
        "source": ["src"],
        "video": ["src", "poster"],
        "audio": ["src"],
        "track": ["src"],
        "script": ["src"],
        "link": ["href"],
    }
    for tag_name, attrs in attr_map.items():
        for tag in soup.find_all(tag_name):
            for a in attrs:
                val = tag.get(a)
                if not val or not can_fetch_url(val):
                    continue
                absu = urljoin(base, val)
                p = mapping.get(absu)
                if p is not None:
                    tag[a] = to_rel(p)
                    for rm in ("integrity", "crossorigin", "referrerpolicy"):
                        if rm in tag.attrs:
                            del tag.attrs[rm]
    for tag in soup.select("img[srcset], source[srcset]"):
        srcset_val = tag.get("srcset", "")
        parts = []
        for candidate in SRCSET_SPLIT_RE.split(srcset_val.strip()):
            if not candidate:
                continue
            comp = WS_RE.split(candidate.strip())
            if not comp:
                continue
            url_part = comp[0]
            desc = " ".join(comp[1:])
            absu = urljoin(base, url_part)
            p = mapping.get(absu)
            url_out = to_rel(p) if p is not None else url_part
            parts.append((url_out, desc))
        tag["srcset"] = ", ".join([f"{u} {d}".strip() for u, d in parts if u])


def rewrite_css_text_in_context(
    css_text: str, css_base_url: str, mapping: AssetStore, rel_base_dir: Path
) -> str:
    def map_url(u: str) -> str:
        absu = urljoin(css_base_url, u)
        p = mapping.get(absu)
        if p is None:
            return u
        return Path(os.path.relpath(p, rel_base_dir)).as_posix()

    def repl_url(m: re.Match) -> str:
        q = m.group(1) or ""
        u = m.group(2).strip()
        nu = map_url(u)
        return f"url({q}{nu}{q})"

    def repl_import(m: re.Match) -> str:
        q = m.group(1) or ""
        u = m.group(2).strip()
        nu = map_url(u)
        return f"@import url({q}{nu}{q});"

    t = CSS_URL_RE.sub(repl_url, css_text)
    t = CSS_IMPORT_RE.sub(repl_import, t)
    return t


def rewrite_inline_css_in_html(
    soup: BeautifulSoup, page_url: str, mapping: AssetStore, html_dir: Path
) -> None:
    base = effective_base_url(soup, page_url)
    for tag in soup.select("[style]"):
        css = tag.get("style")
        if not css:
            continue
        new_css = rewrite_css_text_in_context(css, base, mapping, html_dir)
        if new_css != css:
            tag["style"] = new_css
    for style in soup.find_all("style"):
        if style.string:
            new_text = rewrite_css_text_in_context(
                style.string, base, mapping, html_dir
            )
            if new_text != style.string:
                style.string.replace_with(new_text)


def rewrite_links_in_place(
    soup: BeautifulSoup,
    page_url: str,
    expected_pages: Dict[str, Path],
    html_dir: Path,
    *,
    strip_params: bool,
    allow_params: Set[str],
) -> None:
    base = effective_base_url(soup, page_url)
    for a in soup.select("a[href]"):
        href = a.get("href")
        if not href or not can_fetch_url(href):
            continue
        absu = urljoin(base, href)
        parsed = urlparse(absu)
        frag = parsed.fragment
        norm = normalize_url(absu, strip_params=strip_params, allow_params=allow_params)
        target = expected_pages.get(norm)
        if target:
            rel = Path(os.path.relpath(target, html_dir)).as_posix()
            if frag:
                rel = f"{rel}#{frag}"
            a["href"] = rel


def rewrite_js_text_regex(
    js_text: str, js_base_url: str, mapping: AssetStore, rel_base_dir: Path
) -> str:
    def repl(m: re.Match) -> str:
        q = m.group("q")
        u = m.group("u")
        absu = urljoin(js_base_url, u)
        p = mapping.get(absu)
        if p is None:
            return m.group(0)
        rel = Path(os.path.relpath(p, rel_base_dir)).as_posix()
        return f"{q}{rel}{q}"

    try:
        return JS_STRING_URL_RE.sub(repl, js_text)
    except Exception:
        return js_text


def rewrite_js_text_ast(
    js_text: str, js_base_url: str, mapping: AssetStore, rel_base_dir: Path
) -> str:
    try:
        import esprima  # pip install esprima
    except Exception:
        logging.error(
            "esprima not installed. Disable --rewrite-js-ast or: pip install esprima"
        )
        return js_text
    try:
        ast = esprima.parseScript(js_text, loc=False, range=True, tolerant=True)
    except Exception as e:
        logging.warning("AST parse failed: %s", e)
        return js_text

    replacements: List[Tuple[int, int, str]] = []

    def add_literal_rewrite(lit_node):
        if not lit_node or lit_node.get("type") != "Literal":
            return
        if not isinstance(lit_node.get("value"), str):
            return
        start, end = lit_node["range"]
        q = (
            js_text[start]
            if start < len(js_text) and js_text[start] in ("'", '"')
            else '"'
        )
        absu = urljoin(js_base_url, lit_node["value"])
        p = mapping.get(absu)
        if p is None:
            return
        rel = Path(os.path.relpath(p, rel_base_dir)).as_posix()
        replacements.append((start, end, f"{q}{rel}{q}"))

    def prop_name(member):
        if not member or member.get("type") != "MemberExpression":
            return None
        if member.get("computed"):
            return None
        prop = member.get("property")
        if prop and prop.get("type") == "Identifier":
            return prop.get("name")
        return None

    def walk(node, parent=None):
        if isinstance(node, list):
            for x in node:
                walk(x, parent)
            return
        if not isinstance(node, dict):
            return
        t = node.get("type")

        if t == "ImportDeclaration":
            add_literal_rewrite(node.get("source"))

        if t in ("CallExpression", "NewExpression"):
            callee = node.get("callee")
            args = node.get("arguments") or []
            if callee and callee.get("type") == "Identifier":
                name = callee.get("name")
                if name in ("require", "fetch") and args:
                    add_literal_rewrite(args[0] if isinstance(args[0], dict) else None)
            if callee and callee.get("type") == "Import":
                if args:
                    add_literal_rewrite(args[0] if isinstance(args[0], dict) else None)
            if (
                t == "NewExpression"
                and callee
                and callee.get("type") == "Identifier"
                and callee.get("name") == "Request"
            ):
                if args:
                    add_literal_rewrite(args[0] if isinstance(args[0], dict) else None)
            if (
                callee
                and callee.get("type") == "MemberExpression"
                and prop_name(callee) == "open"
            ):
                if len(args) >= 2 and isinstance(args[1], dict):
                    add_literal_rewrite(args[1])

        if t == "AssignmentExpression":
            left = node.get("left")
            right = node.get("right")
            if left and left.get("type") == "MemberExpression":
                pn = prop_name(left)
                if pn in {"src", "href"} and isinstance(right, dict):
                    add_literal_rewrite(right)

        for k, v in node.items():
            if k in ("range",):
                continue
            walk(v, node)

    walk(ast, None)

    if not replacements:
        return js_text

    out = []
    last = len(js_text)
    for s, e, repl in sorted(replacements, key=lambda x: x[0], reverse=True):
        out.append(js_text[e:last])
        out.append(repl)
        last = s
    out.append(js_text[0:last])
    return "".join(reversed(out))


# -------------------- Robots/Sitemaps --------------------


def fetch_robots(
    base_url: str,
    session: requests.Session,
    throttle: Throttle,
    archiver: Optional[WarcArchiver],
) -> Optional[robotparser.RobotFileParser]:
    try:
        robots_url = urljoin(base_url, "/robots.txt")
        r = throttled_get(session, throttle, robots_url, timeout=10)
        if archiver and archiver.enabled:
            try:
                archiver.record_buffered(r, r.content)
            except Exception:
                pass
        if r.status_code >= 400 or not r.text:
            return None
        rp = robotparser.RobotFileParser()
        rp.parse(r.text.splitlines())
        return rp
    except Exception:
        return None


def discover_sitemaps(
    session: requests.Session,
    throttle: Throttle,
    base_url: str,
    archiver: Optional[WarcArchiver],
) -> List[str]:
    sites: List[str] = []
    try:
        robots_url = urljoin(base_url, "/robots.txt")
        r = throttled_get(session, throttle, robots_url, timeout=10)
        if archiver and archiver.enabled:
            try:
                archiver.record_buffered(r, r.content)
            except Exception:
                pass
        if r.status_code == 200 and r.text:
            for line in r.text.splitlines():
                if line.strip().lower().startswith("sitemap:"):
                    sites.append(line.split(":", 1)[1].strip())
    except Exception:
        pass
    return sites


def parse_sitemap_urls(
    session: requests.Session,
    throttle: Throttle,
    sitemap_url: str,
    archiver: Optional[WarcArchiver],
) -> List[str]:
    urls: List[str] = []
    try:
        r = throttled_get(session, throttle, sitemap_url, timeout=15)
        if archiver and archiver.enabled:
            try:
                archiver.record_buffered(r, r.content)
            except Exception:
                pass
        if r.status_code >= 400:
            return urls
        root = ET.fromstring(r.text)
        if root.tag.endswith("sitemapindex"):
            for loc in root.findall(".//{*}loc"):
                child = (loc.text or "").strip()
                if child:
                    urls.extend(parse_sitemap_urls(session, throttle, child, archiver))
        else:
            for loc in root.findall(".//{*}loc"):
                if loc.text:
                    urls.append(loc.text.strip())
    except Exception:
        pass
    return urls


# -------------------- Rendering --------------------


class HtmlRenderer:
    def fetch(
        self, session: requests.Session, throttle: Throttle, url: str, timeout: float
    ) -> Tuple[Optional[str], Optional[str]]:
        raise NotImplementedError

    def close(self) -> None:
        pass


class RequestsRenderer(HtmlRenderer):
    def __init__(self, archiver: Optional[WarcArchiver] = None):
        self.archiver = archiver

    def fetch(
        self, session: requests.Session, throttle: Throttle, url: str, timeout: float
    ) -> Tuple[Optional[str], Optional[str]]:
        r = throttled_get(session, throttle, url, timeout=timeout)
        if self.archiver and self.archiver.enabled:
            try:
                self.archiver.record_buffered(r, r.content)
            except Exception:
                pass
        if r.status_code >= 400:
            return None, None
        ct = (r.headers.get("Content-Type") or "").lower()
        if "text/html" not in ct and "application/xhtml+xml" not in ct:
            return None, None
        if not r.encoding:
            try:
                r.encoding = r.apparent_encoding or "utf-8"
            except Exception:
                r.encoding = "utf-8"
        return r.text, r.encoding


class PlaywrightRenderer(HtmlRenderer):
    def __init__(
        self,
        wait_until: str = "networkidle",
        timeout_ms: int = 10000,
        block_media: bool = True,
        session: Optional[requests.Session] = None,
    ):
        self.wait_until = wait_until
        self.timeout_ms = timeout_ms
        self.block_media = block_media
        self.session = session
        self._pl = None
        self._browser = None

    def _cookies_for_url(self, url: str) -> List[dict]:
        if self.session is None:
            return []
        out = []
        u = urlparse(url)
        host = u.hostname or ""
        path = u.path or "/"
        secure = u.scheme == "https"
        for c in self.session.cookies:
            dom = (c.domain or "").lstrip(".")
            host_ok = (host == dom) or (dom and host.endswith("." + dom))
            path_ok = (path or "/").startswith(c.path or "/")
            sec_ok = (not c.secure) or secure
            if host_ok and path_ok and sec_ok:
                out.append(
                    {
                        "name": c.name,
                        "value": c.value,
                        "domain": c.domain or host,
                        "path": c.path or "/",
                        "secure": bool(c.secure),
                        "httpOnly": False,
                    }
                )
        return out

    def _ensure_browser(self):
        if self._pl is None or self._browser is None:
            try:
                from playwright.sync_api import sync_playwright
            except Exception:
                logging.error(
                    "Playwright not installed. Run: pip install playwright && playwright install"
                )
                return False
            self._pl = sync_playwright().start()
            self._browser = self._pl.chromium.launch(headless=True)
        return True

    def fetch(
        self, session: requests.Session, throttle: Throttle, url: str, timeout: float
    ) -> Tuple[Optional[str], Optional[str]]:
        if not self._ensure_browser():
            return None, None
        html: Optional[str] = None
        try:
            throttle.acquire(url)
            ua = None
            locale = None
            if self.session is not None:
                ua = self.session.headers.get("User-Agent")
                locale = (self.session.headers.get("Accept-Language") or "en-US").split(
                    ","
                )[0]
            context = self._browser.new_context(user_agent=ua, locale=locale)
            if self.session is not None:
                cookies = self._cookies_for_url(url)
                if cookies:
                    context.add_cookies(cookies)
                headers = dict(self.session.headers)
                context.set_extra_http_headers(headers)
            page = context.new_page()
            if self.block_media:

                def _route(route):
                    rt = route.request.resource_type
                    if rt in ("image", "media", "font"):
                        route.abort()
                    else:
                        route.continue_()

                page.route("**/*", _route)
            page.goto(url, wait_until=self.wait_until, timeout=self.timeout_ms)
            html = page.content()
            context.close()
            throttle.on_result(url, 200, {})
        except Exception as e:
            logging.warning("Playwright render failed for %s: %s", url, e)
            html = None
        return html, ("utf-8" if html is not None else None)

    def close(self) -> None:
        try:
            if self._browser:
                self._browser.close()
        except Exception:
            pass
        try:
            if self._pl:
                self._pl.stop()
        except Exception:
            pass


def get_renderer(
    settings: Settings, session: requests.Session, archiver: Optional[WarcArchiver]
) -> HtmlRenderer:
    if settings.render_js:
        return PlaywrightRenderer(
            settings.wait_until,
            settings.render_timeout_ms,
            settings.render_block_media,
            session,
        )
    return RequestsRenderer(archiver)


# -------------------- CSS dependency expansion --------------------


def expand_css_dependencies(
    session: requests.Session,
    throttle: Throttle,
    base_origin: str,
    output_root: Path,
    settings: Settings,
    robots: Optional[robotparser.RobotFileParser],
    store: AssetStore,
    css_urls_to_scan: Set[str],
    scanned_css: Set[str],
    archiver: Optional[WarcArchiver],
    max_passes: int = 2,
) -> None:
    for _ in range(max_passes):
        new_urls: Set[str] = set()
        for css_url in list(css_urls_to_scan):
            if css_url in scanned_css:
                continue
            scanned_css.add(css_url)
            css_path = store.get(css_url)
            if not css_path or css_path.suffix.lower() != ".css":
                continue
            try:
                text = css_path.read_text(encoding="utf-8", errors="ignore")
            except Exception:
                continue
            for u in parse_css_urls(text):
                if not can_fetch_url(u):
                    continue
                absu = urljoin(css_url, u)
                if store.contains(absu):
                    continue
                if not url_allowed(absu, base_origin, settings):
                    continue
                new_urls.add(absu)
        if not new_urls:
            break
        downloaded = download_all(
            session,
            throttle,
            new_urls,
            base_origin,
            output_root,
            settings,
            robots,
            archiver,
        )
        store.update_from_downloaded(downloaded, site_root(base_origin, output_root))
        css_urls_to_scan = {
            u for u, p in downloaded.items() if p.suffix.lower() == ".css"
        }


# -------------------- Checkpoint I/O --------------------


def default_checkpoint_path(base_url: str, output_root: Path) -> Path:
    return meta_dir(base_url, output_root) / "checkpoint.json"


def atomic_write_json(path: Path, data: dict) -> None:
    ensure_parent_dir(path)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(data, indent=2), encoding="utf-8")
    os.replace(tmp, path)


def save_checkpoint(
    base_url: str,
    output_root: Path,
    *,
    start_norm: str,
    frontier: deque,
    pages_done: int,
    page_paths: List[str],
    scanned_css: Set[str],
    settings: Settings,
    seen: SeenStore,
    assets: AssetStore,
    kv_seen_path: Optional[Path],
    kv_asset_path: Optional[Path],
) -> None:
    root = site_root(base_url, output_root)
    rel_pages = [str(Path(p).resolve().relative_to(root)) for p in page_paths]
    ck = {
        "start_norm": start_norm,
        "frontier": list(frontier),
        "pages_done": pages_done,
        "page_paths": rel_pages,
        "scanned_css": list(scanned_css),
        "kv_backend": settings.kv_backend,
        "kv_paths": {
            "seen": (
                str(kv_seen_path.resolve().relative_to(root)) if kv_seen_path else None
            ),
            "asset": (
                str(kv_asset_path.resolve().relative_to(root))
                if kv_asset_path
                else None
            ),
        },
    }
    if settings.kv_backend == "none":
        if settings.checkpoint_include_seen_when_mem:
            ck["seen_urls"] = seen.dump_all() or []
        mem_assets = {}
        for u, p in list(
            assets.iter_url_paths(
                root,
                {
                    ".css",
                    ".js",
                    ".mjs",
                    ".png",
                    ".jpg",
                    ".jpeg",
                    ".gif",
                    ".svg",
                    ".ico",
                    ".webp",
                    ".woff",
                    ".woff2",
                    ".ttf",
                    ".otf",
                    ".json",
                    ".webmanifest",
                    ".mp4",
                    ".webm",
                    ".mp3",
                    ".ogg",
                    ".wav",
                },
            )
        ):
            mem_assets[u] = str(p.resolve().relative_to(root))
        ck["asset_map"] = mem_assets
    p = (
        Path(settings.checkpoint_file)
        if settings.checkpoint_file
        else default_checkpoint_path(base_url, output_root)
    )
    atomic_write_json(p, ck)
    logging.info("checkpoint saved: %s", p)


def load_checkpoint(
    base_url: str, output_root: Path, path: Optional[str]
) -> Optional[dict]:
    p = Path(path) if path else default_checkpoint_path(base_url, output_root)
    if not p.exists():
        return None
    try:
        data = json.loads(p.read_text(encoding="utf-8"))
        return data
    except Exception as e:
        logging.warning("failed to load checkpoint %s: %s", p, e)
        return None


# -------------------- Main: single page --------------------


def clone_single_page(
    base_url: str,
    output_folder: str,
    settings: Settings,
    archiver: Optional[WarcArchiver],
) -> None:
    out_root = Path(output_folder).resolve()
    out_root.mkdir(parents=True, exist_ok=True)
    session = build_session()
    apply_auth_to_session(session, settings)
    throttle = Throttle(settings)
    robots = (
        fetch_robots(base_url, session, throttle, archiver)
        if settings.respect_robots
        else None
    )
    renderer = get_renderer(settings, session, archiver)

    try:
        logging.info("GET %s", base_url)
        html, _ = renderer.fetch(session, throttle, base_url, settings.timeout)
        if html is None:
            print(f"Critical error: failed to fetch HTML for {base_url}")
            return
        soup = bs4_parse(html)

        root = site_root(base_url, out_root)
        asset_store: AssetStore = MemAsset()

        # filter assets by allowed policy
        raw_assets = extract_asset_urls(soup, base_url, skip_js=settings.skip_js)
        asset_urls = filter_allowed(raw_assets, base_url, settings)
        downloaded = download_all(
            session,
            throttle,
            asset_urls,
            base_url,
            out_root,
            settings,
            robots,
            archiver,
        )
        asset_store.update_from_downloaded(downloaded, root)

        scanned_css: Set[str] = set()
        css_to_scan: Set[str] = {
            u for u, p in downloaded.items() if p.suffix.lower() == ".css"
        }
        expand_css_dependencies(
            session,
            throttle,
            base_url,
            out_root,
            settings,
            robots,
            asset_store,
            css_to_scan,
            scanned_css,
            archiver,
        )

        html_path = local_html_path_for_url(base_url, base_url, out_root)
        html_dir = html_path.parent
        html_dir.mkdir(parents=True, exist_ok=True)
        rewrite_html_assets_and_srcset(soup, base_url, asset_store, html_dir)
        rewrite_inline_css_in_html(soup, base_url, asset_store, html_dir)

        # JS rewriting
        if settings.rewrite_js_ast or settings.rewrite_js_urls:
            for js_url, js_path in asset_store.iter_url_paths(root, {".js", ".mjs"}):
                try:
                    txt = js_path.read_text(encoding="utf-8", errors="ignore")
                    if settings.rewrite_js_ast:
                        new_txt = rewrite_js_text_ast(
                            txt, js_url, asset_store, js_path.parent
                        )
                    else:
                        new_txt = rewrite_js_text_regex(
                            txt, js_url, asset_store, js_path.parent
                        )
                    if new_txt != txt:
                        js_path.write_text(new_txt, encoding="utf-8")
                except Exception:
                    pass

        html_path.write_text(serialize_html(soup), encoding="utf-8")

        # Rewrite CSS after deps resolved
        for css_url, css_path in asset_store.iter_url_paths(root, {".css"}):
            try:
                text = css_path.read_text(encoding="utf-8", errors="ignore")
                new_text = rewrite_css_text_in_context(
                    text, css_url, asset_store, css_path.parent
                )
                if new_text != text:
                    css_path.write_text(new_text, encoding="utf-8")
            except Exception:
                pass

        write_manifest(
            base_url,
            out_root,
            pages=[str(html_path)],
            assets=list(asset_store.iter_paths(root)),
        )
        print("Cloning complete")
        print(f"Saved to: {html_path}")
    finally:
        try:
            renderer.close()
        except Exception:
            pass


# -------------------- Main: crawler with resume --------------------


def mirror_site(
    base_url: str,
    output_folder: str,
    settings: Settings,
    archiver: Optional[WarcArchiver],
) -> None:
    out_root = Path(output_folder).resolve()
    out_root.mkdir(parents=True, exist_ok=True)
    root = site_root(base_url, out_root)
    meta = meta_dir(base_url, out_root)
    meta.mkdir(parents=True, exist_ok=True)

    # Persistent stores
    kv_seen_path: Optional[Path] = None
    kv_asset_path: Optional[Path] = None
    if settings.kv_backend == "dbm":
        kv_dir = Path(settings.kv_dir) if settings.kv_dir else meta
        kv_seen_path = kv_dir / "seen.db"
        kv_asset_path = kv_dir / "assets.db"
        seen: SeenStore = DBMSeen(kv_seen_path)
        asset_store: AssetStore = DBMAsset(kv_asset_path)
    else:
        seen = MemSeen()
        asset_store = MemAsset()

    session = build_session()
    apply_auth_to_session(session, settings)
    throttle = Throttle(settings)
    robots = (
        fetch_robots(base_url, session, throttle, archiver)
        if settings.respect_robots
        else None
    )
    if robots and settings.delay <= 0.0:
        try:
            # use session UA for crawl-delay
            sess_ua = session.headers.get("User-Agent", "*")
            d = robots.crawl_delay(sess_ua)
            if d:
                settings.delay = max(settings.delay, float(d))
        except Exception:
            pass

    renderer = get_renderer(settings, session, archiver)

    start_norm = normalize_url(
        base_url, strip_params=settings.strip_params, allow_params=settings.allow_params
    )
    frontier: deque[Tuple[str, int]] = deque([(start_norm, 0)])
    enqueued: Set[str] = {start_norm}
    expected_page_map: Dict[str, Path] = {
        start_norm: local_html_path_for_url(start_norm, base_url, out_root)
    }
    page_paths: List[str] = []
    pages_done = 0
    scanned_css: Set[str] = set()

    # Resume
    if settings.resume:
        ck = load_checkpoint(base_url, out_root, settings.checkpoint_file)
        if ck:
            logging.info("resuming from checkpoint")
            start_norm = ck.get("start_norm", start_norm)
            frontier = deque([(u, int(d)) for u, d in ck.get("frontier", [])])
            enqueued = {u for u, _ in frontier}
            pages_done = int(ck.get("pages_done", 0))
            page_paths = [str(root / rel) for rel in ck.get("page_paths", [])]
            scanned_css = set(ck.get("scanned_css", []))
            expected_page_map = {
                u: local_html_path_for_url(u, base_url, out_root) for u, _ in frontier
            }
            expected_page_map[start_norm] = local_html_path_for_url(
                start_norm, base_url, out_root
            )
            if settings.kv_backend == "none":
                if settings.checkpoint_include_seen_when_mem and "seen_urls" in ck:
                    seen = MemSeen(ck["seen_urls"])
                if "asset_map" in ck:
                    # convert stored relative paths to absolute on load
                    asset_store = MemAsset(
                        {
                            u: str((root / rel).resolve())
                            for u, rel in ck["asset_map"].items()
                        }
                    )
            else:
                kp = ck.get("kv_paths", {})
                if kp:
                    if kp.get("seen"):
                        kv_seen_path = root / kp["seen"]
                        seen = DBMSeen(kv_seen_path)
                    if kp.get("asset"):
                        kv_asset_path = root / kp["asset"]
                        asset_store = DBMAsset(kv_asset_path)
        else:
            logging.info("no checkpoint found, starting fresh")

    # optional sitemap seeding
    if settings.sitemap_seed:
        for sm in discover_sitemaps(session, throttle, base_url, archiver):
            for u in parse_sitemap_urls(session, throttle, sm, archiver):
                if not can_fetch_url(u):
                    continue
                if not url_allowed(u, base_url, settings):
                    continue
                n = normalize_url(
                    u,
                    strip_params=settings.strip_params,
                    allow_params=settings.allow_params,
                )
                if n not in enqueued:
                    frontier.append((n, 0))
                    enqueued.add(n)
                    expected_page_map[n] = local_html_path_for_url(
                        n, base_url, out_root
                    )

    last_ck = time.monotonic()

    try:
        while frontier and pages_done < settings.max_pages:
            url, depth = frontier.popleft()
            if url in seen:
                continue
            seen.add(url)

            if settings.respect_robots and robots is not None:
                if not robots.can_fetch(session.headers.get("User-Agent", "*"), url):
                    logging.info("robots disallow page: %s", url)
                    continue

            logging.info(
                "Fetch page [%d/%d] depth=%d: %s",
                pages_done + 1,
                settings.max_pages,
                depth,
                url,
            )
            html_text, _ = renderer.fetch(session, throttle, url, settings.timeout)
            if html_text is None:
                ap = download_one(
                    session,
                    throttle,
                    url,
                    base_url,
                    out_root,
                    settings,
                    robots,
                    archiver,
                )
                if ap:
                    asset_store.set(url, ap, root)
                continue

            soup = bs4_parse(html_text)

            if depth < settings.max_depth:
                anchors = extract_anchor_links(soup, url)
                for link in anchors:
                    if not url_allowed(link, base_url, settings):
                        continue
                    n = normalize_url(
                        link,
                        strip_params=settings.strip_params,
                        allow_params=settings.allow_params,
                    )
                    if n not in enqueued:
                        frontier.append((n, depth + 1))
                        enqueued.add(n)
                        expected_page_map[n] = local_html_path_for_url(
                            n, base_url, out_root
                        )

            raw_assets = extract_asset_urls(soup, url, skip_js=settings.skip_js)
            asset_urls = filter_allowed(raw_assets, base_url, settings)
            downloaded = download_all(
                session,
                throttle,
                asset_urls,
                base_url,
                out_root,
                settings,
                robots,
                archiver,
            )
            asset_store.update_from_downloaded(downloaded, root)

            css_to_scan: Set[str] = {
                u for u, p in downloaded.items() if p.suffix.lower() == ".css"
            }
            expand_css_dependencies(
                session,
                throttle,
                base_url,
                out_root,
                settings,
                robots,
                asset_store,
                css_to_scan,
                scanned_css,
                archiver,
            )

            # JS rewriting for newly downloaded files
            if settings.rewrite_js_ast or settings.rewrite_js_urls:
                for js_url, js_path in [
                    (u, p)
                    for u, p in downloaded.items()
                    if p.suffix.lower() in {".js", ".mjs"}
                ]:
                    try:
                        txt = js_path.read_text(encoding="utf-8", errors="ignore")
                        if settings.rewrite_js_ast:
                            new_txt = rewrite_js_text_ast(
                                txt, js_url, asset_store, js_path.parent
                            )
                        else:
                            new_txt = rewrite_js_text_regex(
                                txt, js_url, asset_store, js_path.parent
                            )
                        if new_txt != txt:
                            js_path.write_text(new_txt, encoding="utf-8")
                    except Exception:
                        pass

            html_path = expected_page_map.get(url) or local_html_path_for_url(
                url, base_url, out_root
            )
            html_dir = html_path.parent
            html_dir.mkdir(parents=True, exist_ok=True)
            rewrite_html_assets_and_srcset(soup, url, asset_store, html_dir)
            rewrite_inline_css_in_html(soup, url, asset_store, html_dir)
            rewrite_links_in_place(
                soup,
                url,
                expected_page_map,
                html_dir,
                strip_params=settings.strip_params,
                allow_params=settings.allow_params,
            )
            ensure_parent_dir(html_path)
            html_path.write_text(serialize_html(soup), encoding="utf-8")
            page_paths.append(str(html_path))
            pages_done += 1

            # checkpoint
            now = time.monotonic()
            if (
                settings.checkpoint_pages > 0
                and pages_done % settings.checkpoint_pages == 0
            ) or (
                settings.checkpoint_seconds > 0
                and (now - last_ck) >= settings.checkpoint_seconds
            ):
                save_checkpoint(
                    base_url,
                    out_root,
                    start_norm=start_norm,
                    frontier=frontier,
                    pages_done=pages_done,
                    page_paths=page_paths,
                    scanned_css=scanned_css,
                    settings=settings,
                    seen=seen,
                    assets=asset_store,
                    kv_seen_path=kv_seen_path,
                    kv_asset_path=kv_asset_path,
                )
                last_ck = now

        # Final CSS pass
        for css_url, css_path in asset_store.iter_url_paths(root, {".css"}):
            try:
                text = css_path.read_text(encoding="utf-8", errors="ignore")
                new_text = rewrite_css_text_in_context(
                    text, css_url, asset_store, css_path.parent
                )
                if new_text != text:
                    css_path.write_text(new_text, encoding="utf-8")
            except Exception:
                pass
        # Final JS pass (optional)
        if settings.rewrite_js_ast or settings.rewrite_js_urls:
            for js_url, js_path in asset_store.iter_url_paths(root, {".js", ".mjs"}):
                try:
                    txt = js_path.read_text(encoding="utf-8", errors="ignore")
                    if settings.rewrite_js_ast:
                        new_txt = rewrite_js_text_ast(
                            txt, js_url, asset_store, js_path.parent
                        )
                    else:
                        new_txt = rewrite_js_text_regex(
                            txt, js_url, asset_store, js_path.parent
                        )
                    if new_txt != txt:
                        js_path.write_text(new_txt, encoding="utf-8")
                except Exception:
                    pass

    except KeyboardInterrupt:
        logging.warning("Interrupted. Saving checkpoint.")
        save_checkpoint(
            base_url,
            out_root,
            start_norm=start_norm,
            frontier=frontier,
            pages_done=pages_done,
            page_paths=page_paths,
            scanned_css=scanned_css,
            settings=settings,
            seen=seen,
            assets=asset_store,
            kv_seen_path=kv_seen_path,
            kv_asset_path=kv_asset_path,
        )
        raise
    finally:
        try:
            seen.close()
        except Exception:
            pass
        try:
            asset_store.close()
        except Exception:
            pass
        try:
            renderer.close()
        except Exception:
            pass

    write_manifest(
        base_url, out_root, pages=page_paths, assets=list(asset_store.iter_paths(root))
    )
    print("Mirroring complete")
    print(f"Pages saved: {pages_done}")
    print(f"Root: {site_root(base_url, out_root)}")


# -------------------- Manifest --------------------


def write_manifest(
    base_url: str, output_root: Path, *, pages: List[str], assets: List[Path]
) -> None:
    root = site_root(base_url, output_root)
    meta = meta_dir(base_url, output_root)
    meta.mkdir(parents=True, exist_ok=True)

    def rel(p: Union[str, Path]) -> str:
        return str(Path(p).resolve().relative_to(root))

    # dedupe while preserving order
    pages_rel = list(dict.fromkeys(rel(p) for p in pages))
    assets_rel = list(dict.fromkeys(rel(a) for a in assets))
    # RFC3339 UTC timestamp without microseconds
    created_ts = (
        datetime.now(timezone.utc)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z")
    )
    data = {
        "site": base_url,
        "created_utc": created_ts,
        "pages": pages_rel,
        "assets": assets_rel,
        "layout": {
            "pages_dir": "pages/",
            "assets_dir": "assets/",
            "vendor_dir": "assets/vendor/",
        },
        "notes": "Assets grouped by type. Third-party assets under vendor/<host>/.",
    }
    (meta / "manifest.json").write_text(json.dumps(data, indent=2), encoding="utf-8")


# -------------------- Config loader --------------------


def load_config_file(path: str) -> Dict[str, Union[str, int, float, bool, List[str]]]:
    p = Path(path)
    suf = p.suffix.lower()
    if suf in {".toml", ".tml"}:
        try:
            import tomllib  # py311+
        except Exception:
            try:
                import tomli as tomllib  # backport
            except Exception:
                raise RuntimeError("TOML config requires Python 3.11+ or 'tomli'")
        with open(p, "rb") as f:
            return tomllib.load(f) or {}
    elif suf in {".yaml", ".yml"}:
        try:
            import yaml
        except Exception:
            raise RuntimeError("YAML config requires 'PyYAML'")
        with open(p, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}
            if not isinstance(data, dict):
                raise RuntimeError("Top-level YAML must be a mapping")
            return data
    else:
        raise RuntimeError("Unsupported config format. Use .toml or .yaml")


# -------------------- CLI --------------------


def build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="Clone a page or crawl a site with structured output.",
        add_help=True,
    )
    p.add_argument("--config", type=str, help="path to config.toml|.yaml", default=None)

    p.add_argument("url", help="http(s) URL")
    p.add_argument("output_folder", help="output directory")
    p.add_argument("--external", action="store_true", help="include third-party assets")
    p.add_argument("--respect-robots", action="store_true", help="respect robots.txt")
    p.add_argument("--delay", type=float, default=0.0, help="base delay seconds")
    p.add_argument(
        "--timeout", type=float, default=15.0, help="request timeout seconds"
    )
    p.add_argument("--workers", type=int, default=16, help="concurrent downloads")
    p.add_argument(
        "--max-bytes", type=int, default=50_000_000, help="max bytes per file"
    )
    p.add_argument(
        "--skip-js", action="store_true", help="do not download script files"
    )
    p.add_argument("--verbose", action="store_true", help="debug logging")

    # crawl
    p.add_argument(
        "--crawl", action="store_true", help="crawl links and mirror multiple pages"
    )
    p.add_argument("--max-pages", type=int, default=100, help="max HTML pages")
    p.add_argument("--max-depth", type=int, default=1, help="max link depth")
    p.add_argument(
        "--include", type=str, default=None, help="only crawl URLs matching regex"
    )
    p.add_argument("--exclude", type=str, default=None, help="skip URLs matching regex")
    p.add_argument(
        "--no-strip-params", action="store_true", help="keep all query parameters"
    )
    p.add_argument(
        "--allow-param",
        action="append",
        default=[],
        help="query parameter name to keep",
    )
    p.add_argument("--sitemap-seed", action="store_true", help="seed from sitemap(s)")

    # render
    p.add_argument(
        "--render-js", action="store_true", help="render with Playwright if installed"
    )
    p.add_argument(
        "--render-timeout-ms", type=int, default=10000, help="Playwright timeout ms"
    )
    p.add_argument(
        "--wait-until", type=str, default="networkidle", help="Playwright wait_until"
    )
    p.add_argument(
        "--no-render-block-media",
        action="store_true",
        help="do not block images/fonts during render",
    )

    # throttle
    p.add_argument("--global-rps", type=float, default=2.0, help="global requests/sec")
    p.add_argument(
        "--per-host-rps", type=float, default=1.0, help="per-host requests/sec"
    )
    p.add_argument("--burst", type=int, default=2, help="token-bucket burst")
    p.add_argument("--jitter", type=float, default=0.3, help="delay jitter 0..1")
    p.add_argument(
        "--max-backoff", type=float, default=60.0, help="max backoff seconds"
    )
    p.add_argument(
        "--no-auto-throttle", action="store_true", help="disable 429/503 backoff"
    )

    # resume / checkpoints
    p.add_argument(
        "--resume", action="store_true", help="resume from checkpoint if present"
    )
    p.add_argument(
        "--checkpoint-file", type=str, default=None, help="checkpoint JSON path"
    )
    p.add_argument(
        "--checkpoint-pages", type=int, default=50, help="checkpoint every N pages"
    )
    p.add_argument(
        "--checkpoint-seconds",
        type=float,
        default=120.0,
        help="checkpoint every N seconds",
    )
    p.add_argument(
        "--no-checkpoint-seen",
        action="store_true",
        help="when in-memory, do not dump 'seen' into checkpoint",
    )

    # persistent KV
    p.add_argument(
        "--kv-backend",
        type=str,
        choices=["none", "dbm"],
        default="none",
        help="persistent store for seen/asset_map",
    )
    p.add_argument(
        "--kv-dir",
        type=str,
        default=None,
        help="directory for KV files (default: site/meta)",
    )

    # rewriting
    p.add_argument(
        "--rewrite-js-urls",
        action="store_true",
        help="best-effort regex rewrite of URL-like strings in JS",
    )
    p.add_argument(
        "--rewrite-js-ast",
        action="store_true",
        help="AST-based JS URL literal rewriting (requires esprima)",
    )

    # WARC
    p.add_argument(
        "--warc",
        action="store_true",
        help="write WARC to meta/archive.warc.gz unless --warc-path provided",
    )
    p.add_argument("--warc-path", type=str, default=None, help="custom WARC path")
    p.add_argument("--no-warc-gzip", action="store_true", help="disable gzip for WARC")
    p.add_argument(
        "--warc-new", action="store_true", help="create new WARC (do not append)"
    )

    # auth / session
    p.add_argument(
        "--cookies",
        type=str,
        default=None,
        help="cookies.txt (Netscape/Mozilla format)",
    )
    p.add_argument(
        "--header",
        action="append",
        default=[],
        help="extra request header 'Name: value'",
    )
    p.add_argument("--auth-basic", type=str, default=None, help="basic auth user:pass")
    p.add_argument("--auth-bearer", type=str, default=None, help="bearer token")

    return p


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = build_arg_parser()
    preliminary, _ = parser.parse_known_args(argv)
    if preliminary.config:
        cfg = load_config_file(preliminary.config)
        if isinstance(cfg, dict):
            flat = dict(cfg)
            for g in (
                "crawl",
                "render",
                "throttle",
                "checkpoint",
                "kv",
                "rewrite",
                "warc",
                "auth",
                "general",
            ):
                if isinstance(cfg.get(g), dict):
                    flat.update(cfg[g])
            parser.set_defaults(**flat)
    args = parser.parse_args(argv)
    return args


def default_warc_path(base_url: str, output_root: Path) -> Path:
    return meta_dir(base_url, output_root) / "archive.warc.gz"


def main(argv: Optional[List[str]] = None) -> None:
    args = parse_args(argv)
    if urlparse(args.url).scheme not in {"http", "https"}:
        print("Invalid URL. Use http:// or https://")
        sys.exit(1)

    settings = Settings(
        timeout=args.timeout,
        workers=max(1, args.workers),
        delay=max(0.0, args.delay),
        same_origin_only=not args.external,
        respect_robots=args.respect_robots,
        max_bytes=max(1024, args.max_bytes),
        skip_js=args.skip_js,
        crawl=args.crawl,
        max_pages=max(1, args.max_pages),
        max_depth=max(0, args.max_depth),
        include=args.include,
        exclude=args.exclude,
        strip_params=not args.no_strip_params,
        allow_params=set(args.allow_param or []),
        sitemap_seed=args.sitemap_seed,
        render_js=args.render_js,
        render_timeout_ms=args.render_timeout_ms,
        wait_until=args.wait_until,
        render_block_media=not args.no_render_block_media,
        global_rps=max(0.01, args.global_rps),
        per_host_rps=max(0.01, args.per_host_rps),
        burst=max(1, args.burst),
        jitter=max(0.0, min(1.0, args.jitter)),
        max_backoff=max(1.0, args.max_backoff),
        auto_throttle=not args.no_auto_throttle,
        resume=args.resume,
        checkpoint_file=args.checkpoint_file,
        checkpoint_pages=max(0, args.checkpoint_pages),
        checkpoint_seconds=max(0.0, args.checkpoint_seconds),
        checkpoint_include_seen_when_mem=not args.no_checkpoint_seen,
        kv_backend=args.kv_backend,
        kv_dir=args.kv_dir,
        rewrite_js_urls=args.rewrite_js_urls,
        rewrite_js_ast=args.rewrite_js_ast,
        warc=args.warc or bool(args.warc_path),
        warc_path=args.warc_path,
        warc_gzip=not args.no_warc_gzip,
        warc_append=not args.warc_new,
        cookies_file=args.cookies,
        extra_headers=args.header or [],
        auth_basic=args.auth_basic,
        auth_bearer=args.auth_bearer,
    )

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(levelname)s: %(message)s",
    )

    archiver: Optional[WarcArchiver] = None
    if settings.warc:
        warc_p = (
            Path(settings.warc_path)
            if settings.warc_path
            else default_warc_path(args.url, Path(args.output_folder))
        )
        archiver = WarcArchiver(
            warc_p, gzip=settings.warc_gzip, append=settings.warc_append
        )
        if archiver.enabled:
            logging.info("WARC: %s", warc_p)

    print("Reminder: only clone content you own or have permission to copy.")
    try:
        if settings.crawl:
            mirror_site(args.url, args.output_folder, settings, archiver)
        else:
            clone_single_page(args.url, args.output_folder, settings, archiver)
    finally:
        try:
            if archiver:
                archiver.close()
        except Exception:
            pass


if __name__ == "__main__":
    main()
