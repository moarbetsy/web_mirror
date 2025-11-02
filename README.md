
***

# Web Mirror & Single-Page Cloner

A versatile tool to clone a single webpage or mirror a portion of a website to a local directory. It intelligently rewrites all HTML, CSS, and JavaScript references to point to the local copies, organizing assets by type into a clean folder structure.

Key features include polite throttling to avoid overwhelming servers, optional JavaScript rendering with Playwright for dynamic content, resumable crawling sessions, and optional WARC archiving for long-term preservation.

> **Disclaimer:** This tool should only be used to clone content that you own or have explicit permission to copy. Always respect the website's terms of service and applicable laws.

## Features

- **Dual Modes:**
    - **Clone Mode:** Downloads a single page and all its necessary assets.
    - **Mirror Mode:** Crawls a website by following links up to a specified depth and page limit.

- **Clean Output Structure:**
    - `pages/` for all HTML files.
    - `assets/{img, css, js, font, media, data}/` for first-party assets.
    - `assets/vendor/<hostname>/...` for any third-party assets.
    - `meta/` for a `manifest.json` file and an optional `archive.warc.gz`.

- **Robust & Resilient Fetching:**
    - Automatic retries with exponential backoff.
    - Token-bucket throttling on a per-host and global basis.
    - Respects `Retry-After` headers.
    - Optional support for `robots.txt` and `crawl-delay` directives.

- **Intelligent URL Handling:**
    - Normalizes all links for consistency.
    - By default, strips common tracking parameters (e.g., `utm_*`, `gclid`, `fbclid`).
    - Provides an allow-list to preserve specific query parameters.

- **Advanced Content Rendering:**
    - Standard fetching via HTTP requests by default.
    - Optional client-side rendering with Playwright (`--render-js`) to capture dynamically loaded content.

- **Comprehensive Asset Rewriting:**
    - Rewrites paths in HTML (`src`, `href`, `srcset`), inline CSS (`url()`), and CSS `@import` statements.
    - Optional rewriting of URLs found within JavaScript files.

- **Crawl Resumption:**
    - Saves progress to a checkpoint file, allowing you to resume an interrupted crawl with the `--resume` flag.

- **Flexible Authentication:**
    - Supports cookies files, custom headers, and both Basic and Bearer token authentication.

## Requirements

- Python 3.9+
- **Core Dependencies:** `requests`, `beautifulsoup4` (with `lxml` recommended for performance).
- **Optional Features:**
    - `playwright` for JavaScript rendering.
    - `warcio` for WARC archiving.
    - `esprima` for advanced JavaScript rewriting.
    - `PyYAML` and `tomli` (for Python ≤3.10) for config file support.

### Installation

1.  **Install the core libraries:**
    ```bash
    pip install -U requests beautifulsoup4 lxml
    ```

2.  **Install optional extras as needed:**
    ```bash
    pip install -U playwright warcio esprima PyYAML tomli
    ```

3.  **If using Playwright, install its browser binaries:**
    ```bash
    playwright install
    ```

## Quick Start

The two positional arguments are always `URL` and `OUTPUT_DIR`.

**1. Clone a single page (first-party assets only):**
```bash
python web_mirror.py https://example.com out/
```

**2. Clone a page with all its third-party assets:**
```bash
python web_mirror.py --external https://example.com out/
```

**3. Mirror a site up to 500 pages within a depth of 2:**
```bash
python web_mirror.py --crawl --max-pages 500 --max-depth 2 https://example.com out/
```

**4. Mirror a site using its sitemap to find pages and render JavaScript:**
```bash
python web_mirror.py --crawl --sitemap-seed --render-js https://example.com out/
```

**5. Resume a previously interrupted crawl:**
```bash
python web_mirror.py --crawl --resume https://example.com out/
```

**6. Keep specific query parameters while stripping others:**
```bash
python web_mirror.py --crawl --allow-param id --allow-param page https://example.com out/
```

**7. Create a WARC archive while cloning:**
```bash
python web_mirror.py --warc https://example.com out/
```

**8. Use a cookies file, add a custom header, and set rate limits:**
```bash
python web_mirror.py --cookies cookies.txt --header "X-Requested-By: mirror" \
  --per-host-rps 0.5 --global-rps 1.0 https://example.com out/
```

## Command-Line Interface

The script is invoked as follows:

```
python web_mirror.py [--config FILE] [FLAGS...] URL OUTPUT_DIR
```

#### **General**
*   `--external`: Download third-party assets.
*   `--respect-robots`: Respect the site's `robots.txt` rules.
*   `--delay SECONDS`: Wait time between requests.
*   `--timeout SECONDS`: Request timeout.
*   `--workers N`: Number of concurrent download workers.
*   `--max-bytes N`: Maximum size for downloaded files.
*   `--skip-js`: Do not download JavaScript files.
*   `--verbose`: Enable detailed logging.

#### **Crawl**
*   `--crawl`: Enable mirror mode.
*   `--max-pages N`: Maximum number of pages to crawl.
*   `--max-depth N`: Maximum link depth to follow.
*   `--include REGEX`: Regex pattern for URLs to include.
*   `--exclude REGEX`: Regex pattern for URLs to exclude.
*   `--no-strip-params`: Disable stripping of tracking URL parameters.
*   `--allow-param NAME`: Preserve a specific URL parameter.
*   `--sitemap-seed`: Use the site's sitemap as the initial source of URLs.

#### **Render**
*   `--render-js`: Use Playwright to render JavaScript.
*   `--render-timeout-ms N`: Timeout for the rendering process.
*   `--wait-until STR`: Playwright `waitUntil` event (`load`, `domcontentloaded`, `networkidle`).
*   `--no-render-block-media`: Allow Playwright to load images and other media.

#### **Throttle**
*   `--global-rps F`: Global requests per second.
*   `--per-host-rps F`: Requests per second for a single host.
*   `--burst N`: Number of requests allowed in a burst.
*   `--jitter F`: Random delay factor for requests.
*   `--max-backoff SECONDS`: Maximum delay for retries.
*   `--no-auto-throttle`: Disable automatic throttling based on `Retry-After` headers.

#### **Resume / Checkpoints**
*   `--resume`: Resume from the last checkpoint.
*   `--checkpoint-file PATH`: Path for the checkpoint file.
*   `--checkpoint-pages N`: Save a checkpoint every N pages.
*   `--checkpoint-seconds S`: Save a checkpoint every S seconds.

#### **Rewriting**
*   `--rewrite-js-urls`: Rewrite URLs found in JavaScript files using regex.
*   `--rewrite-js-ast`: Rewrite URLs by parsing the JavaScript AST (requires `esprima`).

#### **WARC**
*   `--warc`: Enable writing to a WARC file.
*   `--warc-path PATH`: Specify the output path for the WARC file.
*   `--no-warc-gzip`: Disable gzip compression for the WARC file.
*   `--warc-new`: Start a new WARC file, ignoring any existing one.

#### **Authentication**
*   `--cookies PATH`: Path to a Netscape-format cookies file.
*   `--header "Name: value"`: Add a custom HTTP header to requests.
*   `--auth-basic "user:pass"`: Use HTTP Basic authentication.
*   `--auth-bearer TOKEN`: Use Bearer token authentication.

## Configuration Files

You can manage settings using a TOML or YAML file via the `--config` flag. The keys in the file correspond to the CLI argument names.

#### **TOML Example (`config.toml`)**
```toml
[crawl]
crawl = true
max_pages = 500
max_depth = 2

[render]
render_js = true
wait_until = "networkidle"

[throttle]
global_rps = 1.5
per_host_rps = 1.0

[auth]
cookies = "cookies.txt"
header = ["X-Forwarded-Proto: https"]
```

#### **YAML Example (`config.yaml`)**
```yaml
crawl:
  crawl: true
  max_pages: 250
  max_depth: 1
render:
  render_js: false
throttle:
  global_rps: 2.0
rewrite:
  rewrite_js_urls: true
```

To use a configuration file, run:
```bash
python web_mirror.py --config config.toml https://example.com out/
```

## Output Layout

The output directory is organized as follows:

```
out/
└── example.com/
    ├── pages/
    │   └── index.html
    ├── assets/
    │   ├── css/
    │   ├── img/
    │   ├── js/
    │   ├── font/
    │   └── media/
    ├── assets/
    │   └── vendor/
    │       └── another-domain.com/
    └── meta/
        ├── manifest.json
        ├── checkpoint.json  # (if enabled)
        └── archive.warc.gz  # (if enabled)
```
- Filenames include a short hash of the original URL to prevent naming conflicts.
- The `manifest.json` file contains metadata about the crawl session.

#### **Manifest Example**
```json
{
  "site": "https://example.com",
  "created_utc": "2025-11-02T12:00:00Z",
  "pages": ["pages/index.html"],
  "assets": ["assets/css/style_a1b2c3.css"],
  "layout": {
    "pages_dir": "pages/",
    "assets_dir": "assets/",
    "vendor_dir": "assets/vendor/"
  }
}
```

## Programmatic Use

The core functions can be imported and used in other Python scripts.

```python
from web_mirror import clone_single_page, mirror_site, Settings

# Define settings for the operation
settings = Settings()

# Clone a single page
clone_single_page(
    url="https://example.com",
    output_folder="out",
    settings=settings,
    loop=None # asyncio loop, can be None
)
```

## Security Considerations

- **Permissions:** Only mirror content you are legally permitted to copy.
- **Throttling:** Use the throttling options to be respectful to web servers and avoid being blocked.
- **Credentials:** Keep files containing cookies and authentication tokens secure and do not commit them to public repositories.
```

Notes: integrates absolute‑path asset mapping, consistent robots UA handling, Playwright UA/locale, and timezone‑aware manifest timestamp. Derived from your `web_mirror.py` and `Readme.md`. :contentReference[oaicite:2]{index=2} :contentReference[oaicite:3]{index=3}
```
