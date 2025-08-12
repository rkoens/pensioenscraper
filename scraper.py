#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Pension Scraper
- PFZW (JSON links discovered on HTML page)
- PMT (inline HTML tables)
- PME (paged HTML tables with ?page=N)
Stores new versions only (hash-based) in data/<site>/<category>.
Tracks seen artifacts in seen.sqlite3
"""

import os
import re
import time
import hashlib
import sqlite3
import argparse
from datetime import datetime
from urllib.parse import urljoin, urlparse, urlencode

import requests
import pandas as pd
from bs4 import BeautifulSoup
from io import StringIO

# ------------------ Config ------------------

DATA_DIR = os.environ.get("PS_DATA_DIR", "data")
DB_PATH = os.environ.get("PS_DB_PATH", "seen.sqlite3")

SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": "FTM-pension-scraper/1.0 (+contact: newsroom)",
    "Accept": "text/html,application/json"
})

SITES = [
    {
        "name": "pfzw_transparantielijsten",
        "mode": "json_links",
        "start_url": "https://www.pfzw.nl/over-pfzw/beleggen-voor-een-goed-pensioen/soorten-beleggingen.html",
        "must_contain": ["transparantielijsten", ".json"],
        "category_regex": r"/(?P<cat>Aandelen|Obligaties|Vastgoed|Infrastructuur|Alternatieven)_\d{4}Q[1-4]\.json$",
        "date_regexes": [
            (r"(?P<year>20\d{2})Q(?P<q>[1-4])", "quarter")
        ],
    },
    {
        "name": "pmt_aandelen_obligaties",
        "mode": "html_tables",
        "start_url": "https://www.pmt.nl/over-pmt/zo-beleggen-we/waar-beleggen-we-in/aandelen-en-obligaties",
        "must_include_headings": ["Aandelen", "Obligaties"],
    },
    {
        "name": "pme_waarin_beleggen_we",
        "mode": "paged_html_tables",
        "start_url": "https://www.pmepensioen.nl/beleggen/waarin-beleggen-we",
        "categories": {
            "Aandelen_en_Bedrijfsobligaties": {"label_contains": ["Aandelen en bedrijfsobligaties"]},
            "Staatsobligaties": {"label_contains": ["Staatsobligaties"]}
        }
    }
]

# ------------------ Storage ------------------

def init_db():
    con = sqlite3.connect(DB_PATH)
    con.execute("""CREATE TABLE IF NOT EXISTS seen(
        url TEXT PRIMARY KEY,
        etag TEXT,
        last_modified TEXT,
        content_hash TEXT,
        fetched_at TEXT
    )""")
    con.commit()
    return con

def content_hash_bytes(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()

def dataframe_hash(df: pd.DataFrame) -> str:
    payload = df.to_csv(index=False).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()

def safe_filename(s: str) -> str:
    return re.sub(r"[^A-Za-z0-9._-]+", "_", s)

def already_seen(con, url):
    cur = con.execute("SELECT etag,last_modified,content_hash FROM seen WHERE url=?", (url,))
    return cur.fetchone()

def upsert_seen(con, url, etag, last_modified, c_hash):
    con.execute("""INSERT INTO seen(url, etag, last_modified, content_hash, fetched_at)
                   VALUES(?,?,?,?,?)
                   ON CONFLICT(url) DO UPDATE SET etag=excluded.etag,
                                                 last_modified=excluded.last_modified,
                                                 content_hash=excluded.content_hash,
                                                 fetched_at=excluded.fetched_at""",
                (url, etag, last_modified, c_hash, datetime.utcnow().isoformat()))
    con.commit()

def save_blob(site_name, category, url, blob, dry_run=False):
    ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    base = os.path.basename(urlparse(url).path) or "file.bin"
    outdir = os.path.join(DATA_DIR, site_name, category)
    os.makedirs(outdir, exist_ok=True)
    outpath = os.path.join(outdir, f"{ts}__{safe_filename(base)}")
    if not dry_run:
        with open(outpath, "wb") as f:
            f.write(blob)
    return outpath

def save_table(site_name, category, df, dry_run=False):
    ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    outdir = os.path.join(DATA_DIR, site_name, category)
    os.makedirs(outdir, exist_ok=True)
    outpath = os.path.join(outdir, f"{ts}__data.csv")
    if not dry_run:
        df.to_csv(outpath, index=False)
    return outpath

# ------------------ HTTP helpers ------------------

def get_html(url):
    r = SESSION.get(url, timeout=30)
    r.raise_for_status()
    return r.text, r.url

def head_meta(url):
    try:
        r = SESSION.head(url, timeout=20, allow_redirects=True)
        return r.headers.get("ETag"), r.headers.get("Last-Modified")
    except requests.RequestException:
        return None, None

def fetch_if_new(con, url, dry_run=False):
    etag, last_mod = head_meta(url)
    prev = already_seen(con, url)

    headers = {}
    if etag:
        headers["If-None-Match"] = etag
    if last_mod:
        headers["If-Modified-Since"] = last_mod

    try:
        r = SESSION.get(url, headers=headers, timeout=60)
        if r.status_code == 304:
            return None, None, None  # unchanged
        r.raise_for_status()
    except requests.RequestException as e:
        print(f"[WARN] GET failed {url}: {e}")
        return None, None, None

    blob = r.content
    chash = content_hash_bytes(blob)

    if prev and prev[2] == chash:
        return None, None, None

    if not dry_run:
        upsert_seen(con, url, r.headers.get("ETag"), r.headers.get("Last-Modified"), chash)
    return blob, r.headers.get("ETag"), r.headers.get("Last-Modified")

# ------------------ PFZW (json_links) ------------------

def extract_json_urls(html, base_url):
    soup = BeautifulSoup(html, "html.parser")
    urls = set()

    for tag, attr in (("a", "href"), ("link", "href"), ("script", "src")):
        for el in soup.find_all(tag):
            href = el.get(attr)
            if href:
                u = urljoin(base_url, href)
                if u.lower().endswith(".json"):
                    urls.add(u)

    for m in re.finditer(r'https?://[^\s"\'<>]+\.json', html):
        urls.add(m.group(0))

    return sorted(urls)

def match_filters(urls, must_contain):
    out = []
    for u in urls:
        if all(s.lower() in u.lower() for s in must_contain):
            out.append(u)
    return out

def parse_date_from_name(url, date_regexes):
    fname = os.path.basename(urlparse(url).path)
    for rx, kind in date_regexes:
        m = re.search(rx, fname)
        if m:
            if kind == "quarter":
                y = int(m.group("year"))
                q = int(m.group("q"))
                return (y, q, None)
            elif kind == "ymd":
                y, mth, d = int(m.group("year")), int(m.group("month")), int(m.group("day"))
                return (y, None, datetime(y, mth, d))
    return None

def category_of(url, category_regex):
    m = re.search(category_regex, url, flags=re.I)
    return m.group("cat") if m else "uncategorized"

def choose_latest_per_category(urls, site):
    buckets = {}
    for u in urls:
        cat = category_of(u, site["category_regex"])
        dt_tuple = parse_date_from_name(u, site["date_regexes"])
        buckets.setdefault(cat, []).append((u, dt_tuple))

    chosen = {}
    for cat, items in buckets.items():
        with_dates = [(u, dt) for u, dt in items if dt]
        if with_dates:
            best = sorted(with_dates, key=lambda x: (x[1][0], x[1][1] or 0, x[1][2] or datetime.min))[-1][0]
            chosen[cat] = best
        else:
            items_with_lm = []
            for u, _ in items:
                et, lm = head_meta(u)
                ts = 0
                if lm:
                    try:
                        ts = time.mktime(datetime.strptime(lm, "%a, %d %b %Y %H:%M:%S %Z").timetuple())
                    except Exception:
                        ts = 0
                items_with_lm.append((u, ts))
            best = sorted(items_with_lm, key=lambda x: x[1])[-1][0]
            chosen[cat] = best
    return chosen

def scrape_site_json_links(site, con, dry_run=False, only_categories=None):
    html, final_url = get_html(site["start_url"])
    all_json = extract_json_urls(html, final_url)
    filtered = match_filters(all_json, site["must_contain"])
    if not filtered:
        print(f"[WARN] No JSON links found for {site['name']}")
        return

    latest_per_cat = choose_latest_per_category(filtered, site)

    for category, url in latest_per_cat.items():
        if only_categories and category not in only_categories:
            continue
        blob, et, lm = fetch_if_new(con, url, dry_run=dry_run)
        if blob:
            path = save_blob(site["name"], category, url, blob, dry_run=dry_run)
            print(f"[NEW ] {site['name']}/{category}: {url} -> {path}")
        else:
            print(f"[SKIP] {site['name']}/{category}: no change")

# ------------------ PMT (html_tables) ------------------

def nearest_heading_label(table):
    cap = table.find("caption")
    if cap and cap.get_text(strip=True):
        return cap.get_text(strip=True)
    for attr in ("aria-label", "summary"):
        if table.has_attr(attr) and table[attr].strip():
            return table[attr].strip()
    prev = table.find_previous(lambda tag: tag.name in ("h2", "h3", "h4"))
    if prev:
        return prev.get_text(strip=True)
    return "table"

def normalize_df(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [str(c).strip() for c in df.columns]

    def _trim(x):
        return x.strip() if isinstance(x, str) else x

    try:
        df = df.map(_trim)  # pandas ≥ 2.2
    except AttributeError:
        df = df.applymap(_trim)  # fallback

    df = df.replace(r"^\s*$", pd.NA, regex=True)
    df = df.dropna(how="all")
    df = df.dropna(axis=1, how="all")
    return df

def html_tables_extract(html, url, must_include_headings=None):
    soup = BeautifulSoup(html, "html.parser")
    tables = soup.find_all("table")
    results = []

    for i, tbl in enumerate(tables, 1):
        label = nearest_heading_label(tbl)
        if must_include_headings:
            label_lc = label.lower()
            if not any(h.lower() in label_lc for h in must_include_headings):
                continue
        try:
            df_list = pd.read_html(StringIO(str(tbl)))
        except ValueError:
            continue
        if not df_list:
            continue

        df = normalize_df(df_list[0])
        if df.empty:
            continue

        category = f"{label or 'table'}"
        results.append((category, df))

    return results

def scrape_site_html_tables(site, con, dry_run=False):
    """Scrape a page containing inline HTML tables (PMT-style)."""
    html, final_url = get_html(site["start_url"])
    tables = html_tables_extract(html, final_url, site.get("must_include_headings"))

    if not tables:
        print(f"[WARN] No tables found on {site['name']}")
        return

    for category, df in tables:
        synthetic_url = f"{final_url}#{category}"
        h = dataframe_hash(df)
        prev = already_seen(con, synthetic_url)

        if prev and prev[2] == h:
            print(f"[SKIP] {site['name']}/{category}: no change")
            continue

        if not dry_run:
            upsert_seen(con, synthetic_url, None, None, h)
        path = save_table(site["name"], category, df, dry_run=dry_run)
        print(f"[NEW ] {site['name']}/{category} -> {path}")

# ------------------ PME (paged_html_tables) ------------------

PAGERE = re.compile(r"Pagina\s+(?P<cur>\d+)\s+van\s+(?P<total>\d+)", re.I)

def parse_total_pages(soup):
    for el in soup.find_all(string=PAGERE):
        m = PAGERE.search(el)
        if m:
            return int(m.group("total"))
    return 1

def extract_tables_generic(html):
    soup = BeautifulSoup(html, "html.parser")
    tables = []
    for tbl in soup.find_all("table"):
        head = tbl.find_previous(lambda t: t.name in ("h2", "h3", "h4"))
        label = head.get_text(strip=True) if head else "table"
        try:
            df = pd.read_html(StringIO(str(tbl)))[0]
        except ValueError:
            continue
        if df.empty:
            continue
        df = normalize_df(df)
        tables.append((label, df))
    return tables, soup

def euro_to_number(s):
    if s is None:
        return None
    s = str(s).replace("\xa0", " ").replace("€", "").strip()
    s = s.replace(".", "")
    if "," in s and s.rsplit(",", 1)[1].isdigit():
        whole, frac = s.rsplit(",", 1)
        whole = whole.replace(" ", "").replace(".", "")
        try:
            return float(f"{whole}.{frac}")
        except ValueError:
            return None
    s2 = s.replace(" ", "")
    return int(s2) if s2.isdigit() else None

def scrape_pme_paged(site, con, dry_run=False):
    start_url = site["start_url"]
    html, final_url = get_html(start_url)
    tables, soup = extract_tables_generic(html)
    total = parse_total_pages(soup)

    holdings_list, sovs_list = [], []

    for p in range(total):
        url = f"{final_url}?{urlencode({'page': p})}" if p else final_url
        html, _ = get_html(url)
        tables, _ = extract_tables_generic(html)
        for label, df in tables:
            label_lc = label.lower()
            if "aandelen en bedrijfsobligaties" in label_lc:
                holdings_list.append(df)
            elif "staatsobligaties" in label_lc:
                sovs_list.append(df)

    def tidy(df):
        if df is None or df.empty:
            return pd.DataFrame()
        df = df.copy()
        if "Bedrag in €" in df.columns:
            df["Bedrag in € (num)"] = df["Bedrag in €"].map(euro_to_number)
        return df

    holdings = tidy(pd.concat(holdings_list, ignore_index=True) if holdings_list else pd.DataFrame())
    sovs = tidy(pd.concat(sovs_list, ignore_index=True) if sovs_list else pd.DataFrame())

    for cat, df in [("Aandelen_en_Bedrijfsobligaties", holdings), ("Staatsobligaties", sovs)]:
        if df.empty:
            print(f"[WARN] {site['name']}/{cat}: empty result")
            continue
        key = f"{final_url}#{cat}"
        h = dataframe_hash(df)
        prev = already_seen(con, key)
        if prev and prev[2] == h:
            print(f"[SKIP] {site['name']}/{cat}: no change")
            continue
        if not dry_run:
            upsert_seen(con, key, None, None, h)
        path = save_table(site["name"], cat, df, dry_run=dry_run)
        print(f"[NEW ] {site['name']}/{cat} -> {path}")

# ------------------ Runner ------------------

def run(selected_site=None, dry_run=False, only_categories=None):
    con = init_db()
    for site in SITES:
        if selected_site and site["name"] != selected_site:
            continue
        print(f"[INFO] Site: {site['name']}")
        mode = site.get("mode", "json_links")
        try:
            if mode == "json_links":
                scrape_site_json_links(site, con, dry_run=dry_run, only_categories=only_categories)
            elif mode == "html_tables":
                scrape_site_html_tables(site, con, dry_run=dry_run)
            elif mode == "paged_html_tables":
                scrape_pme_paged(site, con, dry_run=dry_run)
            else:
                print(f"[WARN] Unknown mode {mode} for {site['name']}")
        except Exception as e:
            print(f"[ERROR] {site['name']}: {e}")

if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Pension holdings scraper")
    ap.add_argument("--site", help="Only run a single site by name", default=None)
    ap.add_argument("--dry-run", action="store_true", help="Do not write files or DB updates")
    ap.add_argument("--only-categories", nargs="*", help="Limit to specific categories (json_links mode)")
    args = ap.parse_args()

    run(selected_site=args.site, dry_run=args.dry_run, only_categories=args.only_categories)
