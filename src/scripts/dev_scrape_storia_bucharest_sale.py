# src/scripts/dev_scrape_storia_bucharest_sale.py
from __future__ import annotations

import random
import time
from pathlib import Path

import pandas as pd
import requests
from bs4 import BeautifulSoup
import urllib.parse


BASE_URL = "https://www.storia.ro/ro/rezultate/vanzare/apartament/bucuresti"

# High cap; stops automatically when pages end/repeat.
MAX_PAGES = 2000

SLEEP_RANGE = (0.8, 2.0)
CHECKPOINT_EVERY = 25

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "ro-RO,ro;q=0.9,en-US;q=0.8,en;q=0.7",
    "Referer": "https://www.google.com/",
}


def parse_page(page_url: str, html: str) -> list[dict]:
    soup = BeautifulSoup(html, "html.parser")

    cards = soup.select("li > article")
    listings: list[dict] = []

    for card in cards:
        title_el = card.select_one("[data-cy='listing-item-title']")
        title = title_el.get_text(" ", strip=True) if title_el else ""

        price_el = card.select_one("[data-sentry-element='MainPrice'], [class*='price']")
        price = price_el.get_text(" ", strip=True) if price_el else ""

        loc_el = card.select_one("[data-sentry-component='Address']")
        location = loc_el.get_text(" ", strip=True) if loc_el else ""

        a = card.select_one("[data-cy='listing-item-link']")
        link = urllib.parse.urljoin(page_url, a["href"]) if a and a.get("href") else ""

        dl = card.select_one("dl")
        rooms, surface, floor, stare = None, None, None, None
        if dl:
            dt_tags = dl.select("dt")
            dd_tags = dl.select("dd")
            for dt, dd in zip(dt_tags, dd_tags):
                label = dt.get_text(" ", strip=True).lower()
                value = dd.get_text(" ", strip=True)
                if "camere" in label:
                    rooms = value
                elif "metru" in label or "m²" in value:
                    surface = value
                elif "etaj" in label:
                    floor = value
                elif "stare" in label:
                    stare = value

        listings.append(
            {
                "title": title,
                "price": price,
                "location": location,
                "rooms": rooms,
                "surface": surface,
                "floor": floor,
                "stare": stare,
                "url": link,
            }
        )

    return listings


def main():
    out_dir = Path("rezultate_imobiliare") / "raw"
    out_dir.mkdir(parents=True, exist_ok=True)

    out_path = out_dir / "storia_bucuresti_apartamente_vanzare.csv"
    partial_path = out_dir / "storia_bucuresti_apartamente_vanzare__partial.csv"

    all_data: list[dict] = []
    seen_urls: set[str] = set()

    print("Base:", BASE_URL)
    print(f"Will scrape up to {MAX_PAGES} pages, stopping automatically when pages end/repeat.\n")

    for page in range(1, MAX_PAGES + 1):
        url = BASE_URL if page == 1 else f"{BASE_URL}?page={page}"
        print(f"Scraping page {page}/{MAX_PAGES}: {url}")

        resp = requests.get(url, headers=HEADERS, timeout=30)
        if resp.status_code != 200:
            print(f"⚠️ Page {page} failed (HTTP {resp.status_code}) — stopping.")
            break

        listings = parse_page(url, resp.text)
        print(f"✅ Extracted {len(listings)} listings")

        if len(listings) == 0:
            print("No listings found — stopping.")
            break

        page_urls = [x["url"] for x in listings if x.get("url")]
        unique_new_urls = [u for u in page_urls if u not in seen_urls]

        if page > 1 and len(unique_new_urls) == 0:
            print("No new listing URLs on this page (likely end or repeated content) — stopping.")
            break

        for u in unique_new_urls:
            seen_urls.add(u)

        all_data.extend(listings)

        if page % CHECKPOINT_EVERY == 0:
            df_partial = pd.DataFrame(all_data)
            if "url" in df_partial.columns:
                df_partial = df_partial.drop_duplicates(subset=["url"], keep="first")
            df_partial.to_csv(partial_path, index=False, encoding="utf-8-sig")
            print(f"💾 checkpoint saved: {partial_path} (unique={len(df_partial)})")

        time.sleep(random.uniform(*SLEEP_RANGE))

    df = pd.DataFrame(all_data)
    if "url" in df.columns:
        df = df.drop_duplicates(subset=["url"], keep="first")
    df.to_csv(out_path, index=False, encoding="utf-8-sig")

    print(f"\n✅ TOTAL unique listings scraped: {len(df)}")
    print(f"💾 Saved: {out_path}")
    print(df.head(10).to_string(index=False))


if __name__ == "__main__":
    main()
