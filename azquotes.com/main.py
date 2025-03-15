#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue May 16 14:47:34 2023
Description: scrapes azquotes.com
@author: ro9naja
"""

import dataclasses
import json
import re
from functools import partial
from random import randint
from time import sleep
from typing import Any

import parsel
from httpx import Client
from rich import print
from rich.progress import Progress

BASE_URL = "https://www.azquotes.com"
URL = "/quotes/topics/around-the-corner.html"
USER_AGENT = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"


@dataclasses.dataclass
class Quote:
    quote: str
    author: str


def fetch(url: str, client: Client) -> parsel.Selector:
    """
    Scrapes the specified URL and returns a parsel.Selector object.
    """
    response = client.get(url)
    if response.status_code == 200:
        return parsel.Selector(response.text)
    else:
        print(f"Request failed with status code: {response.status_code}")
        return None


def scrape_quotes(sel: parsel.Selector) -> tuple[list[Quote], [str | None]]:
    """
    Extracts quotes from a parsel.Selector object.
    """
    quotes = []
    quote_divs = sel.css("div.wrap-block")
    for quote_div in quote_divs:
        quote = quote_div.css("a.title::text").get()
        author = quote_div.css("div.author a::text").get()
        quotes.append(Quote(quote=quote, author=author))
    next_page_uri = sel.css(".next a::attr(href)").get()
    return quotes, next_page_uri


def get_quotes(url: str = URL) -> list[Quote]:
    """
    Retrieves quotes from the specified URL.
    """
    quotes = []
    headers = {"User-Agent": USER_AGENT}
    with Client(headers=headers, base_url=BASE_URL, http2=True) as client:
        fetcher = partial(fetch, client=client)
        sel = fetcher(url)
        pages_str = sel.css(".pager > span:first-child::text").get()
        pages_str = re.search(r"of\s+(\d{1,})", pages_str).group(1)
        pages = int(pages_str)
        with Progress() as progress:
            task = progress.add_task("[red]Scraping...", total=pages)
            new_quotes, next_page_uri = scrape_quotes(sel)
            quotes.extend(new_quotes)
            progress.update(task, advance=1)
            while next_page_uri:
                sleep(randint(1, 3))
                sel = fetcher(next_page_uri)
                new_quotes, next_page_uri = scrape_quotes(sel)
                quotes.extend(new_quotes)
                progress.update(task, advance=1)
    return quotes


def encode_value(x: Any) -> Any:
    if dataclasses.is_dataclass(x):
        return dataclasses.asdict(x)
    return x


def main():
    """
    Main function.
    """
    quotes = get_quotes()
    with open("quotes.json", "w") as f:
        json.dump(quotes, f, indent=4, default=encode_value)


if __name__ == "__main__":
    main()
