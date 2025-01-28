#!/usr/bin/env python3
"""
File: gea_scholarship.py
Author: ro9naja
Email: ro9naja@gmail.com
Github: https://github.com/ro9naja
Description: scrape griffith.edu.au scolarships pages
"""

import argparse
import asyncio
from dataclasses import asdict, dataclass
from functools import partial
import io
import json
import random
import re
from typing import Callable, Final, Optional

from curl_cffi.requests import AsyncSession
from markdownify import markdownify
from rich import print
from selectolax.parser import HTMLParser
from tqdm.asyncio import tqdm


URL: Final = "https://www.griffith.edu.au/scholarships"
DLY: Final = (0.5, 2.5)
DRP: Final = [
    "form",
    "script",
    "style",
    "img",
    "video",
    "svg",
    "path",
    "g",
    "noscript",
    "meta",
    "header",
    "footer",
    "nav",
    "iframe",
]


@dataclass
class Scholarship:
    name: str
    url: str
    html: Optional[str] = None

    @classmethod
    def from_dict(cls, d):
        return cls(**d)

    def serialize(self):
        return asdict(self)


class Scholarships(list[Scholarship]):
    def __init__(self, l):
        super().__init__(Scholarship.from_dict(d) for d in l)

    def save_to_json(self, output: io.TextIOWrapper) -> None:
        print(f"[+] Saving {len(self)} scholarships to {output.name}")
        scholarships = [ss.serialize() for ss in self]
        with output as f:
            json.dump(scholarships, f)


async def get_parser(client: AsyncSession, url: str, print_code=False) -> HTMLParser:
    resp = await client.get(url)
    resp.raise_for_status()
    if print_code:
        print(resp.status_code)
    return HTMLParser(resp.text)


async def get_page(fetch: Callable, url: str) -> Scholarships:
    dox = await fetch(url, print_code=True)
    scholarships = dox.css("table tr td:first-child a")
    scholarships = Scholarships(
        [
            {
                "name": s.text().strip(),
                "url": s.attrs.get("href").strip("/"),
            }
            for s in scholarships
            if s.text().strip()
        ]
    )
    return scholarships


async def get_scholarship(fetch: Callable, datum: Scholarship, retries=4) -> None:
    await asyncio.sleep(random.uniform(*DLY))
    try:
        dox = await fetch(datum.url)
    except Exception as e:
        if retries > 0:
            await asyncio.sleep(random.uniform(*DLY))
            await get_scholarship(fetch, datum, retries - 1)
            return
        else:
            raise Exception(f"{e}: {datum.url}")
    body = dox.body
    body.strip_tags(DRP)
    body = re.sub(r"\\n\s+\\n", "\\n", body.html)
    datum.html = markdownify(body)


async def main(config, output: io.TextIOWrapper) -> None:
    """main execution"""
    random.seed(0)
    with config as f:
        config = json.load(f)
    async with (
        AsyncSession(
            headers=config.get("headers", {}),
            cookies=config.get("cookies", {}),
            impersonate="chrome124",
        ) as client,
        asyncio.Semaphore(5),
    ):
        fetch = partial(get_parser, client)
        print(f"[+] Retrieving {URL} ... ", end="")
        scholarships = await get_page(fetch, URL)
        print(f"[+] Found {len(scholarships)} scholarships")
        while len(runs := [ss for ss in scholarships if ss.html is None]) > 0:
            results = []
            random.shuffle(runs)
            await asyncio.sleep(random.uniform(*DLY))
            for r in tqdm.as_completed([get_scholarship(fetch, t) for t in runs]):
                try:
                    results.append(await r)
                except Exception as e:
                    results.append(e)
            results = list(filter(None, results))
            if len(results) > 0:
                print(results)
        scholarships.save_to_json(output)


def parse_args() -> argparse.Namespace:
    """parse arguments"""
    parser = argparse.ArgumentParser(description="scrape griffith.edu.au")
    parser.add_argument(
        "-c",
        "--config",
        type=argparse.FileType("r"),
        default="cookies_headers.json",
        help="cookies and headers json",
    )
    parser.add_argument(
        "-o",
        "--output",
        type=argparse.FileType("w"),
        default="scholarships.json",
        help="output json",
    )
    return parser.parse_args()


if __name__ == "__main__":
    asyncio.run(main(**vars(parse_args())))
