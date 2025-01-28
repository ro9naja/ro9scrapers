#!/usr/bin/env python3
"""
File: sec_02.py
Author: ro9naja
Email: ro9naja@email.com
Github: https://github.com/ro9naja
Description: scrapes SEC documents for Company Information and Item 1C
"""

import argparse
import asyncio
from dataclasses import dataclass
from html import unescape
import io
import json
import re
from typing import Optional

from curl_cffi.requests import AsyncSession
from markdownify import markdownify
from parsel import Selector, SelectorList
from rich import print


@dataclass
class Company:
    EntityFileNumber: str
    EntityRegistrantName: str
    EntityIncorporationStateCountryCode: str
    EntityTaxIdentificationNumber: Optional[str] = None


@dataclass
class SECCompany:
    Companies: list[Company]
    EntityAddressAddressLines: list[str]
    EntityAddressCityOrTown: str
    EntityAddressPostalZipCode: str
    CityAreaCode: str
    LocalPhoneNumber: str
    TradingSymbol: str
    SecurityExchangeName: str
    item_1c: Optional[str] = None
    EntityAddressStateOrProvince: Optional[str] = None
    EntityAddressCountry: Optional[str] = None

    @property
    def name(self) -> str:
        return "\n".join(c.EntityRegistrantName for c in self.Companies)

    @property
    def address(self) -> str:
        addcoma = "" if self.EntityAddressCityOrTown.endswith(",") else ","
        return "{}\n{}{} {}\n{}".format(
            "\n".join(self.EntityAddressAddressLines),
            self.EntityAddressCityOrTown,
            addcoma,
            self.EntityAddressStateOrProvince or self.EntityAddressCountry,
            self.EntityAddressPostalZipCode,
        )

    @property
    def telefon(self) -> str:
        return f"({self.CityAreaCode}) {self.LocalPhoneNumber}"


class EDGARSearch:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session
        self._current_url = None

    async def _get(self, url: str) -> Selector:
        headers = {"Referer": self._current_url or ""} if self._current_url else {}
        res = await self._session.get(url, headers=headers)
        res.raise_for_status()
        self._current_url = res.url
        sel = Selector(text=res.text)
        sel.root.make_links_absolute(res.url)
        return sel

    def _parse_entity(self, sel: Selector) -> SECCompany:
        comk = (
            "EntityFileNumber",
            "EntityRegistrantName",
            "EntityIncorporationStateCountryCode",
            "EntityTaxIdentificationNumber",
        )

        eaal = "EntityAddressAddressLine"

        seck = (
            "EntityAddressCityOrTown",
            "EntityAddressStateOrProvince",
            "EntityAddressCountry",
            "EntityAddressPostalZipCode",
            "CityAreaCode",
            "LocalPhoneNumber",
            "TradingSymbol",
            "SecurityExchangeName",
        )

        def _clean(x):
            return k if (k := x.attrib.get("name")) is None else k.replace("dei:", "")

        def _continuedat(node) -> str:
            text = node.xpath(".//text()").get()
            if "continuedat" in node.attrib:
                _id = node.attrib.get("continuedat")
                cdat = sel.css(f'[id="{_id}"]::text').get()
                text = f"{text}{cdat}"
            return re.sub(r"\s+", " ", text)

        companies_info = list(
            filter(None, [sel.css(f'[name="dei:{dei}"]') for dei in comk])
        )
        companies_info = [
            {
                k: re.sub(r"[\n\xa0]", " ", d.xpath(".//text()").get())
                for d in c
                if (k := _clean(d))
            }
            for c in list(zip(*companies_info))
        ]
        [
            c_i.update(dict.fromkeys(set(comk).difference(c_i.keys()), "N/A"))
            for c_i in companies_info
        ]

        eaals: list[str] = [
            re.sub(r"\s+", " ", "".join(e.xpath(".//text()").getall()))
            for e in sel.css(f'[name*="dei:{eaal}"]')
        ]
        secc_info_g = [sel.css(f'[name="dei:{dei}"]') for dei in seck]

        secc_info_g = filter(None, secc_info_g)
        secc_info = {
            "EntityAddressAddressLines": eaals,
            **{k: _continuedat(n) for n in secc_info_g if (k := _clean(n))},
        }
        return SECCompany(
            Companies=[Company(**ci) for ci in companies_info],
            **secc_info,
        )

    def _parse_item_1c(self, sel: Selector) -> tuple[str | None, bool]:
        start_p = re.compile(r"item\s+1c", re.I)
        end_p = re.compile(r"item\s+1d|item\s+2{1}", re.I)
        start_m = start_p.findall(sel.root.text_content())
        end_m = end_p.findall(sel.root.text_content())
        if len(start_m) == 0 or len(end_m) == 0:
            return None, False
        start = re.sub(r"\s+", " ", start_m.pop())
        end = re.sub(r"\s+", " ", end_m.pop())
        end_targets = sel.xpath(f"//*[starts-with(normalize-space(.), '{end}')]")
        if len(end_targets) == 0:
            return None, False
        sel.xpath(
            f'//a[starts-with(normalize-space(text()), "Table of Contents")]'
        ).drop()
        sel.xpath('//table[.//comment()[contains(.,"Sequence")]]').drop()
        for x in sel.xpath("//table"):
            if x.root.text_content().isspace():
                x.drop()
        for hr in sel.xpath("//hr"):
            p = hr.xpath("./preceding-sibling::*[1][.//text()]")
            if p.xpath(".//text()").get("").isdigit():
                p.drop()
            hr.drop()

        def not_decendants(sl: SelectorList) -> Selector:
            result = []
            for s in sl:
                if any(s.root in o.root.iterdescendants() for o in sl):
                    continue
                result.append(s)
            return result.pop()

        end_target = not_decendants(end_targets)
        targets = end_target.xpath("./preceding-sibling::*")
        if len(targets) == 0:
            return None, False
        targets.reverse()
        cut_off = None
        for i, t in enumerate(targets):
            found_1c = t.xpath(f".//*[starts-with(normalize-space(.), '{start}')]")
            if len(found_1c) > 0:
                cut_off = -1 - i
                break
        targets.reverse()
        data = targets[cut_off:]
        data = "\n".join(data.getall())
        data = markdownify(unescape(data))
        data = re.sub(r"\n{3,}", "\n\n", data)

        return data, True

    async def run(self, url: str) -> dict:
        sel = await self._get(url)
        return {
            "entity": self._parse_entity(sel),
            "item": self._parse_item_1c(sel),
        }


async def main(
    url: str, bheaders: dict, kookies: list[dict], output: io.TextIOWrapper
) -> None:
    """main function"""
    async with AsyncSession(
        headers=bheaders,
        impersonate="chrome124",
        allow_redirects=True,
    ) as session, asyncio.Semaphore(4):
        for ck in kookies:
            session.cookies.set(**ck)
        scraper = EDGARSearch(session)
        result = await scraper.run(url)
    with output:
        json.dump(result, output, indent=2)


def parse_args() -> argparse.Namespace:
    """Parses command line arguments."""

    class JsonToObjectType:
        def __init__(self, mode="r", *args, **kwargs):
            self._f = argparse.FileType(mode, *args, **kwargs)

        def __call__(self, string: str) -> dict | list[dict]:
            with self._f(string) as fo:
                values = json.load(fo)
            return values

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-u",
        "--url",
        help="URL to scrape",
    )
    parser.add_argument(
        "-b" "--bheaders",
        type=JsonToObjectType("r"),
        default="headers.json",
        help="Path to config json file",
    )
    parser.add_argument(
        "-k",
        "--kookies",
        type=JsonToObjectType("r"),
        default="search_cookies.json",
        help="Path to cookies json file",
    )
    parser.add_argument(
        "-o",
        "--output",
        type=argparse.FileType("w"),
        default="sec_10k_item_1c.json",
        help="Path to output json file",
    )
    return parser.parse_args()


if __name__ == "__main__":
    asyncio.run(main(**vars(parse_args())))
