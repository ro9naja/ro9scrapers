#!/usr/bin/env python3
"""
File: sec_01.py
Author: ro9naja
Email: ro9naja@email.com
Github: https://github.com/ro9naja
Description: scrapes SEC documents for Company Information and Item 1C
"""

import argparse
import asyncio
from dataclasses import dataclass
from functools import partial
import json
import re
import io
from typing import Any, Final, Optional
from html import unescape
from urllib.parse import parse_qs, urljoin, urlparse

from curl_cffi.requests import AsyncSession
from markdownify import markdownify
from parsel import Selector, SelectorList
from rich import print


URL: Final = "https://www.sec.gov/cgi-bin/current"
DEBUG: bool = False


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
    TradingSymbol: Optional[str] = None
    SecurityExchangeName: Optional[str] = None
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

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "address": self.address,
            "telefon": self.telefon,
            "trading_symbol": self.TradingSymbol,
            "exchange": self.SecurityExchangeName,
            "item_1c": self.item_1c,
        }

    class JSONEncoder(json.JSONEncoder):
        def default(self, o):
            if isinstance(o, SECCompany):
                return o.to_dict()
            else:
                return super().default(o)


async def get_parser(
    session: AsyncSession, *args, **kwargs
) -> tuple[Selector, dict[str, list[str]]]:
    res = await session.get(*args, **kwargs)
    res.raise_for_status()
    assert res.status_code == 200
    parsed_q = parse_qs(urlparse(res.url).query)
    sel = Selector(text=res.text)
    if not isinstance(sel.root, dict):
        sel.root.make_links_absolute(res.redirect_url or res.url)
    return sel, parsed_q


def log_debug(fn, *args, **kwargs) -> None:
    if DEBUG:
        print(f"[!] {fn}", *args, **kwargs)


def not_decendants(sl: SelectorList) -> Selector:
    result = []
    for s in sl:
        if any(s.root in o.root.iterdescendants() for o in sl):
            continue
        result.append(s)
    return result.pop()


def parse_method_1(sel: Selector) -> tuple[str | None, bool]:
    logd = partial(log_debug, "parse_method_1")
    logd("[red][+]Checking ITEM 1C[/red]")
    start_p = re.compile(r"item\s+1c", re.I)
    end_p = re.compile(r"item\s+1d|(item\s+2\.?)\s+Properties", re.I)
    start_m = start_p.findall(sel.root.text_content())
    end_m = end_p.findall(sel.root.text_content())
    if len(start_m) == 0 or len(end_m) == 0:
        return None, False
    start_m, end_m = map(lambda x: x.pop(), (start_m, end_m))
    logd(f"start_m: {start_m}, end_m: {end_m}")
    start = re.sub(r"\s+", " ", start_m)
    end = re.sub(r"\s+", " ", end_m)
    logd(f"start: {start}, end: {end}")
    end_targets = sel.xpath(
        f"//body//*[starts-with(normalize-space(.), '{end}')]"
        "|"
        f"//body/*[.//*[starts-with(normalize-space(.), '{end}')]]"
    )
    if len(end_targets) == 0:
        logd(f"end_targets: {end_targets}")
        return None, False
    # logd(f"end_targets: {end_targets}")
    sel.xpath(f'//a[starts-with(normalize-space(text()), "Table of Contents")]').drop()
    sel.xpath('//table[.//comment()[contains(.,"Sequence")]]').drop()
    for x in sel.xpath("//table"):
        if x.root.text_content().isspace():
            x.drop()
    for hr in sel.xpath("//hr"):
        p = hr.xpath("./preceding-sibling::*[1][.//text()]")
        if p.xpath(".//text()").get("").isdigit():
            p.drop()
        hr.drop()
    end_target = not_decendants(end_targets)
    # logd(f"end_target: {end_target}")
    targets = end_target.xpath("self::* | ./preceding-sibling::*")
    if len(targets) == 0:
        return None, False
    logd(f"targets: {len(targets)}")
    targets.reverse()
    cut_off = None
    for i, t in enumerate(targets):
        found_1c = t.xpath(f".//*[starts-with(normalize-space(.), '{start}')]")
        if len(found_1c) > 0:
            cut_off = -1 - i
            logd(f"found_1c cut_off {cut_off}")
            break
    targets.reverse()
    data = targets[cut_off:]
    data = "\n".join(data.getall())
    data = markdownify(unescape(data))
    data = re.sub(r"\n[\s\xa0\u200b]+\n", "\n\n", data).strip()
    start_m, end_m = list(map(lambda x: re.sub(r"\s+", r"\\s+", x), (start_m, end_m)))
    logd(f"start_m: {start_m}, end_m: {end_m}")
    data = (
        m.group(1)
        if (m := re.search(rf"({start_m}.*){end_m}", data, re.I | re.S))
        else "[red] !! REGEX NOT FOUND !! [/red]"
    )
    data = re.sub(
        r"Item[\s\\n\xa0\u200b]+1C.?[\\n\s\xa0\u200b\*]+", r"Item 1C. ", data, re.I
    )

    return data, True


def parse_entity(
    sel: Selector,
) -> tuple[dict[str, Any], list[dict[str, str]]]:
    logd = partial(log_debug, "parse_entity")
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

    company_info = list(filter(None, [sel.css(f'[name="dei:{dei}"]') for dei in comk]))
    logd(f"company_info: \n{company_info}")
    logd(list(zip(*company_info)))
    company_info = [
        {
            k: re.sub(r"[\n\xa0]", " ", d.xpath(".//text()").get())
            for d in c
            if (k := _clean(d))
        }
        for c in list(zip(*company_info))
    ]
    logd(f"company_info: {company_info}")
    [
        c_i.update(dict.fromkeys(set(comk).difference(c_i.keys()), "N/A"))
        for c_i in company_info
    ]
    logd(f"company_info:\n{company_info}")

    eaals: list[str] = [
        re.sub(r"\s+", " ", "".join(e.xpath(".//text()").getall()))
        for e in sel.css(f'[name*="dei:{eaal}"]')
    ]
    sec_company_info_g = [sel.css(f'[name="dei:{dei}"]') for dei in seck]
    logd(f"Missing: {[x for z,x in zip(sec_company_info_g, seck) if z is None]}")

    sec_company_info_g = filter(None, sec_company_info_g)
    sec_company_info = {
        "EntityAddressAddressLines": eaals,
        **{k: _continuedat(n) for n in sec_company_info_g if (k := _clean(n))},
    }
    return sec_company_info, company_info


async def get_item_1c(session: AsyncSession, tenk: str) -> SECCompany:
    logd = partial(log_debug, "get_item_1c")
    sget = partial(get_parser, session)
    # open 2nd page
    sel2, _ = await sget(tenk)
    assert (tenk_2nd := sel2.css(".tableFile a::attr(href)").get()) is not None
    # open 3rd page
    selx, params_3 = await sget(tenk_2nd)
    doc = params_3.get("doc", [])[0]
    print(f"[+] Opening {tenk_2nd}...")
    logd("params_3", params_3)
    logd("doc", doc)
    if doc is not None:
        url_metalinks = urljoin(tenk_2nd, f"{doc.rpartition('/')[0]}/MetaLinks.json")
        # XMLHttpRequest get MetaLinks.json
        _ = await sget(
            url_metalinks,
            headers={"X-Requested-With": "XMLHttpRequest", "Referer": tenk_2nd},
        )
        tenk_3rd = urljoin(tenk_2nd, "/ixviewer/ix.html")
        # open 4th page
        _ = await sget(tenk_3rd, params={"doc": doc})
        tenk_4th = urljoin(tenk_3rd, doc)
        # open 5th page
        selx, _ = await sget(tenk_4th)
    print()
    try:
        sec_info, companies = parse_entity(selx)
        methods = [
            parse_method_1,
        ]
        item_1c: str | None = None
        successful: bool = False

        while not successful and len(methods):
            method = methods.pop()
            item_1c, successful = method(selx)
        companies = [Company(**c) for c in companies]
        # assert item_1c is not None
        seccom = SECCompany(
            Companies=companies,
            **sec_info,
            item_1c=item_1c,
        )
    except Exception as exp:
        debuging(selx)
        raise exp
    else:
        print(seccom)
        return seccom


async def get_10k(session: AsyncSession, url: str) -> list[SECCompany]:
    res = await session.get(url, params={"q1": 0, "q2": 0, "q3": 0})
    sel = Selector(text=res.text)
    sel.root.make_links_absolute(res.redirect_url or res.url)
    a_10ks = filter(
        None,
        [
            a.xpath("./@href").get()
            for a in sel.css("pre a")
            if a.xpath("./text()").get("").lower().startswith("10-k")
        ][:10],
    )
    result = []
    for r in asyncio.as_completed([get_item_1c(session, a_10k) for a_10k in a_10ks]):
        result.append(await r)
    return result


async def main(
    bheaders: dict[str, str],
    cookies: list[dict[str, Any]],
    output: io.TextIOWrapper,
    debug: bool,
) -> None:
    global DEBUG
    DEBUG = debug
    async with (
        AsyncSession(
            headers=bheaders,
            impersonate="chrome124",
            allow_redirects=True,
        ) as session,
        asyncio.Semaphore(9),
    ):
        for ck in cookies:
            session.cookies.set(**ck)
        tenks = await get_10k(session, URL)
    with output:
        json.dump(tenks, output, indent=2, cls=SECCompany.JSONEncoder)


def parse_args() -> argparse.Namespace:
    class JsonToObjectType:
        def __init__(self, mode="r", *args, **kwargs):
            self._f = argparse.FileType(mode, *args, **kwargs)

        def __call__(self, string: str) -> dict:
            with self._f(string) as fo:
                values = json.load(fo)
            return values

    parser = argparse.ArgumentParser()
    parser.add_argument("-d", "--debug", action="store_true")
    parser.add_argument(
        "-b",
        "--bheaders",
        type=JsonToObjectType("r"),
        default="headers.json",
        help="default: headers.json",
    )
    parser.add_argument(
        "-c",
        "--cookies",
        type=JsonToObjectType("r"),
        default="cookies.json",
        help="default: cookies.json",
    )
    parser.add_argument(
        "-o",
        "--output",
        type=argparse.FileType("w"),
        default="secc_item_1cs.json",
        help="default: secc_item_1cs.json",
    )
    return parser.parse_args()


def debuging(sel: Selector) -> None:
    rtree = sel.root.getroottree()
    rtree.write("debug.html", encoding="utf-8", method="html")


if __name__ == "__main__":
    asyncio.run(main(**vars(parse_args())))
