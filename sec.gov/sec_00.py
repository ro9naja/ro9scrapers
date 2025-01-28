#!/usr/bin/env python3
"""
File: sec_00.py
Author: ro9naja
Email: ro9naja@email.com
Github: https://github.com/ro9naja
Description: scrapes SEC documents for Company Information and Item 1C
"""

from dataclasses import dataclass
from functools import partial
from typing import Generator, Final
from urllib.parse import urljoin
import re

from httpx import Client, QueryParams
from lxml.html import HtmlElement, fromstring
from rich import print
from selectolax.parser import HTMLParser


URL: Final = "https://www.sec.gov/cgi-bin/current"
U_A: Final = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"


@dataclass
class Company:
    EntityFileNumber: str
    EntityRegistrantName: str
    EntityIncorporationStateCountryCode: str
    EntityTaxIdentificationNumber: str | None


@dataclass
class SecCompany:
    Companies: list[Company]
    EntityAddressAddressLines: list[str]
    EntityAddressCityOrTown: str
    EntityAddressPostalZipCode: str
    CityAreaCode: str
    LocalPhoneNumber: str
    TradingSymbol: str
    SecurityExchangeName: str
    item_1c: str | None = None
    EntityAddressStateOrProvince: str | None = None
    EntityAddressCountry: str | None = None

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
        areacode = re.sub(r"\D", "", self.CityAreaCode)
        return f"({areacode}) {self.LocalPhoneNumber}"

    # def __repr__(self) -> str:
    #     i1c_sum = f"{self.item_1c[:128]}...\n...{self.item_1c[-128:]}"
    #     return f"{self.name}\n{self.address}\n{self.telefon}\n{i1c_sum}"


def get_parser(client: Client, *args, **kwargs) -> tuple[HTMLParser, QueryParams]:
    res = client.get(*args, **kwargs)
    sign = "[green]✓[/green]" if res.status_code == 200 else "[red]✗[/red]"
    print(f" [blue]▶▶▶[/blue] {sign}", end=" ")
    assert res.status_code == 200
    return HTMLParser(res.content), res.url.params


def log_debug(debug: bool, *args, **kwargs) -> None:
    if debug:
        print(*args, **kwargs)


def parse_method_1(hpr: HTMLParser, debug: bool = False) -> tuple[str | None, bool]:
    logd = partial(log_debug, debug)
    logd("Checking #item_1c_cybersecurity")
    if hpr.css_first("#item_1c_cybersecurity") is None:
        return None, False
    logd("Found #item_1c_cybersecurity")
    pars = hpr.css("#item_1c_cybersecurity, #item_1c_cybersecurity ~ *")
    stop = hpr.css_first("#item_1c_cybersecurity ~ [id]")
    assert stop is not None
    data = pars[: pars.index(stop)]
    if not len(data):
        logd(f"pars: {pars} stop: {stop}")
        return None, False
    data = [
        t
        for d in data
        if (not (t := d.text()).isspace() and len(t) and not t.isdigit())
    ]
    return "\n".join(data), True


def remove_decendants(nodes: list[HtmlElement]) -> list[HtmlElement]:
    for n in nodes:
        if any(n in o.iterdescendants() for o in nodes):
            nodes.remove(n)
    return nodes


def parse_method_2(hpr: HTMLParser, debug: bool = False) -> tuple[str | None, bool]:
    logd = partial(log_debug, debug)
    logd("Checking ITEM 1C")
    dox: HtmlElement = fromstring(hpr.html)
    xpt = '//*[starts-with(normalize-space(.), "{0}")]' "[self::div or self::p]"
    item1cpats = ("Item 1C", "Item\xa01C", "ITEM 1C", "ITEM\xa01C")
    targets = dox.xpath(" | ".join([xpt.format(k) for k in item1cpats]))
    if len(targets) == 0:
        return None, False
    logd(f"targets: {targets}")
    # remove childs
    targets = remove_decendants(targets)
    logd(f"targets: {targets}")
    target = targets.pop()
    if target is None:
        return None, False
    logd("Found ITEM 1C")
    logd(target, target.attrib, target.text_content(), sep="\n")
    start = target if target.attrib.has_key("id") else None
    stop = None
    preds = ["", "-sibling"]
    pred: str = ""
    while any(s is None for s in (start, stop)) and len(preds):
        pred = preds.pop()
        logd(f"pred: {pred}")
        if start is None:
            start = (target.xpath(f"preceding{pred}::*[.//@id]") or [None]).pop()
        stop = (target.xpath(f"following{pred}::*[.//@id]") or [None]).pop(0)
    if any((start is None, stop is None)):
        logd(f"start: {start}, stop: {stop}", sep="\n")
        return None, False
    assert start is not None
    assert stop is not None
    logd(f"start: {start.attrib.get('id')}, stop: {stop.attrib.get('id')}")

    nodes_pred = (
        f"./self::* | ./following{pred}::*"
        if start == target
        else f"./following{pred}::*"
    )
    nodes = start.xpath(nodes_pred)
    data = nodes[: nodes.index(stop)]
    data = remove_decendants(data)
    if not len(data):
        logd("No data")
        return None, False
    data = [
        t
        for d in data
        if (not (t := d.text_content()).isspace() and len(t) and not t.isdigit())
    ]
    return "\n".join(data), True


def parse_entity(
    hpr: HTMLParser, debug: bool = False
) -> tuple[dict[str, str], list[dict[str, str]]]:
    logd = partial(log_debug, debug)
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
        return k if (k := x.attributes.get("name")) is None else k.replace("dei:", "")

    def _continuedat(node) -> str:
        if "continuedat" in node.attributes:
            _id = node.attributes.get("continuedat")
            cdat = hpr.css_first(f'[id="{_id}"]').text()
            return f"{node.text()}{cdat}"
        else:
            return node.text()

    company_info = list(filter(None, [hpr.css(f'[name="dei:{dei}"]') for dei in comk]))
    logd(f"company_info: {company_info}")
    company_info = [
        {k: d.text() for d in c if (k := _clean(d))} for c in list(zip(*company_info))
    ]
    logd(f"company_info: {company_info}")
    [
        c_i.update(dict.fromkeys(set(comk).difference(c_i.keys()), "N/A"))
        for c_i in company_info
    ]
    logd(f"company_info:\n{company_info}")

    eaals: list[str] = [e.text() for e in hpr.css(f'[name*="dei:{eaal}"]')]
    sec_company_info_g = [hpr.css_first(f'[name="dei:{dei}"]') for dei in seck]
    logd(f"Missing: {[x for z,x in zip(sec_company_info_g, seck) if z is None]}")

    sec_company_info_g = filter(None, sec_company_info_g)
    sec_company_info = {
        "EntityAddressAddressLines": eaals,
        **{k: _continuedat(n) for n in sec_company_info_g if (k := _clean(n))},
    }
    return sec_company_info, company_info


def get_item_1c(
    tenk: str, client: Client, debug: bool = False
) -> Generator[SecCompany, None, None]:
    logd = partial(log_debug, debug)
    clget = partial(get_parser, client)
    url_2nd_page = urljoin(URL, tenk)
    # open 2nd page
    hpr2, _ = clget(url_2nd_page)
    assert (
        tenk_2nd := hpr2.css_first(".tableFile").css_first("a").attributes.get("href")
    ) is not None
    url_3rd_page = urljoin(url_2nd_page, tenk_2nd)
    # open 3rd page
    hprx, params_3 = clget(url_3rd_page)
    doc = params_3.get("doc")
    logd("\ntenk_2nd", tenk_2nd)
    logd("params_3", params_3)
    logd("doc", doc)
    if doc is not None:
        url_metalinks = urljoin(
            url_3rd_page, f"{doc.rpartition('/')[0]}/MetaLinks.json"
        )
        # XMLHttpRequest get MetaLinks.json
        _ = clget(url_metalinks)
        url_4th_page = urljoin(url_3rd_page, "/ixviewer/ix.html")
        # open 4th page
        _ = clget(url_4th_page, params={"doc": doc})
        url_5th_page = urljoin(url_4th_page, doc)
        # open 5th page
        hprx, _ = clget(url_5th_page)
    print()
    try:
        sec_info, companies = parse_entity(hprx)
        methods = [
            parse_method_1,
            parse_method_2,
        ]
        item_1c: str | None = None
        successful: bool = False

        while not successful and len(methods):
            method = methods.pop()
            item_1c, successful = method(hprx, debug=debug)
        companies = [Company(**c) for c in companies]
        # assert item_1c is not None
        seccom = SecCompany(
            Companies=companies,
            **sec_info,  # type: ignore
            item_1c=item_1c,
        )
    except Exception as exp:
        debuging(hprx)
        raise exp
    else:
        yield seccom


def get_10k(client: Client, url: str) -> Generator[SecCompany, None, None]:
    res = client.get(url, params={"q1": 0, "q2": 0, "q3": 0})
    hpr = HTMLParser(res.content)
    for a_10k in filter(
        None,
        [
            a.attributes.get("href")
            for a in hpr.css("pre a")
            if a.text().lower().startswith("10-k")
        ][:10],
    ):
        yield from get_item_1c(a_10k, client, debug=True)


def main() -> None:
    with Client(headers={"User-Agent": U_A}) as client:
        for tenk in get_10k(client, URL):
            print(repr(tenk))


def debuging(hpr: HTMLParser) -> None:
    content = hpr.html
    if content is None:
        return
    with open("error.html", "wb") as f:
        f.write(content.encode("utf-8"))


if __name__ == "__main__":
    # debuging()
    main()
