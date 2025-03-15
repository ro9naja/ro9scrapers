#!/usr/bin/env python3
# coding: utf-8

import logging

import tls_client
from parsel import Selector
from rich.logging import RichHandler

URL = "https://wiki.lineageos.org"


def main():
    logging.basicConfig(
        level="INFO",
        format="%(message)s",
        datefmt="[%X]",
        handlers=[RichHandler(markup=True)],
    )
    ses = tls_client.Session(
        client_identifier="chrome_120",
        random_tls_extension_order=True,
    )
    url = f"{URL}/devices/"
    response = ses.get(url)
    logging.info(f"{url}: {response.status_code}")
    sel = Selector(response.text)
    sel.root.make_links_absolute(URL)
    brand_divs = sel.css(".devices[data-vendor]")
    for brand_div in brand_divs:
        brand = brand_div.xpath("./@data-vendor").get()
        devices = brand_div.xpath(
            ".//div[contains(@class, 'item')]"
            "[not(contains(@class, 'discontinued'))]"
            "[@data-codename]"
        )
        if not devices:
            continue
        logging.info(brand)
        for device in devices:
            device_name = device.xpath(
                ".//*[contains(@class, 'devicename')]/text()"
            ).get()
            code_name = device.xpath("./@data-codename").get()
            link = device.xpath("./@data-url").get()
            logging.info(
                {
                    "devicename": device_name,
                    "codename": code_name,
                    "link": f"{URL}{link}",
                }
            )


if __name__ == "__main__":
    main()
