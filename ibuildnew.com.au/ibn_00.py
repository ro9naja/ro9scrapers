#!/usr/bin/env python3
"""
File: ibn_00.py
Author: ro9naja
Email: ro9naja@gmail.com
Github: https://github.com/ro9naja
Description: scrape www.ibuildnew.com.au for products and vendors and save to airtable.com
"""

from dataclasses import dataclass
from functools import partial
import json
import re
import tomllib
from typing import Any, Final, Generator, Self
from urllib.parse import urljoin, urlparse

from httpx import Client
from rich import print
from selectolax.parser import HTMLParser, Node

from tables import ATVendor, ATProduct, generate_vendor_mapping


U_A: Final = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/38.0.101.76 Safari/537.36"
PLX: tuple = tuple()
VLX: tuple = tuple()
VDD: dict[int, str] = dict()


@dataclass
class InfoRest:
    established: str
    designs: str | None = None
    budgets: str | None = None
    build_volume: str | None = None
    service: str | None = None
    guarantee: str | None = None

    @classmethod
    def from_node(cls, n: Node) -> Self:
        key_map = dict(
            flag="established",
            large="designs",
            dollar="budgets",
            hammer="build_volume",
            wrench="service",
            up="guarantee",
        )

        cleaner = dict(
            established=lambda x: m.group() if (m := re.search(r"\d{4}", x)) else x,
            designs=lambda x: m.group() if (m := re.search(r"\d+", x)) else x,
        )

        def _ukm(k):
            return key_map.get(
                m.group()
                if (m := re.search(r"|".join(key_map.keys()), k))
                else "error",
                "unknown",
            )

        return cls(
            **dict(
                (
                    dk := _ukm(r.css_first("i").attributes.get("class")),
                    cleaner.get(dk, lambda x: re.sub(r"\s{2,}", " ", x))(
                        r.css_first(".bp-text-space").text(strip=True)
                    ),
                )
                for r in n.css(".row")
            )
        )

    def __str__(self) -> str:
        return "\n".join(f"{k}: {v}" for k, v in self.__dict__.items() if v)


@dataclass
class Vendor:
    id: int
    name: str
    primary_contact_name: str
    primary_contact_phone: str
    iovox_phone: str
    iovox_phone_formatted: str
    url: str
    website: str | None
    social_media: dict[str, str | None]
    info_rest: InfoRest
    video_url: str | None
    description: str
    awards: list[str | None]

    @classmethod
    def from_node(cls, n: Node, clg: partial) -> Self:
        url = n.attributes.get("href")
        hpr = clg(url)

        def _cfga(s, a):
            e = hpr.css_first(s)
            if e is None:
                return None
            return e.attributes.get(a)

        def _smkv(n):
            d = n.attributes
            k = d.get("title").lower()
            v = d.get("href").split("?")[0]
            return k, v

        try:
            d = json.loads(_cfga("[data-builder]", "data-builder") or "{}")
            website = _cfga("a[data-tracking][class*=website]", "href")
            social_media = dict(
                _smkv(n) for n in hpr.css(".strike-social a[data-tracking]")
            )
            info_rest = InfoRest.from_node(hpr.css_first(".builders-info-rest"))
            vurl = _cfga("a[data-tracking][data-url*=video]", "data-url")
            if vurl is not None:
                vpr = clg(vurl)
                video_url = vpr.css_first(".popup-content iframe").attributes.get("src")
                if "youtube" in video_url:
                    video_url = urljoin(
                        "https://youtu.be",
                        urlparse(video_url).path.split("/").pop(),
                    )
            else:
                video_url = None
            description = hpr.css_first(".js-truncate.text-16").text(strip=True)
            awards = [tr.text(strip=True) for tr in hpr.css("[class*=awards] tr")]
        except Exception as exp:
            with open("error.html", "w") as f:
                f.write(hpr.html)
            raise exp

        sd = {k: d.get(k) for k in cls.__annotations__.keys() if k in d}
        return cls(
            **sd,
            website=website,
            social_media=social_media,
            info_rest=info_rest,
            video_url=video_url,
            description=description,
            awards=awards,
        )

    def to_airtable(self) -> ATVendor:
        return ATVendor(
            builder_id=self.id,
            name=self.name,
            info=str(self.info_rest),
            description=self.description,
            phone_number=self.iovox_phone_formatted,
            primary_contact=self.primary_contact_name,
            primary_contact_phone=self.primary_contact_phone,
            website=self.website,
            facebook=self.social_media.get("facebook"),
            twitter=self.social_media.get("twitter"),
            instagram=self.social_media.get("instagram"),
            video=self.video_url,
            awards="\n".join(filter(None, self.awards)),
        )


@dataclass
class Builder:
    id: int
    name: str
    url: str
    iovox_phone: str

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> Self:
        rd = {k: d.get(k, 0 if k == "id" else "") for k in cls.__annotations__.keys()}
        return cls(**rd)


@dataclass
class ImageObject:
    original_url: str | None

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> Self:
        rd = {k: d.get(k) for k in cls.__annotations__.keys()}
        return cls(**rd)


@dataclass
class Tracking:
    regions: list[str] | None

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> Self:
        rd = {k: d.get(k) for k in cls.__annotations__.keys()}
        return cls(**rd)


@dataclass
class Product:
    id: int
    name: str
    url: str
    tour_url: str
    optimized_description: str
    base_price: int
    bedrooms: int
    bathrooms: int
    living_spaces: int
    car_spaces: int
    floor_count: int
    house_squares: float
    block_width: float
    block_length: float
    study: bool
    alfresco: bool
    duplex: bool
    builder: Builder
    tracking: Tracking
    floor_plans: list[ImageObject]
    first_image: ImageObject
    images: list[ImageObject | None]

    @property
    def at_gallery(self) -> list[dict[str, str | None]]:
        images = set(
            (
                self.first_image.original_url,
                *(i.original_url for i in self.images if i is not None),
            )
        )
        return [{"url": url} for url in images]

    @property
    def at_floorplan(self) -> list[dict[str, str | None]]:
        images = set((*(i.original_url for i in self.floor_plans if i is not None),))
        return [{"url": url} for url in images]

    @property
    def at_build_locations(self) -> str:
        return "\n".join(self.tracking.regions or "")

    @staticmethod
    def yes_or_no(b: bool) -> str:
        return "Yes" if b else "No"

    @classmethod
    def from_node(cls, n: Node, clg: partial) -> Self:
        assert (s := n.attributes.get("data-product")) is not None
        d = json.loads(s)
        url = d.get("url")
        hpr = clg(url)
        d["optimized_description"] = hpr.css_first(".product-info__description").text(
            strip=True
        )
        sd = {k: d.get(k) for k in cls.__annotations__.keys()}
        sd["builder"] = Builder.from_dict(sd.pop("builder"))
        sd["tracking"] = Tracking.from_dict(sd.pop("tracking"))
        sd["floor_plans"] = [ImageObject.from_dict(f) for f in sd.pop("floor_plans")]
        sd["first_image"] = ImageObject.from_dict(sd.pop("first_image"))
        sd["images"] = [ImageObject.from_dict(i) for i in sd.pop("images")]

        return cls(**sd)

    def to_airtable(self) -> ATProduct:
        return ATProduct(
            name=self.name,
            base_price=self.base_price,
            gallery=self.at_gallery,
            vendor=[ATVendor.from_id(VDD.get(self.builder.id, ""))],
            bedrooms=self.bedrooms,
            bathrooms=self.bathrooms,
            living_spaces=self.living_spaces,
            car_spaces=self.car_spaces,
            floors=self.floor_count,
            study=self.yes_or_no(self.study),
            alfresco=self.yes_or_no(self.alfresco),
            duplex=self.yes_or_no(self.duplex),
            floor_plan=self.at_floorplan,
            house_size=float(self.house_squares),
            block_width=float(self.block_width),
            block_length=float(self.block_length),
            description=self.optimized_description,
            tour_3d=self.tour_url,
            build_locations=self.at_build_locations,
        )


def get_hparser(client: Client, url: str) -> HTMLParser:
    response = client.get(url)
    return HTMLParser(response.content)


def get_products(clget: partial) -> Generator[Product, None, None]:
    def _getproducts(url: str) -> Generator[Product, None, None]:
        parser = clget(url)
        yield from (Product.from_node(p, clget) for p in parser.css("[data-product]"))
        nlx = parser.css_first("a.pagination__direction.pagination__direction--next")
        if nlx is None:
            return
        for p in _getproducts(urljoin(url, nlx.attributes.get("href"))):
            yield p

    for product_g in (_getproducts(url) for url in PLX):
        yield from product_g


def get_vendors(clget: partial) -> Generator[Vendor, None, None]:
    def _getvendors(url: str) -> Generator[Vendor, None, None]:
        parser = clget(url)
        yield from (
            Vendor.from_node(v, clget) for v in parser.css("[data-client-type] a[href]")
        )

    for vendor_g in (_getvendors(url) for url in VLX):
        yield from vendor_g


def main() -> None:
    global PLX, VLX, VDD
    with open("inputs.toml", "rb") as fd:
        d = tomllib.load(fd)
        PLX = tuple(d["products"])
        VLX = tuple(d["vendors"])

    with Client(headers={"User-Agent": U_A}) as client:
        clget = partial(get_hparser, client)
        vendors: list[ATVendor] = []
        products: list[ATProduct] = []

        for i, vendor in enumerate(get_vendors(clget)):
            print(vendor)
            print(i)
            vendors.append(vendor.to_airtable())

        print(f"[+] Saving [blue]{len(vendors)} vendors[/blue] to airtable.com")
        ATVendor.batch_save(vendors)
        print("[+] [green]Done![/green]")

        VDD = generate_vendor_mapping()
        # print(VDD)
        for i, product in enumerate(get_products(clget)):
            print(product)
            print(i)
            products.append(product.to_airtable())

        print(f"[+] Saving [blue]{len(products)} products[/blue] to airtable.com")
        ATProduct.batch_save(products)
        print("[+] [green]Done![/green]")


if __name__ == "__main__":
    main()
