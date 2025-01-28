#!/usr/bin/env python3
"""
File: scdr.py
Author: ro9naja
Email: ro9naja@gmail.com
Github: https://github.com/ro9naja
Description: scrapes journals from:
    aom.org
    cambridge.org
    informs.org
    palgrave.com
    sagepub.com
    sciencedirect.com
    wiley.com
"""

from abc import ABC, abstractmethod
import argparse
import asyncio
from dataclasses import asdict, dataclass
import io
import json
import random
import textwrap
from typing import Any, Awaitable, Callable, Optional

from curl_cffi.requests import AsyncSession
from parsel import Selector
from rich import print
import tldextract as tld


@dataclass
class Journal:
    title: str
    link: str
    abstract: str
    authors: Optional[str] = None
    published: Optional[str] = None

    @classmethod
    def from_dict(cls, d):
        return cls(**d)

    def to_dict(self):
        return asdict(self)


@dataclass
class Journals:
    uri: str
    data: list[Journal]

    def __len__(self):
        return len(self.data)

    def to_dict(self) -> dict:
        return dict(uri=self.uri, data=[j.to_dict() for j in self.data])


class JournalsJSONEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, Journals):
            return o.to_dict()
        return super().default(o)


class Scraper(ABC):
    @abstractmethod
    async def _scrapes(self, sel: Selector) -> list[dict]:
        pass

    @abstractmethod
    async def _get_journals(self, start_uri: str) -> Journals:
        pass

    @abstractmethod
    async def run(self, start_uris: list[str]) -> list[Any]:
        pass


class JournalScraper(Scraper):
    def __init__(self, domain: str, session: AsyncSession):
        self.name = domain
        self._session = session
        self._current_url = None

    async def _get_url(self, url, xhr=False) -> tuple[str | dict[str, Any], str]:
        headers = {"X-Requested-With": "XMLHttpRequest"} if xhr else {}
        if self._current_url:
            headers["referer"] = self._current_url
        res = await self._session.get(
            url,
            headers=headers,
            allow_redirects=True,
        )
        res.raise_for_status()
        content_type = res.headers["content-type"].split(";")[0]
        result = {
            "text/html": lambda x: x.text,
            "application/json": lambda x: x.json(),
            "application/xml": lambda x: x.text,
        }.get(content_type, lambda x: x.text)(res)
        return result, res.url

    async def _get_parser(self, url, xhr=False) -> Selector:
        html, current_url = await self._get_url(url, xhr=xhr)
        self._current_url = current_url
        assert not isinstance(html, dict), "_get_parser: is not HTML"
        sel = Selector(text=html)
        sel.root.make_links_absolute(current_url)
        return sel

    async def _xhr_parser(self, url) -> Selector:
        return await self._get_parser(url, xhr=True)

    async def _scrape_articles(
        self,
        get_article_coro: Callable[[Any], Awaitable[Any]],
        items: list[Any],
    ) -> list[Any]:
        result = []
        for r in asyncio.as_completed([get_article_coro(item) for item in items]):
            try:
                t = await r
                result.append(t)
            except Exception as e:
                result.append(e)
        return result

    async def _scrapes(self, sel: Selector) -> list[dict]:
        title = sel.xpath("//h1").get()
        link = sel.xpath("//a/@href").get()
        abstract = sel.xpath("//p/text()").get()
        return [dict(title=title, link=link, abstract=abstract)]

    async def _get_journals(self, start_uri, tries=4) -> Journals:
        try:
            sel = await self._get_parser(start_uri)
        except Exception as e:
            if tries > 0:
                await asyncio.sleep(random.uniform(0.5, 1.5))
                return await self._get_journals(start_uri, tries - 1)
            else:
                raise Exception(f"{e}: {start_uri}")

        data = await self._scrapes(sel)
        return Journals(start_uri, [Journal.from_dict(d) for d in data])

    async def run(self, start_uris: list[str]) -> list[Journals | Exception]:
        print(f"[+] [green]{self.name}[/green]: {len(start_uris)}")
        results = []
        for r in asyncio.as_completed([self._get_journals(uri) for uri in start_uris]):
            try:
                results.append(await r)
            except Exception as e:
                print(f"[red][!] {self.name}[/red]: {e}")
                results.append(e)
        return results


class aom(JournalScraper):
    def __init__(self, session: AsyncSession):
        super().__init__("aom", session)

    async def _scrapes(self, sel: Selector) -> list[dict]:
        link = sel.xpath("//a[@title='In-Press']/@href").get()
        assert link, f"{self.name} _scrapes: no link"
        print(f"[+] Opening {link}")
        sel = await self._get_parser(link)
        print(sel.css("title::text").get())
        articles = sel.css(".issue-item")
        nps = sel.css(".pagination a:not(.active)::attr(href)").getall()
        for np in nps:
            sel = await self._get_parser(np)
            articles.extend(sel.css(".issue-item"))
        print(f"[+] Articles: {len(articles)}")

        return [
            {
                "title": article.css(".issue-item__title a::text").get(),
                "link": article.css(".issue-item__title a::attr(href)").get(),
                "abstract": "".join(article.css(".toc-item__abstract ::text").getall()),
                "authors": ", ".join(
                    article.css(".loa a[title]::attr(title)").getall()
                ),
                "published": article.xpath(
                    './/ul[has-class("toc-item__detail")]/li[contains(., "Published")]/text()'
                ).get(),
            }
            for article in articles
            if article.css('.accordion a[title="Preview Abstract"]')
        ]


class cambridge(JournalScraper):
    def __init__(self, session: AsyncSession):
        super().__init__("cambridge", session)

    async def _scrapes(self, sel: Selector) -> list[dict]:
        link = sel.xpath('//a[contains(., "FirstView")]/@href').get()
        assert link, f"{self.name} _scrapes: no link"
        print(f"[+] Opening {link}")
        sel = await self._get_parser(link)
        print(sel.css("title::text").get())
        articles = sel.css(".product-listing-with-inputs-content .details")
        print(f"[+] Articles: {len(articles)}")
        return [
            {
                "title": article.css(".title a::text").get("").strip(),
                "link": article.css(".title a::attr(href)").get(),
                "abstract": "".join(
                    article.xpath(".//*[@data-abstract-type]//text()").getall()
                ),
                "authors": ", ".join(article.css(".author a::text").getall()),
                "published": article.css(".date::text").get("").strip(),
            }
            for article in articles
        ]


class informs(JournalScraper):
    def __init__(self, session: AsyncSession):
        super().__init__("informs", session)

    async def _scrapes(self, sel: Selector) -> list[dict]:
        link = sel.xpath(
            '//*[@class="loi__navigation-list"]//a[contains(., "ARTICLES IN ADVANCE")]/@href'
        ).get()
        assert link, f"{self.name} _scrapes: no link"
        print(f"[+] Opening {link}")
        sel = await self._get_parser(link)
        print(sel.css("title::text").get())
        articles = sel.css(".issue-item")
        print(f"[+] Articles: {len(articles)}")
        return [
            {
                "title": art.css(".issue-item__title a::text").get(),
                "link": art.css(".issue-item__title a::attr(href)").get(),
                "abstract": "".join(
                    art.css(".toc-item__abstract .hlFld-Abstract ::text").getall()
                ),
                "authors": ", ".join(
                    " ".join(t.xpath("./text()").getall())
                    for t in art.css(".loa a.entryAuthor")
                ),
                "published": art.xpath(
                    './/div[has-class("toc-item__detail")]/p[contains(., "Published")]/text()'
                ).get(),
            }
            for art in articles
        ]


class palgrave(JournalScraper):
    def __init__(self, session: AsyncSession):
        super().__init__("palgrave", session)

    async def _scrapes(self, sel: Selector) -> list[dict]:
        link = sel.xpath(
            '//a[@data-track="click"]'
            '[contains(., "View Online First Articles")]/@href'
        ).get()
        assert link, f"{self.name} _scrapes: no link"
        print(f"[+] Opening {link}")
        sel = await self._get_parser(link)
        print(sel.css("title::text").get())
        articles = [
            art
            for art in sel.css(".c-list-group__item")
            if not art.xpath(
                './/*[has-class("c-meta__item")][contains(., "Book Review")]'
            )
            and not art.xpath(
                './/*[has-class("c-meta__item")][contains(., "Editorial")]'
            )
        ]
        alinks = [art.css(".c-card__title a::attr(href)").get() for art in articles]
        print(f"[+] Articles: {len(alinks)}")

        async def _get_article(link):
            html, current_url = await self._get_url(link)
            assert isinstance(html, str), f"{self.name} _get_article: is not HTML"
            sel = Selector(text=html)
            sel.root.make_links_absolute(current_url)
            return {
                "title": sel.css(".c-article-title::text").get(),
                "link": link,
                "abstract": "".join(sel.css("#Abs1-content ::text").getall()),
                "authors": ", ".join(
                    sel.css(
                        '.c-article-author-list__item a[data-test="author-name"]::text'
                    ).getall()
                ),
                "published": sel.css(".c-article-identifiers__item time::text").get(),
            }

        result = await self._scrape_articles(_get_article, alinks)
        return result


class sagepub(JournalScraper):
    def __init__(self, session: AsyncSession):
        super().__init__("sagepub", session)

    async def _scrapes(self, sel: Selector) -> list[dict]:
        link = sel.css('a[data-id="onlinefirst"]::attr(href)').get()
        assert link, f"{self.name} _scrapes: no link"
        print(f"[+] Opening {link}")
        sel = await self._get_parser(link)
        filterable = sel.css(".ofh > .ofh__actions > a").get()
        if filterable:
            link = f"{link}?startPage=&ContentItemType=research-article"
            sel = await self._get_parser(link)
        print(sel.css("title::text").get())
        articles = []

        async def _parse_articles(sel):
            for art in sel.css(".issue-item"):
                articles.append(
                    {
                        "title": art.css(".issue-item__heading::text").get(),
                        "link": art.css(".issue-item__title a::attr(href)").get(),
                        "abstract": art.xpath(
                            './/*[has-class("issue-item__abstract__content")]/text()'
                        ).get(),
                        "authors": art.css(".loa span[id]::text").get(),
                        "published": art.css(
                            ".issue-item__header span:nth-child(3)::text"
                        ).re_first(r"\w{3} \d{1,2}, \d{4}"),
                    }
                )
            np = sel.css("a.pagination__link.next::attr(href)").get()
            if np:
                sel = await self._xhr_parser(np)
                await _parse_articles(sel)

        await _parse_articles(sel)

        print(f"[+] Articles: {len(articles)}")
        return articles


class sciencedirect(JournalScraper):
    def __init__(self, session: AsyncSession):
        super().__init__("sciencedirect", session)

    async def _get_abstract(self, issn, pii: str) -> str:
        data, _ = await self._get_url(
            f"https://www.sciencedirect.com/journal/{issn}/abstract?pii={pii}"
        )
        assert isinstance(data, dict), "_get_abstract: is not dict"
        jsel = Selector(text=json.dumps(data))
        html = jsel.jmespath("data[*].abstracts[?class=='author'].html[]").get()
        sel = Selector(text=html)
        sel.xpath("//h5").drop()
        html = sel.xpath("//text()").get()
        return html if html else ""

    async def _scrapes(self, sel: Selector) -> list[dict]:
        link = sel.css("a.button-alternative.js-listing-link::attr(href)").get()
        assert link, f"{self.name} _scrapes: no link"
        print(f"[+] Opening {link}")
        sel = await self._get_parser(link)
        print(sel.css("title::text").get())
        issn = (
            rs.split("/")[-1]
            if (rs := sel.css('link[title="RSS"]::attr(href)').get())
            else None
        )
        print(f"[+] ISSN: {issn}")
        articles = [
            article
            for article in sel.css(".js-article-list-item.article-item")
            if article.css(".js-article-subtype").get()
        ]
        print(f"[+] Articles: {len(articles)}")

        async def _get_article(article):
            anc = article.css("a.article-content-title[id]")
            lid = anc.css("::attr(id)").get()
            return {
                "title": article.css(".js-article-title::text").get(),
                "link": anc.css("::attr(href)").get(),
                "authors": article.css(".js-article__item__authors::text").get(),
                "abstract": await self._get_abstract(issn, lid),
            }

        result = await self._scrape_articles(_get_article, articles)
        return result


class wiley(JournalScraper):
    def __init__(self, session: AsyncSession):
        super().__init__("wiley", session)

    async def _get_abstract(self, url: str) -> tuple[str, str]:
        html, _ = await self._get_url(url)
        assert isinstance(html, str), f"{self.name} _get_abstract: is not HTML"
        sel = Selector(text=html)
        abs = "".join(
            sel.xpath('//section[./*[.="Abstract"]]//div//text()').getall()
        ).strip()
        authors = ", ".join(sel.css(".loa-authors p.author-name::text").getall())
        return abs, authors

    async def _scrapes(self, sel: Selector) -> list[dict]:
        link = sel.css("[data-menu-label='Early View'] a::attr(href)").get()
        assert link, "_scrapes: no early view link"
        print(f"[+] Opening {link}")
        sel = await self._get_parser(link)
        print(sel.css("title::text").get())
        articles = [
            a
            for a in sel.xpath("//div[contains(@class, 'issue-item')]")
            if not a.xpath('.//*[contains(@class, "corrections-container")]').get()
            and a.css('a[title="Abstract"]')
        ]
        print(f"[+] Articles: {len(articles)}")

        async def _get_article(article):
            try:
                abl = article.css(
                    '.issue-item__links a[title="Abstract"]::attr(href)'
                ).get()
                title = article.css("a.issue-item__title h2::text").get()
                alink = article.css("a.issue-item__title::attr(href)").get()
                published = article.css(".ePubDate span:last-child::text").get()
                abstract, authors = await self._get_abstract(abl)
                return {
                    "title": title,
                    "link": alink,
                    "authors": authors,
                    "published": published,
                    "abstract": abstract,
                }
            except Exception as e:
                print(e)

        result = await self._scrape_articles(_get_article, articles)
        return result


class Crawler:
    _scrapers = {
        "aom": aom,
        "cambridge": cambridge,
        "informs": informs,
        "palgrave": palgrave,
        "sagepub": sagepub,
        "sciencedirect": sciencedirect,
        "wiley": wiley,
    }

    def __init__(self, session: AsyncSession, input: dict) -> None:
        self._input = input
        self._session = session

    async def run(self) -> list[Journals | Exception]:
        result = []
        for regd, start_uris in self._input.items():
            domain = tld.extract(regd).domain
            if domain not in self._scrapers:
                continue
            scraper = self._scrapers[domain](self._session)
            journals = await scraper.run(start_uris)
            # print(journals)
            result.extend(journals)
        return result


async def main(
    config, input: dict, kookies: list[dict], output: io.TextIOWrapper
) -> None:
    async with AsyncSession(
        headers=config.get("headers"),
        cookies=config.get("cookies"),
        impersonate="chrome124",
    ) as session:
        for k in kookies:
            _ = k.pop("expires")
            session.cookies.set(**k)
        crawler = Crawler(session, input)
        collections = await crawler.run()
    num_collections = sum(
        len(journals) for journals in collections if isinstance(journals, Journals)
    )
    print(
        f"[+] [green]Saving {num_collections} Journals to[/green] [blue]{output.name}[/blue]"
    )
    with output as fo:
        json.dump(collections, fo, cls=JournalsJSONEncoder)


def parse_args() -> argparse.Namespace:
    """parse arguments"""

    def description():
        return textwrap.dedent(
            "\n\t".join(
                [
                    "Crawler for journals:",
                    "aom.org",
                    "cambridge.org",
                    "informs.org",
                    "palgrave.com",
                    "sagepub.com",
                    "sciencedirect.com",
                    "wiley.com",
                ]
            )
        )

    class JsonToDictType:
        def __init__(self, mode="r", *args, **kwargs):
            self._f = argparse.FileType(mode, *args, **kwargs)

        def __call__(self, string: str) -> dict:
            with self._f(string) as fo:
                values = json.load(fo)
            return values

    parser = argparse.ArgumentParser(
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description=description(),
    )
    parser.add_argument(
        "-i",
        "--input",
        type=JsonToDictType("r"),
        default="links.json",
        help="default: 'links.json'",
    )
    parser.add_argument(
        "-c",
        "--config",
        type=JsonToDictType("r"),
        default="cookies_headers.json",
        help="default: 'cookies_headers.json'",
    )
    parser.add_argument(
        "-k",
        "--kookies",
        type=JsonToDictType("r"),
        default="domains_cookies.json",
        help="default: 'domains_cookies.json'",
    )
    parser.add_argument(
        "-o",
        "--output",
        type=argparse.FileType("w"),
        default="journals_collection.json",
        help="default: 'journals_collection.json'",
    )
    return parser.parse_args()


if __name__ == "__main__":
    asyncio.run(main(**vars(parse_args())))
