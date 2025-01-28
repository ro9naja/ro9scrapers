import argparse
import json

import nodriver as uc
from rich import print


async def main(urls: list[str], url: str) -> None:
    urls = list(filter(None, urls + [url]))
    browser = await uc.start(headless=False)
    p = await browser.get("https://www.google.com")
    await p
    for url in urls:
        print(f"[+] Opening {url}")
        await p.get(url)
        await p.wait()
        await p.bring_to_front()
        await p
        input("[?] Press enter to continue --->> ")

    cookies = await browser.cookies.get_all(requests_cookie_format=True)
    # print(type(cookies), cookies)
    await p.close()
    cookies_list = []
    for ck in cookies:
        cookies_list.append(
            {
                "name": ck.name,
                "value": ck.value,
                "domain": ck.domain,
                "path": ck.path,
                "expires": ck.expires,
            }
        )
    print(cookies_list)
    print("[+] Saving cookies to domains_cookies.json")
    with open("domains_cookies.json", "w") as f:
        json.dump(cookies_list, f)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-u",
        "--url",
        help="URL to scrape",
    )
    return parser.parse_args()


if __name__ == "__main__":
    urls = [
        "https://journals.aom.org/toc/amj/current",
        "https://journals.sagepub.com/home/ORM",
        "https://onlinelibrary.wiley.com/journal/10970266",
        "https://pubsonline.informs.org/journal/mnsc",
        "https://www.cambridge.org/core/journals/business-ethics-quarterly",
        "https://www.palgrave.com/gp/journal/41267",
        "https://www.sciencedirect.com/journal/journal-of-business-venturing",
    ]
    uc.loop().run_until_complete(main(urls))
