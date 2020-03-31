from concurrent.futures.thread import ThreadPoolExecutor
from dataclasses import dataclass
import logging
import json
from math import ceil
from typing import Tuple, List, Iterable

import requests
from bs4 import BeautifulSoup, Tag
from src.categories import CATEGORIES

logger = logging.getLogger()

MEDIAWORLD_URL = "https://www.mediaworld.it/"
MEDIAWORLD_PRODUCT_LIST_URL = MEDIAWORLD_URL + "catalogo/{category}/{sub_category}"
MEDIAWORLD_PRODUCT_LIST_WITH_PAGE_URL = MEDIAWORLD_PRODUCT_LIST_URL + "?pageNumber={page}"


@dataclass
class ProductInfo:
    code: str
    price: float


def get_num_pages(category: str, sub_category: str) -> Tuple[str, str, int]:
    """
    Returns the number of pages to be scraped given the category and sub-category.
    Implemented by scraping the first page.
    """
    response = requests.get(
        MEDIAWORLD_PRODUCT_LIST_URL.format(category=category, sub_category=sub_category)
    )
    soup = BeautifulSoup(response.text, "html.parser")
    element: Tag = next(
        e
        for e in soup.findAll("span", attrs={"data-pagination-count": True})
        if len(e.attrs["data-pagination-total"]) > 0
    )
    pagination_count, pagination_total = (
        int(element.attrs["data-pagination-count"]),
        int(element.attrs["data-pagination-total"]),
    )
    num_pages = int(ceil(pagination_total / pagination_count))

    logger.info(f"{category} - {sub_category} - {num_pages}")
    return category, sub_category, num_pages


def get_prices(category: str, sub_category: str, page: int) -> List[ProductInfo]:
    """
    Scrapes and returns all product infos given a category, a sub category, and a page number.
    """
    logger.info(f"Working on {category} - {sub_category} - page {page}")

    response = requests.get(
        MEDIAWORLD_PRODUCT_LIST_WITH_PAGE_URL.format(
            category=category, sub_category=sub_category, page=page
        )
    )
    soup = BeautifulSoup(response.text, "html.parser")
    elements: Iterable[Tag] = soup.findAll(attrs={"data-pcode": True})

    return [
        ProductInfo(code=el.attrs["data-pcode"], price=float(el.attrs["data-gtm-price"]))
        for el in elements
    ]


def main():
    with ThreadPoolExecutor() as pool:
        logger.info("Begin scraping number of pages")

        categories_and_num_pages = list(
            pool.map(
                lambda p: get_num_pages(*p),
                [
                    (category, sub_category)
                    for category in CATEGORIES
                    for sub_category in CATEGORIES[category]
                ],
            )
        )

        logger.info("Stopped scraping number of pages")

        logger.info("Begin scraping products information")
        products = list(
            pool.map(
                lambda p: get_prices(*p),
                [
                    (category, sub_category, page)
                    for category, sub_category, num_pages in categories_and_num_pages
                    for page in range(1, num_pages + 1)
                ],
            )
        )

        # Unroll list of lists into a single list
        products = [p for prod in products for p in prod]
        logger.info("Stopped scraping products information")

    logger.info("Scraping done")

    with open("../products.json", "w") as f:
        json.dump([p.__dict__ for p in products], f, indent=2)


if __name__ == "__main__":
    logging.basicConfig(level="INFO")
    main()
