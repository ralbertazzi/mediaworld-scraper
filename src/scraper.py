import json
import logging
from collections import defaultdict
from concurrent.futures.thread import ThreadPoolExecutor
from dataclasses import dataclass
from math import ceil
from typing import Tuple, List, Iterable, Dict

import requests
from bs4 import BeautifulSoup, Tag

logger = logging.getLogger()

MEDIAWORLD_URL = "https://www.mediaworld.it/"
MEDIAWORLD_PRODUCT_LIST_URL = MEDIAWORLD_URL + "catalogo/{category}/{sub_category}"
MEDIAWORLD_PRODUCT_LIST_WITH_PAGE_URL = MEDIAWORLD_PRODUCT_LIST_URL + "?pageNumber={page}"


@dataclass
class ProductInfo:
    code: str
    name: str
    category: str
    sub_category: str
    price: float


def get_categories() -> Dict[str, List[str]]:
    """
    Scrapes categories and sub categories from the main page
    """
    response = requests.get(MEDIAWORLD_URL)
    soup = BeautifulSoup(response.text, "html.parser")
    bar: Tag = soup.find(id="block-catalogomenublock")
    elements = bar.findChildren("a", attrs={"class": "level2-link"}, recursive=True)

    categories = defaultdict(list)
    for el in elements:
        if "catalogo" in el["href"]:
            category, sub_category = el["href"].split("/")[-2:]
            categories[category].append(sub_category)
    return dict(categories)


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

    for el in elements:
        el.findChild(attrs={"class": "product-name"}, recursive=True)

    def _element_to_product_info(el: Tag) -> ProductInfo:
        div: Tag = el.findChild(attrs={"class": "product-name"}, recursive=True)
        return ProductInfo(
            code=el.attrs["data-pcode"],
            name=div.findChild().text,
            category=category,
            sub_category=sub_category,
            price=float(el.attrs["data-gtm-price"]),
        )

    return [_element_to_product_info(el) for el in elements]


def main():
    logger.info("Begin scraping categories")
    categories = get_categories()
    logger.info("Stopped scraping categories")

    with ThreadPoolExecutor() as pool:
        logger.info("Begin scraping number of pages")

        categories_and_num_pages = list(
            pool.map(
                lambda p: get_num_pages(*p),
                [
                    (category, sub_category)
                    for category in categories
                    for sub_category in categories[category]
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
    logging.basicConfig(level="INFO", format="%(asctime)s %(levelname)-8s %(message)s")
    main()
