import argparse
import base64
import csv
import functools
import glob
import http
import itertools
import os
import shutil
import sys
import time
import types
import typing
import urllib.parse

from requests import Response
from requests import Session
from requests import exceptions

import init

T = typing.TypeVar("T")


class Value(typing.TypedDict):
    value: str
    label: str


class Criteria(typing.TypedDict):
    name: str
    displayName: str
    values: list[Value]


class Row(typing.TypedDict):
    marketName: str
    marketBeginDate: str
    marketEndDate: str
    numberOfTrades: int
    totalVolume: int
    openPrice: types.NoneType
    highPrice: types.NoneType
    lowPrice: types.NoneType
    settlementPrice: float
    netOpenInterest: int


class Results(typing.TypedDict):
    subheader: str
    rows: list[Row]


class DataSets(typing.TypedDict):
    results: Results


class ResultsJson(typing.TypedDict):
    datasets: DataSets


BASE_URL = "https://www.ice.com"


def join_url(base: str, *paths: str) -> str:
    if not base.endswith("/"):
        base += "/"
    for path in paths:
        if path:
            base = urllib.parse.urljoin(base, path) + "/"
    return base[:-1]


def encode_data(decoded: str) -> str:
    return base64.urlsafe_b64encode(decoded.encode()).rstrip(b"=").decode()


def decode_data(encoded: str) -> str:
    return base64.urlsafe_b64decode(
        encoded.encode() + b"=" * (-len(encoded) % 8)
    ).decode()


def _update_cookies_input(url: str, session: Session):
    cookies = input(f"[#] Enter cookies<{url}>: ").strip()

    session.headers["Cookie"] = cookies


def _update_cookies_playwright(url: str, session: Session):
    # noinspection PyUnresolvedReferences,PyPackageRequirements
    from playwright.sync_api import sync_playwright

    print(f"[#] Solve reCAPTCHA<{url}>")
    with sync_playwright() as playwright:
        browser = playwright.chromium.launch_persistent_context(
            os.path.realpath("playwright"), channel="msedge", headless=False
        )
        page = browser.pages[0]
        page.goto(url)
        page.wait_for_selector('iframe[title="reCAPTCHA"]', timeout=0, state="hidden")

        for cookie in page.context.cookies():
            session.cookies.set(cookie["name"], cookie["value"])


def _update_cookies(url: str, session: Session):
    try:
        _update_cookies_playwright(url, session)
    except BaseException as _:  # NOQA
        _update_cookies_input(url, session)


class IndexFilter:
    def __init__(self, filters: str):
        self.filters = []
        for filter__ in filter(
            None, (filter_.replace(" ", "") for filter_ in filters.split(","))
        ):
            if "-" in filter__:
                start, end = filter__.split("-")
                if not start:
                    start = 0
                if not end:
                    end = sys.maxsize
                self.filters.append(range(int(start), int(end)))
            else:
                self.filters.append(range(int(filter__), -1))

    def __call__(self, index: int) -> bool:
        if self.filters:
            for filter_ in self.filters:
                if filter_.stop == -1:
                    if index == filter_.start:
                        return True
                elif index in filter_:
                    return True
        else:
            return True
        return False

    def filter(self, iterable: typing.Iterable[T]) -> typing.Iterator[T]:
        return itertools.compress(iterable, map(self, itertools.count()))


class IceReport:
    MARKET_KEY = "selectedMarket"
    TIME_PERIOD_KEY = "selectedTimePeriod"

    def __init__(
        self,
        report_id: int,
        market_filter: str,
        time_period_filter: str,
        column_filter: str,
        base_dir: str,
        base_url: str = BASE_URL,
    ):
        self.report_id = report_id
        self.market_filter = IndexFilter(market_filter)
        self.time_period_filter = IndexFilter(time_period_filter)
        self.column_filter = IndexFilter(column_filter)
        self.base_dir = base_dir

        self._cookie_url = join_url(base_url, "report", str(report_id))
        api_url = join_url(base_url, "marketdata", "api", "reports", str(report_id))
        self._criteria_url = join_url(api_url, "criteria")
        self._results_url = join_url(api_url, "results")

        self.session = Session()

    def request(self, method: http.HTTPMethod, url: str, data: dict) -> Response:
        while True:
            response = self.session.request(method, url, data=data)
            try:
                response.raise_for_status()
            except exceptions.HTTPError as error:
                print(f"[!] {error}")
                if error.response.status_code == http.HTTPStatus.CONFLICT:
                    cookie = input(f"[#] Enter cookie<{self._cookie_url}>: ")
                    self.session.headers["Cookie"] = cookie.strip()
                elif error.response.status_code == http.HTTPStatus.TOO_MANY_REQUESTS:
                    retry_after = int(response.headers["Retry-After"])
                    print(f"[#] Sleep seconds<{retry_after}>")
                    time.sleep(retry_after)
                else:
                    raise
            else:
                return response

    def get_criteria(self) -> tuple[list[str], list[str]]:
        markets = []
        time_periods = []
        response = self.request(http.HTTPMethod.GET, self._criteria_url, {})
        criterion: list[Criteria] = response.json()
        for criteria in criterion:
            if criteria["name"] == self.MARKET_KEY:
                markets = [market["value"] for market in criteria["values"]]
            elif criteria["name"] == self.TIME_PERIOD_KEY:
                time_periods = [
                    time_period["value"] for time_period in criteria["values"]
                ]
        return markets, time_periods

    @functools.lru_cache(1)
    def get_results(self, market: str, time_period: str) -> tuple[str, list[tuple]]:
        response = self.request(
            http.HTTPMethod.POST,
            self._results_url,
            {self.MARKET_KEY: market, self.TIME_PERIOD_KEY: time_period},
        )
        results: ResultsJson = response.json()
        return results["datasets"]["results"]["subheader"], [
            tuple(row.values()) for row in results["datasets"]["results"]["rows"]
        ]

    def dump(self) -> None:
        markets, time_periods = self.get_criteria()
        for time_period in self.time_period_filter.filter(time_periods):
            date = ""
            temp_dir = ""
            for market in self.market_filter.filter(markets):
                if not date:
                    date = self.get_results(market, time_period)[0]
                    temp_dir = os.path.join(self.base_dir, date, f"~{self.report_id}")
                    os.makedirs(temp_dir, exist_ok=True)
                path = os.path.join(temp_dir, f"{encode_data(market)}.csv")
                if os.path.isfile(path):
                    print(f"[~] {market!r}@{time_period!r}")
                else:
                    subheader, rows = self.get_results(market, time_period)
                    if date == subheader:
                        try:
                            with open(path, "w", newline="") as file:
                                writer = csv.writer(file)
                                writer.writerows(map(self.column_filter.filter, rows))
                        except Exception as exception:
                            print(f"[!] {exception}")
                            os.remove(path)
                            raise
                        else:
                            print(f"[+] {market!r}@{time_period!r} -> {path}")
                    else:
                        print(f"[!] {market!r}@{time_period!r} != {subheader}")

            if temp_dir and os.path.isdir(temp_dir):
                base_path = os.path.join(
                    os.path.dirname(temp_dir), f"{self.report_id}.csv"
                )
                with open(base_path, "w", newline="") as base_file:
                    writer = csv.writer(base_file)
                    for path in glob.glob(os.path.join(temp_dir, "*.csv")):
                        if os.path.isfile(path):
                            with open(path) as file:
                                reader = csv.reader(file)
                                writer.writerows(reader)
                        print(f"[<] {path} -> {date}")
                print(f"[+] {self.report_id}@{date} -> {base_path}")
                shutil.rmtree(temp_dir)


def main():
    parser = argparse.ArgumentParser(
        "pyce", formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument(
        "-v", "--version", action="version", version=f"%(prog)s {init.__version__}"
    )
    parser.add_argument(
        "report_id", type=int, help=join_url(BASE_URL, "report", "<report_id>")
    )
    parser.add_argument(
        "-m", "--market-filter", default="-", help="comma separated market filter"
    )
    parser.add_argument(
        "-t",
        "--time-period-filter",
        default="0",
        help="comma separated time period filter",
    )
    parser.add_argument(
        "-c", "--column-filter", default="-", help="comma separated column filter"
    )
    parser.add_argument(
        "-d", "--base-dir", default="pyce", help="Base output directory"
    )

    report = IceReport(**vars(parser.parse_args()))
    report.dump()


if __name__ == "__main__":
    main()
