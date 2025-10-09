from typing import Iterator
from pydantic.dataclasses import dataclass
from datetime import datetime, date
from pydantic import HttpUrl
import os
from pathlib import Path
import polars as pl
import platformdirs
from ratelimit import limits, sleep_and_retry
import requests
import re
from bs4 import BeautifulSoup
from urllib.parse import urljoin

@dataclass
class Top500ListInfo:
    key: str
    title: str
    number: int
    published_on: date
    published_at: str
    url: HttpUrl


_DEFAULT_DOWNLOAD_DIR: Path = Path(platformdirs.user_data_dir("top500", "felsenhower"))
_download_dir: Path | None = None


def set_download_dir(download_dir: str | os.PathLike) -> None:
    download_dir = Path(download_dir)
    if not download_dir.is_dir():
        raise ValueError("Given download_dir is not a directory or does not exist.")
    global _download_dir
    _download_dir = download_dir


def get_download_dir() -> Path:
    return _download_dir or _DEFAULT_DOWNLOAD_DIR


def _prepare_download_path(path: str | os.PathLike) -> Path:
    path = Path(path)
    assert not path.is_absolute()
    download_dir = _download_dir
    if download_dir is None:
        download_dir = _DEFAULT_DOWNLOAD_DIR
        download_dir.mkdir(exist_ok=True)
    return download_dir / path


@sleep_and_retry
@limits(calls=1, period=1)
def _fetch(url):
    print(f"Fetching {url}...")
    return requests.get(url)


_TOP500_OVERVIEW_URL = HttpUrl("https://top500.org/lists/top500/")
_RE_LIST_NAME = re.compile("^(?:June)|(?:November) [0-9]{4}$")
_RE_LIST_HREF = re.compile("^([0-9]{4})/([0-9]{2})$")
_RE_LIST_DESCRIPTION = re.compile(
    r"""
    ^
    The\s+
    ([0-9]+)(?:st|nd|rd|th)
    \s+TOP500\s+List\s+was\s+published\s+
    ([a-zA-Z\.]+\s+[0-9]+,\s+[0-9]{4})
    \s+in\s+
    (.*)\.
    $
""",
    re.VERBOSE | re.DOTALL,
)
_US_STATES = {
    full_name: code
    for (code, full_name) in (
        ("AL", "Alabama"),
        ("AK", "Alaska"),
        ("AZ", "Arizona"),
        ("AR", "Arkansas"),
        ("CA", "California"),
        ("CO", "Colorado"),
        ("CT", "Connecticut"),
        ("DE", "Delaware"),
        ("FL", "Florida"),
        ("GA", "Georgia"),
        ("HI", "Hawaii"),
        ("ID", "Idaho"),
        ("IL", "Illinois"),
        ("IN", "Indiana"),
        ("IA", "Iowa"),
        ("KS", "Kansas"),
        ("KY", "Kentucky"),
        ("LA", "Louisiana"),
        ("ME", "Maine"),
        ("MD", "Maryland"),
        ("MA", "Massachusetts"),
        ("MI", "Michigan"),
        ("MN", "Minnesota"),
        ("MS", "Mississippi"),
        ("MO", "Missouri"),
        ("MT", "Montana"),
        ("NE", "Nebraska"),
        ("NV", "Nevada"),
        ("NH", "New Hampshire"),
        ("NJ", "New Jersey"),
        ("NM", "New Mexico"),
        ("NY", "New York"),
        ("NC", "North Carolina"),
        ("ND", "North Dakota"),
        ("OH", "Ohio"),
        ("OK", "Oklahoma"),
        ("OR", "Oregon"),
        ("PA", "Pennsylvania"),
        ("RI", "Rhode Island"),
        ("SC", "South Carolina"),
        ("SD", "South Dakota"),
        ("TN", "Tennessee"),
        ("TX", "Texas"),
        ("UT", "Utah"),
        ("VT", "Vermont"),
        ("VA", "Virginia"),
        ("WA", "Washington"),
        ("WV", "West Virginia"),
        ("WI", "Wisconsin"),
        ("WY", "Wyoming"),
    )
}


def iter_lists_online(newest_first: bool = True) -> Iterator[Top500ListInfo]:
    def parse_date(date_str):
        for fmt in ("%B %d, %Y", "%b %d, %Y", "%b. %d, %Y"):
            try:
                return datetime.strptime(date_str, fmt).date()
            except ValueError:
                continue
        raise ValueError(f'Unrecognized date format: "{date_str}"')

    def parse_place(place_str):
        if place_str == "Virtual":
            return place_str
        if place_str == "Hamburg":
            return "Hamburg, Germany"
        if place_str.endswith(", Germany"):
            return place_str
        if place_str == "Washington, D.C.":
            return "Washington, D.C., USA"
        for state in _US_STATES.values():
            if place_str.endswith(f", {state}"):
                return f"{place_str}, USA"
        for state in _US_STATES.keys():
            if place_str.endswith(f", {state}"):
                place_str.replace(f", {state}", f", {_US_STATES[state]}")
                return f"{place_str}, USA"
        raise ValueError(f'Unrecognized place "{place_str}"')


    response = _fetch(_TOP500_OVERVIEW_URL)
    response.raise_for_status()
    html = BeautifulSoup(response.text, "html.parser")
    ul_lists = html.find(id="squarelist")
    list_items = ul_lists.find_all("li")
    if not newest_first:
        list_items = reversed(list_items)
    for li in list_items:
        headers = li.find_all("h3")
        assert len(headers) == 1, ("More than one <h3> inside <li>", headers, li)
        header = headers[0]
        m = _RE_LIST_NAME.match(header.text)
        assert m is not None
        list_title = header.text
        anchors = li.find_all("a")
        assert len(anchors) == 1, ("More than one <a> inside <li>", anchors, li)
        anchor = anchors[0]
        href = anchor["href"]
        m = _RE_LIST_HREF.match(href)
        assert m is not None, ("Unexpected link href", href)
        list_id = f"{m[1]}-{m[2]}"
        full_list_url = HttpUrl(urljoin(str(_TOP500_OVERVIEW_URL), href))
        paragraphs = li.find_all("p")
        assert len(paragraphs) == 1, ("More than one <p> inside <li>", paragraphs, li)
        paragraph = paragraphs[0]
        p_text = paragraph.text.strip()
        m = _RE_LIST_DESCRIPTION.match(p_text)
        assert m is not None, ("Unexpected list description", p_text)
        list_number = int(m[1])
        published_date = parse_date(m[2])
        published_place = parse_place(m[3])
        yield Top500ListInfo(
            key=list_id,
            title=list_title,
            number=list_number,
            published_on=published_date,
            published_at=published_place,
            url=full_list_url,
        )


def iter_lists_local(newest_first: bool = True) -> Iterator[Top500ListInfo]:
    pass


def download_list(list_info: Top500ListInfo) -> None:
    pass


def download_all_lists() -> None:
    pass


def read_list(identifier: str | Top500ListInfo) -> pl.DataFrame:
    pass


def main() -> None:
    print("Hello from top500!")
    print("The main method will serve as a downloader.")
    print("TODO: Add argparse and all that...")
