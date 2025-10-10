import csv
import os
import re
import shutil
import tarfile
import tempfile
from datetime import date, datetime
from io import BytesIO
from pathlib import Path
from tarfile import TarInfo
from typing import Iterator
from urllib.parse import urljoin

import pandas as pd
import platformdirs
import polars as pl
import requests
from bs4 import BeautifulSoup
from pydantic import HttpUrl, TypeAdapter
from pydantic.dataclasses import dataclass
from ratelimit import limits, sleep_and_retry


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


@sleep_and_retry
@limits(calls=1, period=1)
def _fetch(url):
    print(f"Fetching {url}...")
    return requests.get(url)


_TOP500_OVERVIEW_URL = HttpUrl("https://top500.org/lists/top500/")
_RE_LIST_NAME = re.compile(r"^(?:June)|(?:November) [0-9]{4}$")
_RE_LIST_HREF = re.compile(r"^([0-9]{4})/([0-9]{2})$")
_RE_LIST_KEY = re.compile(r"^([0-9]{4})-([0-9]{2})$")
_RE_DOWNLOADED_LIST_FILE = re.compile(r"^([0-9]{4})-([0-9]{2})\.tar\.gz$")
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
    for path in sorted(get_download_dir().iterdir(), reverse=newest_first):
        if not _RE_DOWNLOADED_LIST_FILE.match(path.name):
            continue
        with tarfile.open(path, "r:gz") as tar:
            meta_member = tar.getmember("metadata.json")
            meta_fp = tar.extractfile(meta_member)
            adapter = TypeAdapter(Top500ListInfo)
            list_info = adapter.validate_json(meta_fp.read())
            yield list_info


def _get_key(key_or_list_info: str | Top500ListInfo) -> str:
    if isinstance(key_or_list_info, str):
        key = key_or_list_info
    elif isinstance(key_or_list_info, Top500ListInfo):
        key = key_or_list_info.key
    assert _RE_LIST_KEY.match(key)
    return key
    raise ValueError(
        f"key_or_list_info must be either str or Top500ListInfo, passed {type(identifier)}"
    )


def _get_list_info_from_key(key: str) -> Top500ListInfo:
    print(
        "Warning: When downloading multiple lists at once, it is not recommended to specify it using only its key. Pass a Top500ListInfo instead!"
    )
    assert _RE_LIST_KEY.match(key)
    for list_info in iter_lists_online():
        if list_info.key == key:
            return list_info
    raise RuntimeError(f'List info for key "{key}" was not found online.')


def _get_list_info(key_or_list_info: str | Top500ListInfo) -> Top500ListInfo:
    if isinstance(key_or_list_info, str):
        key = key_or_list_info
        list_info = _get_list_info_from_key(key)
        return list_info
    if isinstance(key_or_list_info, Top500ListInfo):
        list_info = key_or_list_info
        return list_info
    raise ValueError(
        f"key_or_list_info must be either str or Top500ListInfo, passed {type(identifier)}"
    )


def download_list(key_or_list_info: str | Top500ListInfo) -> None:
    def download_file_from_link_text(link_text: str, anchors, tar):
        download_anchor = next(filter(lambda a: a.text == link_text, anchors), None)
        assert download_anchor is not None, ("No download link found", link_text)
        href = download_anchor["href"]
        assert href is not None
        full_download_url = HttpUrl(urljoin(str(list_info.url), href))
        response2 = _fetch(full_download_url)
        response2.raise_for_status()
        content = response2.content
        url = str(full_download_url)
        filename_from_url = url[url.rfind("/") + 1 :]
        tarinfo = TarInfo(name=filename_from_url)
        tarinfo.size = len(content)
        bio = BytesIO(content)
        tar.addfile(tarinfo, bio)
        return (filename_from_url, bio)

    def write_tsv_from_excel(excel_name, excel_buf, tar):
        # We convert the Excel files (.xlsx or .xls) to .tsv using pandas.read_excel() and pandas.to_csv().
        # We aren't using polars.read_excel(), but Unfortunately this fails on some Excel files from the TOP500 website.
        # We are using tsv instead of csv, because commas are plenty in the tables are tabs are rare.
        # However, the tabs that ARE there have no business being there in the first place and are likely a result of careless copy-pasting.
        # Therefore, it suffices to simply replace them with spaces.
        # This way, we can avoid quoting in the resulting .tsv file completely.
        df = pd.read_excel(excel_buf, dtype=str, header=None)
        df = df.replace({r"[\t\n\r]": " "}, regex=True)
        # TODO: Remove empty lines if necessary (see 1993-06)
        sio = BytesIO()
        df.to_csv(sio, index=False, header=False, sep="\t", quoting=csv.QUOTE_NONE)
        sio_len = sio.tell()
        sio.seek(0)
        tarinfo = TarInfo(name=str(Path(excel_name).with_suffix(".tsv")))
        tarinfo.size = sio_len
        tar.addfile(tarinfo, sio)

    def write_metadata(list_info, tar):
        bio = BytesIO()
        adapter = TypeAdapter(Top500ListInfo)
        json_bytes = adapter.dump_json(list_info, indent=2)
        bio = BytesIO(json_bytes)
        tarinfo = TarInfo(name="metadata.json")
        tarinfo.size = len(json_bytes)
        tar.addfile(tarinfo, bio)

    key = _get_key(key_or_list_info)

    target_path = get_download_dir() / f"{key}.tar.gz"
    if target_path.exists():
        return

    list_info = _get_list_info(key_or_list_info)

    with tempfile.NamedTemporaryFile(delete_on_close=True) as tmp:
        with tarfile.open(name=tmp.name, mode="w:gz") as tar:
            write_metadata(list_info, tar)
            response = _fetch(list_info.url)
            response.raise_for_status()
            html = BeautifulSoup(response.text, "html.parser")
            navbar = html.find(id="navbarSupportedContentSubmenu")
            anchors = navbar.find_all("a")
            download_file_from_link_text("TOP500 List (XML)", anchors, tar)
            (excel_name, excel_buf) = download_file_from_link_text(
                "TOP500 List (Excel)", anchors, tar
            )
            write_tsv_from_excel(excel_name, excel_buf, tar)

        if _download_dir is None:
            _DEFAULT_DOWNLOAD_DIR.mkdir(exist_ok=True)

        # For now, let's just copy the file for safety.
        # Later, let's fo a hard-link if supported.
        shutil.copy(tmp.name, target_path)


def download_all_lists() -> None:
    for info in iter_lists_online():
        download_list(info)


def read_list(
    key_or_list_info: str | Top500ListInfo, allow_download: bool = True
) -> pl.DataFrame:
    # TODO: Add argument normalized: bool = True
    # normalized == True ==> the columns of the dataframe and their dtype will always be the same
    # normalized == False ==> read raw
    # Current behaviour is normalized == False

    key = _get_key(key_or_list_info)
    assert _RE_LIST_KEY.match(key)
    filename = get_download_dir() / f"{key}.tar.gz"
    if not filename.exists():
        if not allow_download:
            raise RuntimeError(
                f'List "{key}" was not found locally and allow_download == False.'
            )
        download_list(key_or_list_info)
    assert filename.exists()
    with tarfile.open(filename, "r:gz") as tar:
        tsv_members = [
            member for member in tar.getmembers() if Path(member.name).suffix == ".tsv"
        ]
        assert len(tsv_members) == 1
        tsv_fp = tar.extractfile(tsv_members[0])
        df = pl.read_csv(tsv_fp, separator="\t")
        return df


def main() -> None:
    print("Hello from top500!")
    print("The main method will serve as a downloader.")
    print("TODO: Add argparse and all that...")
