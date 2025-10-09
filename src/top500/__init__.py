from typing import Iterator
from pydantic.dataclasses import dataclass
from datetime import datetime, date
from pydantic import HttpUrl
import os
from pathlib import Path
import polars as pl
import platformdirs


@dataclass
class Top500ListInfo:
    key: str
    title: str
    number: int
    published_on: date
    published_at: str
    list_url: HttpUrl
    xml_download_url: HttpUrl
    excel_download_url: HttpUrl


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


def iter_lists_online(newest_first: bool = True) -> Iterator[Top500ListInfo]:
    pass


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
