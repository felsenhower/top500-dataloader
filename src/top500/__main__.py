#!/usr/bin/env python3

import argparse
from collections.abc import Iterable

import polars as pl

import top500


def main() -> None:
    parser = argparse.ArgumentParser(
        prog="top500", description="Download or view TOP500 lists."
    )
    parser.add_argument(
        "-d",
        "--download-dir",
        action="store",
        metavar="dir",
        help=f'Set the download dir. Defaults to "{top500._DEFAULT_DOWNLOAD_DIR}".',
    )
    subparsers = parser.add_subparsers(dest="action", required=True)
    subparsers.add_parser(
        "list-online", help="List TOP500 list issues that are available online."
    )
    subparsers.add_parser(
        "list-local", help="List TOP500 list issues that are available locally."
    )
    download_parser = subparsers.add_parser(
        "download",
        help='Download a TOP500 list issue (see "download --help" for more info).',
    )
    download_parser.add_argument("key", help='The key of the list, e.g. "2025-06".')
    subparsers.add_parser(
        "download-all",
        help="Download all TOP500 list issues that are available online.",
    )
    display_parser = subparsers.add_parser(
        "display",
        help='Display a TOP500 list on the console (see "display --help" for more info).',
    )
    display_parser.add_argument("key", help='The key of the list, e.g. "2025-06".')
    args = parser.parse_args()
    if args.download_dir:
        top500.set_download_dir(args.download_dir)

    def display_list_list(lists: Iterable[top500.Top500ListInfo]) -> None:
        with pl.Config(tbl_rows=-1, fmt_str_lengths=1000):
            df = pl.DataFrame(lists)
            if len(df) == 0:
                print("No entries!")
                return
            print(df)

    match args.action:
        case "list-online":
            display_list_list(top500.iter_lists_online())
        case "list-local":
            display_list_list(top500.iter_lists_local())
        case "download":
            top500.download_list(args.key)
        case "download-all":
            top500.download_all_lists()
        case "display":
            # TODO: Add normalize=True when implemented
            df = top500.read_list(args.key)
            with pl.Config(tbl_rows=-1):
                print(df)
        case _:
            raise RuntimeError(f'Encountered an unexpected argument "{args.action}"')


if __name__ == "__main__":
    main()
