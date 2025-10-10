#!/usr/bin/env python3

import top500


def main() -> None:
    print("Downloading all TOP500 lists...")
    top500.download_all_lists()
    downloaded_lists = list(top500.iter_lists_local(newest_first=False))
    print(f"Found {len(downloaded_lists)} locally.")
    print("Newest list:")
    print(downloaded_lists[-1])
    print("Oldest list:")
    print(downloaded_lists[0])


if __name__ == "__main__":
    main()
