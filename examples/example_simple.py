#!/usr/bin/env python3

import top500


def main() -> None:
    for list_info in top500.iter_lists_online():
        df = top500.read_list(list_info)
        fastest_computer = df["name"][0]
        if fastest_computer is None:
            continue
        print(f"In {list_info.title}, the fastest computer was {fastest_computer}.")


if __name__ == "__main__":
    main()
