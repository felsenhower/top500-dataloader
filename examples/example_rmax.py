"""
This example plots the Rmax of the #1 TOP500 system over time.

Download the lists before you run it.
"""

import matplotlib.pyplot as plt
import top500


def main():
    x = []
    y = []
    lists_local = list(top500.iter_lists_local(newest_first=False))
    if not lists_local:
        raise RuntimeError("Download the TOP500 lists before running this example.")
    for list_info in lists_local:
        x.append(list_info.key)
        df = top500.read_list(list_info, allow_download=False, source="normalized")
        rmax = df["r-max-gflops"][0]
        y.append(rmax)
    plt.scatter(x, y)
    plt.xticks(rotation=90)
    plt.xlabel("TOP500 list release")
    plt.ylabel("Rmax [FLOP/s]")
    plt.yscale("log")
    plt.tight_layout()
    plt.show()


if __name__ == "__main__":
    main()
