"""
This example plots the Rmax of the #1 TOP500 system over time.

Download the lists before you run it.
"""

import matplotlib.pyplot as plt
import top500


def main():
    x = []
    y = []
    for list_info in top500.iter_lists_local(newest_first=False):
        x.append(list_info.key)
        df = top500.read_list(list_info, allow_download=False, source="normalized")
        rmax = df["r-max-gflops"][0]
        y.append(rmax)
    else:
        raise RuntimeError("Download the TOP500 lists before running this example.")
    plt.plot(x, y)
    plt.xticks(rotation=90)
    plt.xlabel("TOP500 list release")
    plt.ylabel("Rmax [FLOP/s]")
    plt.yscale("log")
    plt.tight_layout()
    plt.show()


if __name__ == "__main__":
    main()
