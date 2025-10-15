"""
This example plots the proportion of TOP500 systems that have an accelerator over time.

Download the lists before you run it.
"""

import matplotlib.pyplot as plt
import top500


def main():
    has_acc_by_list = {}
    for list_info in top500.iter_lists_local(newest_first=False):
        df = top500.read_list(list_info, allow_download=False, source="normalized")
        assert len(df) == 500
        has_acc = list(df["accelerator"].is_null().not_())
        if sum(has_acc) > 0:
            has_acc_by_list[list_info.key] = has_acc
    else:
        raise RuntimeError("Download the TOP500 lists before running this example.")
    x = list(has_acc_by_list.keys())
    for limit in (500, 250, 50):
        y = [(100.0 * sum(l[:limit]) / limit) for l in has_acc_by_list.values()]
        plt.plot(x, y, label=f"TOP {limit}")
        print(list(zip(x, y)), limit)
    plt.xticks(rotation=90)
    plt.xlabel("TOP500 list release")
    plt.ylabel("Percentage of systems with an accelerator")
    plt.legend()
    plt.tight_layout()
    plt.show()


if __name__ == "__main__":
    main()
