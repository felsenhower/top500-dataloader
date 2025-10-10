# top500-dataloader

This repository contains a scraper / downloader / dataloader for the [TOP500](https://top500.org/) website.

## Usage

⚠️ Since the module isn't on PyPI, I will use `uv` and `uvx` in all examples, since it works quite well with packages from git.

### As an Executable

When installed, you can invoke the CLI via

```shell
$ python -m top500 --help
usage: top500 [-h] [-d dir] {list-online,list-local,download,download-all,display} ...

Download or view TOP500 lists.

positional arguments:
  {list-online,list-local,download,download-all,display}
    list-online         List TOP500 list issues that are available online.
    list-local          List TOP500 list issues that are available locally.
    download            Download a TOP500 list issue (see "download --help" for more info).
    download-all        Download all TOP500 list issues that are available online.
    display             Display a TOP500 list on the console (see "display --help" for more info).

options:
  -h, --help            show this help message and exit
  -d, --download-dir dir
                        Set the download dir. Defaults to "/home/ruben/.local/share/top500".
```

You can also do the same thing like this with `uvx`:
```shell
$ uvx git+https://github.com/felsenhower/top500-dataloader.git --help
```

If you simply want to download all TOP500 issues to `~/.local/share/top500`, you can do this:
```shell
$ uvx git+https://github.com/felsenhower/top500-dataloader.git download-all
```

To get a nice tabular view of the available lists online:
```shell
$ uvx git+https://github.com/felsenhower/top500-dataloader.git list-online
Fetching https://top500.org/lists/top500/...
shape: (65, 6)
┌─────────┬───────────────┬────────┬──────────────┬───────────────────────────┬─────────────────────────────────────────┐
│ key     ┆ title         ┆ number ┆ published_on ┆ published_at              ┆ url                                     │
│ ---     ┆ ---           ┆ ---    ┆ ---          ┆ ---                       ┆ ---                                     │
│ str     ┆ str           ┆ i64    ┆ date         ┆ str                       ┆ object                                  │
╞═════════╪═══════════════╪════════╪══════════════╪═══════════════════════════╪═════════════════════════════════════════╡
│ 2025-06 ┆ June 2025     ┆ 65     ┆ 2025-06-14   ┆ Hamburg, Germany          ┆ https://top500.org/lists/top500/2025/06 │
│ 2024-11 ┆ November 2024 ┆ 64     ┆ 2024-11-19   ┆ Atlanta, GA, USA          ┆ https://top500.org/lists/top500/2024/11 │
│ 2024-06 ┆ June 2024     ┆ 63     ┆ 2024-06-01   ┆ Hamburg, Germany          ┆ https://top500.org/lists/top500/2024/06 │
│ 2023-11 ┆ November 2023 ┆ 62     ┆ 2023-11-14   ┆ Denver, CO, USA           ┆ https://top500.org/lists/top500/2023/11 │
│ 2023-06 ┆ June 2023     ┆ 61     ┆ 2023-06-01   ┆ Hamburg, Germany          ┆ https://top500.org/lists/top500/2023/06 │
│ 2022-11 ┆ November 2022 ┆ 60     ┆ 2022-11-15   ┆ Dallas, TX, USA           ┆ https://top500.org/lists/top500/2022/11 │
[...]
```

The `key` may be used to get a glimpse of a list like this:
```
$ uvx git+https://github.com/felsenhower/top500-dataloader.git display 2025-06
shape: (500, 7)
┌──────┬─────────────────────────────────┬──────────────────────┬─────────────────────────────────┬────────────────┬─────────────────┬────────────┐
│ Rank ┆ System Name                     ┆ Country              ┆ Manufacturer                    ┆ Rmax [GFlop/s] ┆ Rpeak [GFlop/s] ┆ Power [kW] │
│ ---  ┆ ---                             ┆ ---                  ┆ ---                             ┆ ---            ┆ ---             ┆ ---        │
│ i64  ┆ str                             ┆ str                  ┆ str                             ┆ f64            ┆ f64             ┆ f64        │
╞══════╪═════════════════════════════════╪══════════════════════╪═════════════════════════════════╪════════════════╪═════════════════╪════════════╡
│ 1    ┆ El Capitan                      ┆ United States        ┆ HPE                             ┆ 1.7420e9       ┆ 2.7464e9        ┆ 29581.0    │
│ 2    ┆ Frontier                        ┆ United States        ┆ HPE                             ┆ 1.3530e9       ┆ 2.0557e9        ┆ 24607.0    │
│ 3    ┆ Aurora                          ┆ United States        ┆ Intel                           ┆ 1.0120e9       ┆ 1.9800e9        ┆ 38698.4    │
│ 4    ┆ JUPITER Booster                 ┆ Germany              ┆ EVIDEN                          ┆ 7.934e8        ┆ 9.3e8           ┆ 13088.2    │
│ 5    ┆ Eagle                           ┆ United States        ┆ Microsoft Azure                 ┆ 5.612e8        ┆ 8.468352e8      ┆ null       │
│ 6    ┆ HPC6                            ┆ Italy                ┆ HPE                             ┆ 4.779e8        ┆ 6.0696576e8     ┆ 8460.9     │
│ 7    ┆ Supercomputer Fugaku            ┆ Japan                ┆ Fujitsu                         ┆ 4.4201e8       ┆ 5.37212e8       ┆ 29899.2    │
│ 8    ┆ Alps                            ┆ Switzerland          ┆ HPE                             ┆ 4.349e8        ┆ 5.7484128e8     ┆ 7124.0     │
│ 9    ┆ LUMI                            ┆ Finland              ┆ HPE                             ┆ 3.797e8        ┆ 5.3150515e8     ┆ 7106.8     │
[...]
```

### As a Python Module

```python
import top500

for list_info in top500.iter_lists_online():
    df = top500.read_list(list_info)
    fastest_computer = df["name"][0]
    if fastest_computer is None:
        continue
    print(f"In {list_info.title}, the fastest computer was {fastest_computer}.")
```

The module exports these functions (see [`__init__.py`](https://github.com/felsenhower/top500-dataloader/blob/main/src/top500/__init__.py) for their docstrings):

```python
def set_download_dir(download_dir: str | os.PathLike) -> None:
def get_download_dir() -> Path:
def iter_lists_online(newest_first: bool = True) -> Iterator[Top500ListInfo]:
def iter_lists_local(newest_first: bool = True) -> Iterator[Top500ListInfo]:
def download_list(list_info_or_key: str | Top500ListInfo) -> None:
def download_all_lists() -> None:
def read_list(list_info_or_key: str | Top500ListInfo, allow_download: bool = True, source: str = "normalized") -> pl.DataFrame:
```

Some Python examples are located in the [examples](examples) directory.

The `read_list` function returns a `polars.DataFrame` for the TOP500 list you request.
You can use either the key as a `str` or a `Top500ListInfo` object (but in the first case, the TOP500 overview page may be visited).
If a list is not downloaded yet, it can be automatically downloaded, unless `allow_download` is set to `False`.
The `source` argument can be `excel`, `xml`, `normalized` or `normalized-pretty`.
- `excel` will give you the data like in the Excel file (the columns are not stable).
- `xml` will give you the data like in the XML file (the columns are not stable).
- `normalized` will give you a merge of `excel` and `xml` with stable and sane columns.
- `normalized-pretty` is like `normalized`, but with prettier column names (similar to `excel`).
