# fiscaldata

### Purpose:

[main.py](https://github.com/rstenmark/fiscaldata/blob/master/main.py) creates a time series step chart (see Figure 1) that visualizes US Federal Treasury Bill discounted rates over time. 

### Output:

#### Figure 1:
![Figure 1](https://github.com/rstenmark/fiscaldata/blob/master/Figure_1.png?raw=true)
*Figure 1 is a time series step chart depicting US Federal Treasury Bill discounted rates (AKA "price per $100") over time, since January 1st, 2022. Bill issue dates along the x-axis. Bill discounted rates along the y-axis. Lines plotted in order from top to bottom of chart are: 4-Week, 8-Week, 13-Week, 26-Week, and 52-Week bill discounted rates.*

*Chart last updated: July 13th, 2024*

### Dependencies:

- CPython >= 3.10
- requests === 2.31.0
- polars === 0.19.3
- matplotlib === 3.8.0

### Usage:

1. Clone the repository.
```zsh
$ git clone git://github.com/rstenmark/fiscaldata
$ cd fiscaldata
```
2. Create a virtual environment (recommended, not required).
```zsh
$ python3 -m venv .
```
3. Activate the virtual environment.
```zsh
$ source ./Scripts/activate
```
4. Install Python package dependencies.
```zsh
(fiscaldata) $ python3 -m pip install -r requirements.txt
```
5. *(Optional)* Install `black` formatter.
```zsh
(fiscaldata) $ python3 -m pip install -r requirements-dev.txt
```
6. Run main.py. A window displaying a chart similar to the one above should open within a couple seconds.
```zsh
(fiscaldata) $ python3 main.py
```

---

### Details

This repository contains one CPython 3.10 script, [main.py](https://github.com/rstenmark/fiscaldata/blob/master/main.py). The purpose of this script is to produce a step chart of US Federal Treasury Bill (AKA "T-Bill" or "bill") discounted rates, over time (see Figure 1). This chart is useful for comparing historical discounted rates at a glance. 

This chart is interesting because it clearly illustrates the fact that, over a period of approximately twelve months, beginning January 1st, 2022, bills became significantly discounted. The chart also shows how this significantly discounted rate has persisted over a period of time spanning at least twelve months since January 1st, 2023.

To produce this chart, three steps are taken. First, data for the chart must be sourced. A local cache is checked for API-sourced data that is 24 hours old, or younger. A *cache miss* can occur when the cache is empty, or when all data in the cache is expired. In the event of a cache miss, data is remotely sourced from the United States Federal Treasury Fiscal Data REST API via five HTTP `GET` requests. In the event of a *cache hit*, five Python `pickle` objects are read out of the cache, and unpickled to reveal five `polars.DataFrame` objects.

The second step depends on whether a cache hit occurred in step one. If a cache hit *did not* occur, then for row in each response, the relevant data is extracted from the response JSON, transformed to an appropriate Python type, and loaded into a `polars.DataFrame` which is then sorted by bill issue date, from oldest-to-newest. 

To refresh the cache, these five `polars.DataFrame` objects are then pickled and inserted as individual rows into a SQLite3 database with columns containing: the term length of the associated bill, a hex-encoded blake2b hash of the associated pickle, the time of the row's creation, and the time of its expiry.

If a cache hit *did* occur, however, then five `DataFrame`-containing pickles are simply read from the cache and unpickled.

Step three, graphing the data, is made quite simple by the use of `polars.DataFrame` objects to store the data. A figure containing one subplot is created, the title and axes labels are specified, and five stepped lines are drawn on the single subplot using the `issue_date` and `price_per100` columns, for the x and y-axis respectively. Solid grid lines are enabled for both axes. Dotted, slightly transparent, minor grid lines are enabled on the y-axis only. The legend is enabled. 

Finally, the chart is drawn, and execution of the script is blocked until the user closes the window containing the chart. The script exits upon closure of the chart window.

---

*README.md last updated: July 13th, 2024*