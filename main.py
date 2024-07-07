import requests
import polars
import matplotlib.pyplot as plt
import sqlite3
import pickle
import datetime
from pathlib import Path
from hashlib import blake2b
from typing import Literal


class API:
    """US Treasury Fiscal Data API request and caching logic.
    Contains 2 subclasses:
    Cache: Implements a very simple caching layer to enable re-use of requested data and minimize redundant API calls.
    Requests: Implements logic which pulls data from the API and transforms it for use with matplotlib to generate a
    timeseries plot of historical securities auction data. Calls into the Cache class.
    """

    class Cache:
        """
        Helper class for caching data returned by calls to the US Treasury Fiscal Data API.
        Implements 3 functions:
        initialize: Creates the caching database file and table.
        insert: Stores new data into the caching database.
        pull: Retrieves data from the caching database.
        """

        # The SQLite3 database cache file is stored at this path.
        cache_file_path = Path(__file__).parent / Path("cache.sqlite3")

        # All ISO8601 timestamps are generated using the UTC+0 timezone.
        timezone = datetime.timezone.utc

        @staticmethod
        def initialize() -> None:
            """Creates a sqlite3 database file named cache.sqlite3 in .../fiscaldata/ if it does not already exist.
            Then, creates a table named 'cache' where data sourced from the US Treasury Fiscal Data API is stored. These
            operations are individually no-op if the database file already exists or the cache table already exists.

            Specifically, the cached data is sourced from the v1/accounting/od/auctions_query endpoint, which provides
            data about different types of Treasury issued securities. In the case of this script, data about Treasury
            Bills are received.
            """

            # Try to create .../fiscaldata/cache.sqlite3 or no-op if it already exists:
            try:
                with open(API.Cache.cache_file_path, "x+b") as _:
                    pass
            except FileExistsError:
                # No-op
                pass

            # Try to create a table named "cache" in .../fiscaldata/cache.sqlite3 or no-op if it already exists.

            # The table has 6 columns:
            # "id" is an integer primary key not null alias for _rowid_.
            # "term" is a not null text literal value. One of: "4-Week", "8-Week", "13-Week", "26-Week", "52-Week".
            # > These values correspond to the 5 different maturity lengths of Treasury Bills.
            # "retrieved" is a not null text ISO8601 timestamp. Values in this column indicate to the time and date
            # > when the data was retrieved from the API (really, when it was cached.)
            # "expires" is a not null text ISO8601 timestamp. Values in this column indicate when the row should be
            # > considered "stale" and not reused. By default, this timestamp is one day after the timestamp stored
            # > in the "retrieved" column of the same row.
            # data is a not null blob. These values are pickled polars DataFrames containing plotting-ready transformed
            # > data from previous API calls.
            # blake2b is a unique not null text hexadecimal blake2b hash of the pickled DataFrame stored in "data"
            # > column of the same row.

            with sqlite3.connect(API.Cache.cache_file_path) as con:
                con.execute(
                    "CREATE TABLE IF NOT EXISTS cache (id INTEGER NOT NULL, term TEXT NOT NULL, retrieved TEXT NOT "
                    "NULL, expires TEXT NOT NULL, data BLOB NOT NULL, blake2b TEXT UNIQUE NOT NULL, PRIMARY KEY (id))"
                )

        @staticmethod
        def insert(
            term: Literal["4-Week", "8-Week", "13-Week", "26-Week", "52-Week"],
            _o: object,
        ) -> None:
            """
            Inserts data retrieved from the US Treasury Fiscal Data API into the cache database after it has been
            received and parsed from JSON into a Python object by the caller. This function then serializes the
            caller's data-containing object using pickle, and stored in the cache database alongside its hash,
            the time of its retrieval (really the time of calling this function), and an expiry date (one day from
            its time of retrieval). Data can be pulled from the cache on subsequent executions of this script on the
            same day, rather than making redundant API calls.
            """
            # Serialize (pickle) data from parsed JSON response
            pickled_o = pickle.dumps(_o)
            # Calculate blake2b digest, store as ASCII string
            digest = blake2b(pickled_o).hexdigest()
            # Use UTC+0 as frame of reference for time of retrieval/expiry.
            # Time of retrieval is whenever this function is called, not when the data was truly retrieved.
            # Subsequent loss of accuracy shouldn't matter for this application.
            retrieved = datetime.datetime.now(tz=API.Cache.timezone)
            # Time of expiry is the time of retrieval plus one day.
            expires = retrieved + datetime.timedelta(days=1)
            # INSERT the data into the cache. If there is a conflict caused by trying to insert data with a non-unique
            # hash, use the conflict resolution algorithm IGNORE.
            args = (None, term, retrieved, expires, pickled_o, digest)
            with sqlite3.connect(API.Cache.cache_file_path) as con:
                con.execute(
                    "INSERT OR IGNORE INTO cache VALUES (?, ?, ?, ?, ?, ?)", args
                )

        @staticmethod
        def pull(
            term: Literal["4-Week", "8-Week", "13-Week", "26-Week", "52-Week"]
        ) -> polars.DataFrame | None:
            """Pulls any unexpired DataFrame for the specified bill maturity if there is any. Otherwise,
            returns None."""

            # Select data from up to one row from the cache database.
            with sqlite3.connect(API.Cache.cache_file_path) as con:
                data = con.execute(
                    "SELECT data FROM cache WHERE expires >= ? AND term == ? LIMIT 1",
                    (datetime.datetime.now(tz=API.Cache.timezone), term),
                ).fetchone()

            # If any data was returned by the query, it has to be un-pickled before it can be returned to the caller:
            if data is not None:
                return pickle.loads(data[0])
            else:
                # No data was found in the cache, return None.
                return None

    class Requests:
        """ """

        url = """https://api.fiscaldata.treasury.gov/services/api/fiscal_service/"""

        @staticmethod
        def get_security_auctions(
            term: Literal["4-Week", "8-Week", "13-Week", "26-Week", "52-Week"],
            security_type: str,
            issued_since: str,
            use_cache=True,
        ) -> polars.dataframe:
            """
            Requests historical data on securities auctions from the US Treasury Fiscal Data API, specifically
            Treasury Bills. Then, parses the returned JSON into a Python object, and transforms it into a polars
            DataFrame sorted by each auction's issue date from oldest to newest. If the optional argument use_cache
            is true, then this function will return data from the cache first, if any is available. If there is not any
            cached data, then an API call will be made instead. Responses to successful API calls are cached regardless
            of whether use_cache is true or not, unless the data is already present in the cache.

            See:
            ttps://fiscaldata.treasury.gov/datasets/treasury-securities-auctions-data/treasury-securities-auctions-data
            """
            endpoint = """v1/accounting/od/auctions_query"""

            # If use_cache is specified and there is unexpired data matching the specified term, try to skip the API
            # call and returned the cached data instead.
            if use_cache:
                ret = API.Cache.pull(term)
                if ret is not None:
                    return ret

            # Make an HTTP GET request to the remote API service for fresh data
            response = requests.get(
                f"{API.Requests.url}{endpoint}",
                params={
                    "filter": f"security_term:eq:{term},security_type:eq:{security_type},issue_date:gte:{issued_since}",
                    "sort": "-issue_date",
                },
            )
            # Raise an error if the server responds with an HTTP error code
            response.raise_for_status()

            # Mapping of API response data column names to expected value types
            response_schema: dict[str : str | float] = {
                "issue_date": str,
                "cusip": str,
                "security_term": str,
                "price_per100": float,
                "bid_to_cover_ratio": float,
            }

            # The API responds with a JSON string containing several rows of dictionaries. To plot the relevant data,
            # (bill discounted rate over issued date) it needs to be extracted and transformed to a polars DataFrame.
            # Some rows may contain strings equivalent to "null" where we might otherwise expect a numeric float value.
            # Those rows are discarded since they're not useful for plotting.

            # I think this transformation operation could be cleaner.

            # The transformed data is copied into a list of lists containing string and float values in the order
            # indicated by the response schema specified above.
            data_transform: list[list[str | float]] = []

            # For each row containing a dictionary in the data provided by the API,
            for response_row_dict in response.json()["data"]:
                # The row-wise transformed data will be copied into a list.
                row_transform: list[str | float] = []

                # Use each column name specified in the schema as a key in the response data row dictionary.
                for column_name, value_type in response_schema.items():
                    # This value should be a string:
                    if value_type == str:
                        # Make sure the value contained is actually a string.
                        assert isinstance(response_row_dict[column_name], str)
                        # Append this value to the transformed row.
                        row_transform.append(response_row_dict[column_name])

                    # This value should be a string representing a float.
                    # It could also be a string equivalent to "null".
                    elif value_type == float:
                        # Make sure the value is a string.
                        assert isinstance(response_row_dict[column_name], str)
                        if response_row_dict[column_name] == "null":
                            # If this value is a string "null", assign an empty list to row_transform to indicate
                            # that this row should be discarded from the transform.
                            row_transform = []
                            break
                        else:
                            try:
                                # Try to interpret the string as a float.
                                value = float(response_row_dict[column_name])
                            except ValueError as exc:
                                exc.add_note(
                                    f"The string {response_row_dict[column_name]} in column {column_name}"
                                    f"cannot be interpreted as a float value."
                                )
                                raise exc
                            # Append this value to the transformed row.
                            row_transform.append(value)

                # Append this non-zero length transformed row to the list of transformed rows.
                if not len(row_transform) == 0:
                    data_transform.append(row_transform)

            # Package the transformed data into a polars DataFrame. The DataFrame column types are specified to match
            # the schema described above. The DataFrame is sorted by the bill issue dates, from oldest to newest.
            ret = (
                polars.DataFrame(data_transform, response_schema)
                .with_columns(polars.col("issue_date").str.to_datetime())
                .set_sorted("issue_date")
            )

            # Write the transformed data into the cache database.
            API.Cache.insert(term, ret)

            # Return the transformed data to the caller.
            return ret


if __name__ == "__main__":
    # Initialize the API response cache database. No-op if already initialized.
    API.Cache.initialize()

    # Retrieve data.
    _use_cache = True
    four_week = API.Requests.get_security_auctions(
        "4-Week", "Bill", "2022-01-01", _use_cache
    )
    eight_week = API.Requests.get_security_auctions(
        "8-Week", "Bill", "2022-01-01", _use_cache
    )
    thirteen_week = API.Requests.get_security_auctions(
        "13-Week", "Bill", "2022-01-01", _use_cache
    )
    twentysix_week = API.Requests.get_security_auctions(
        "26-Week", "Bill", "2022-01-01", _use_cache
    )
    fiftytwo_week = API.Requests.get_security_auctions(
        "52-Week", "Bill", "2022-01-01", _use_cache
    )

    # Create a figure containing one subplot
    fig, ax = plt.subplots()

    # Title
    ax.set_title(f"Treasury Bill Discounted Rate by Term Length since 2022-01-01")

    # Labels
    ax.set_xlabel("Issue Date")
    ax.set_ylabel("Price per $100")

    # Create five step-plots for each security type, all using the same figure
    ax.step(four_week["issue_date"], four_week["price_per100"], label="4-Week")
    ax.step(eight_week["issue_date"], eight_week["price_per100"], label="8-Week")
    ax.step(thirteen_week["issue_date"], thirteen_week["price_per100"], label="13-Week")
    ax.step(
        twentysix_week["issue_date"], twentysix_week["price_per100"], label="26-Week"
    )
    ax.step(fiftytwo_week["issue_date"], fiftytwo_week["price_per100"], label="52-Week")

    # Enable solid gridlines on both axes
    ax.grid(axis="both", alpha=1)
    # Enable dotted minor gridlines on the y-axis only (running horizontally across the figure)
    ax.grid(axis="y", which="minor", alpha=0.8, linestyle="--")

    # Enable the legend
    ax.legend()

    # Enable minor ticks
    plt.minorticks_on()

    # Display figures and block until they are closed by the user
    plt.show(block=True)
