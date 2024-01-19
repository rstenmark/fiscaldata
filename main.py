import requests
import json
import polars
import matplotlib.pyplot as plt


def get_security_auctions(
    term: str, security_type: str, issued_since: str
) -> polars.dataframe:
    baseurl = """https://api.fiscaldata.treasury.gov/services/api/fiscal_service/"""
    endpoint = """v1/accounting/od/auctions_query"""
    resp = requests.get(
        f"{baseurl}{endpoint}",
        params={
            "filter": f"security_term:eq:{term},security_type:eq:{security_type},issue_date:gte:{issued_since}",
            "sort": "-issue_date",
        },
    )
    resp.raise_for_status()
    data = json.loads(resp.text)
    schema = {
        "issue_date": str,
        "cusip": str,
        "security_term": str,
        "price_per100": float,
        "bid_to_cover_ratio": float,
    }
    data_clean = []
    for row in data["data"]:
        row_copy = []
        for col in schema:
            if schema[col] == str:
                row_copy.append(row[col])
            elif schema[col] == float:
                if row[col] == "null":
                    row_copy = []
                    break
                else:
                    row_copy.append(float(row[col]))

        if not len(row_copy) == 0:
            data_clean.append(row_copy)

    ret = (
        polars.DataFrame(data_clean, schema)
        .with_columns(polars.col("issue_date").str.to_datetime())
        .set_sorted("issue_date")
    )

    return ret


if __name__ == "__main__":
    # Retrieve data
    four_week = get_security_auctions("4-Week", "Bill", "2022-01-01")
    eight_week = get_security_auctions("8-Week", "Bill", "2022-01-01")
    thirteen_week = get_security_auctions("13-Week", "Bill", "2022-01-01")
    twentysix_week = get_security_auctions("26-Week", "Bill", "2022-01-01")
    fiftytwo_week = get_security_auctions("52-Week", "Bill", "2022-01-01")

    #
    fig, ax = plt.subplots()

    # Title
    ax.set_title(f"Treasury Bill Discounted Rate by Term Length since 2022-01-01")

    # Labels
    ax.set_xlabel("Issue Date")
    ax.set_ylabel("Price per $100")

    #
    ax.step(four_week["issue_date"], four_week["price_per100"], label="4-Week")
    ax.step(eight_week["issue_date"], eight_week["price_per100"], label="8-Week")
    ax.step(thirteen_week["issue_date"], thirteen_week["price_per100"], label="13-Week")
    ax.step(
        twentysix_week["issue_date"], twentysix_week["price_per100"], label="26-Week"
    )
    ax.step(fiftytwo_week["issue_date"], fiftytwo_week["price_per100"], label="52-Week")

    #
    ax.grid(axis="both", alpha=1)
    ax.grid(axis="y", which="minor", alpha=0.8, linestyle="--")

    #
    ax.legend()
    plt.minorticks_on()
    plt.show(block=True)
