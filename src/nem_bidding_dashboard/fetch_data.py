import time

import nemosis.custom_errors
import pandas as pd
from nemosis import defaults, dynamic_data_compiler, static_table

defaults.table_columns["BIDPEROFFER_D"] += ["PASAAVAILABILITY", "ROCDOWN", "ROCUP"]

"""
Function to fetch electricity price data using NEMOSIS. Simply returns fetched
dataframe containing required data at the moment, may be updated if a different
method is used to get or store data. Also generates feather files for each
month of data that is collected.

Arguments:
    start_date: Initial datetime, formatted "DD/MM/YYYY HH:MM:SS" (time always
        set to "00:00:00:)
    end_date: Ending datetime, formatted identical to start_date
    raw_data_cache: Filepath to directory for storing data that is fetched
Returns:
    pd dataframe containing REGIONID, SETTLEMENTDATE and RRP columns
    from the DISPATCHPRICE table from AEMO, covering the specified time
    period.
"""


def get_region_data(start_time: str, end_time: str, raw_data_cache: str):
    try:
        price_data = dynamic_data_compiler(
            start_time,
            end_time,
            "DISPATCHPRICE",
            raw_data_cache,
            keep_csv=False,
            fformat="parquet",
            select_columns=["REGIONID", "SETTLEMENTDATE", "RRP", "INTERVENTION"],
        )
    except nemosis.custom_errors.NoDataToReturn:
        price_data = pd.DataFrame(
            columns=["REGIONID", "SETTLEMENTDATE", "RRP", "INTERVENTION"]
        )

    try:
        demand_data = dynamic_data_compiler(
            start_time,
            end_time,
            "DISPATCHREGIONSUM",
            raw_data_cache,
            keep_csv=False,
            fformat="parquet",
            select_columns=[
                "REGIONID",
                "SETTLEMENTDATE",
                "TOTALDEMAND",
                "INTERVENTION",
            ],
        )
    except nemosis.custom_errors.NoDataToReturn:
        demand_data = pd.DataFrame(
            columns=["REGIONID", "SETTLEMENTDATE", "TOTALDEMAND", "INTERVENTION"]
        )

    assert price_data.empty == demand_data.empty

    if not price_data.empty:
        assert price_data["SETTLEMENTDATE"].max() == demand_data["SETTLEMENTDATE"].max()
        assert price_data["SETTLEMENTDATE"].min() == demand_data["SETTLEMENTDATE"].min()

    price_and_demand_data = pd.merge(
        price_data, demand_data, on=["SETTLEMENTDATE", "REGIONID", "INTERVENTION"]
    )

    if (
        price_and_demand_data.empty
        or price_and_demand_data["SETTLEMENTDATE"].max().strftime("%Y/%m/%d %X")
        != end_time
    ):
        if not price_and_demand_data.empty:
            start_time = (
                price_and_demand_data["SETTLEMENTDATE"].max().strftime("%Y/%m/%d %X")
            )
        try:
            recent_price_and_demand_data = dynamic_data_compiler(
                start_time,
                end_time,
                "DAILY_REGION_SUMMARY",
                raw_data_cache,
                keep_csv=False,
                fformat="parquet",
                select_columns=[
                    "REGIONID",
                    "SETTLEMENTDATE",
                    "TOTALDEMAND",
                    "RRP",
                    "INTERVENTION",
                ],
            )
            price_and_demand_data = pd.concat(
                [price_and_demand_data, recent_price_and_demand_data]
            )
        except nemosis.custom_errors.NoDataToReturn:
            pass
    price_and_demand_data = price_and_demand_data.loc[
        price_and_demand_data["INTERVENTION"] == 0
    ]
    return price_and_demand_data.loc[
        :, ["REGIONID", "SETTLEMENTDATE", "TOTALDEMAND", "RRP"]
    ]


"""
TODO: update this
Function to fetch electricity demand data using NEMOSIS for use in
plot_region_demand. Simply returns fetched dataframe containing required data at
the moment, may be updated if a different method is used to get or store data.
Also generates feather files for each month of data that is collected.
Arguments:
    start_date: Initial datetime, formatted "DD/MM/YYYY HH:MM:SS" (time always
        set to "00:00:00:)
    end_date: Ending datetime, formatted identical to start_date
    raw_data_cache: Filepath to directory for storing data that is fetched
Returns:
    pd dataframe containing REGIONID, SETTLEMENTDATE and TOTALDEMAND columns
    from the DISPATCHREGIONSUM table from AEMO, covering the specified time
    period.
"""


def get_duid_availability_data(start_time: str, end_time: str, raw_data_cache: str):
    try:
        availability_data = dynamic_data_compiler(
            start_time,
            end_time,
            "DISPATCHLOAD",
            raw_data_cache,
            keep_csv=False,
            fformat="parquet",
            select_columns=[
                "SETTLEMENTDATE",
                "INTERVENTION",
                "DUID",
                "AVAILABILITY",
                "TOTALCLEARED",
                "INITIALMW",
                "RAMPDOWNRATE",
                "RAMPUPRATE",
            ],
        )
    except nemosis.custom_errors.NoDataToReturn:
        availability_data = pd.DataFrame(
            columns=[
                "SETTLEMENTDATE",
                "INTERVENTION",
                "DUID",
                "AVAILABILITY",
                "TOTALCLEARED",
                "INITIALMW",
                "RAMPDOWNRATE",
                "RAMPUPRATE",
            ]
        )

    if (
        availability_data.empty
        or availability_data["SETTLEMENTDATE"].max().strftime("%Y/%m/%d %X") != end_time
    ):
        if not availability_data.empty:
            start_time = (
                availability_data["SETTLEMENTDATE"].max().strftime("%Y/%m/%d %X")
            )
        try:
            recent_availability_data = dynamic_data_compiler(
                start_time,
                end_time,
                "NEXT_DAY_DISPATCHLOAD",
                raw_data_cache,
                keep_csv=False,
                fformat="parquet",
                select_columns=[
                    "SETTLEMENTDATE",
                    "INTERVENTION",
                    "DUID",
                    "AVAILABILITY",
                    "TOTALCLEARED",
                    "INITIALMW",
                    "RAMPDOWNRATE",
                    "RAMPUPRATE",
                ],
            )
            availability_data = pd.concat([availability_data, recent_availability_data])
        except nemosis.custom_errors.NoDataToReturn:
            pass
    availability_data = availability_data.loc[availability_data["INTERVENTION"] == 0]
    return availability_data.loc[
        :,
        [
            "SETTLEMENTDATE",
            "DUID",
            "AVAILABILITY",
            "TOTALCLEARED",
            "INITIALMW",
            "RAMPDOWNRATE",
            "RAMPUPRATE",
        ],
    ]


def get_duid_data(raw_data_cache: str):
    duid_data = static_table(
        "Generators and Scheduled Loads",
        raw_data_cache,
        select_columns=[
            "DUID",
            "Region",
            "Fuel Source - Descriptor",
            "Dispatch Type",
            "Technology Type - Descriptor",
            "Station Name",
        ],
    )
    duid_data = duid_data[~duid_data["DUID"].isin(["BLNKVIC", "BLNKTAS"])]
    duid_data.columns = duid_data.columns.str.upper()
    return duid_data


def get_volume_bids(start_time: str, end_time: str, raw_data_cache: str):
    """
    Fetch volume bid data using NEMOSIS. Caches feather file in specified directory for each month of data that is
    collected.

    Arguments:
        start_date: Initial datetime, formatted "DD/MM/YYYY HH:MM:SS" (time always
            set to "00:00:00:)
        end_date: Ending datetime, formatted identical to start_date
        raw_data_cache: Filepath to directory for storing data that is fetched
    Returns:
        pd dataframe containing quantity of bids on a 5 minutely basis. Should have columns
            INTERVAL_DATETIME, SETTLEMENTDATE, DUID, BANDAVAIL1 . . . . BANDAVAIL10
    """
    volume_bids = dynamic_data_compiler(
        start_time=start_time,
        end_time=end_time,
        table_name="BIDPEROFFER_D",
        raw_data_location=raw_data_cache,
        keep_csv=False,
        fformat="parquet",
        select_columns=[
            "INTERVAL_DATETIME",
            "SETTLEMENTDATE",
            "DUID",
            "BIDTYPE",
            "BANDAVAIL1",
            "BANDAVAIL2",
            "BANDAVAIL3",
            "BANDAVAIL4",
            "BANDAVAIL5",
            "BANDAVAIL6",
            "BANDAVAIL7",
            "BANDAVAIL8",
            "BANDAVAIL9",
            "BANDAVAIL10",
            "MAXAVAIL",
            "ROCUP",
            "ROCDOWN",
            "PASAAVAILABILITY",
        ],
    )
    return volume_bids


def get_price_bids(start_time: str, end_time: str, raw_data_cache: str):
    """
    Fetch price bid data using NEMOSIS. Caches feather file in specified directory for each month of data that is
    collected.

    Arguments:
        start_date: Initial datetime, formatted "DD/MM/YYYY HH:MM:SS" (time always
            set to "00:00:00:)
        end_date: Ending datetime, formatted identical to start_date
        raw_data_cache: Filepath to directory for storing data that is fetched
    Returns:
        pd dataframe containing prices of bids on a (market) daily basis. Should have columns
            SETTLEMENTDATE, DUID, PRICEBAND1 . . . . PRICEBAND10
    """
    volume_bids = dynamic_data_compiler(
        start_time=start_time,
        end_time=end_time,
        table_name="BIDDAYOFFER_D",
        raw_data_location=raw_data_cache,
        keep_csv=False,
        fformat="parquet",
        select_columns=[
            "SETTLEMENTDATE",
            "DUID",
            "BIDTYPE",
            "PRICEBAND1",
            "PRICEBAND2",
            "PRICEBAND3",
            "PRICEBAND4",
            "PRICEBAND5",
            "PRICEBAND6",
            "PRICEBAND7",
            "PRICEBAND8",
            "PRICEBAND9",
            "PRICEBAND10",
        ],
    )
    return volume_bids


if __name__ == "__main__":
    raw_data_cache = "D:/nemosis_cache"
    # duid_data = get_duid_data(raw_data_cache)
    # region_data = get_region_data(
    #     "2022/11/01 00:00:00", "2022/11/02 00:00:00", raw_data_cache
    # )
    # print(duid_data)
    # x = 1
    t0 = time.time()
    get_volume_bids("2020/01/23 00:00:00", "2020/01/24 00:00:00", raw_data_cache)
    print(t0 - time.time())
