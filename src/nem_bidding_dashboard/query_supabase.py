import os

import postgrest

postgrest.constants.DEFAULT_POSTGREST_CLIENT_TIMEOUT = (
    15000  # Change supabase client timeout
)
import pandas as pd
from supabase import create_client

"""This module is used for query the database used by the dashboard. It will contain functions for both query the
   production database for the hosted version of the dashboard and functions for querying an sqlite database for use
   by user on their local machine."""


def region_data(start_date, end_date):
    """
    Function to query demand data from supabase. For this function to run the supabase url and key need to be configured
    as environment variables labeled SUPABASE_BIDDING_DASHBOARD_URL and SUPABASE_BIDDING_DASHBOARD_KEY
    respectively.

    Arguments:
        start_date: Initial datetime, formatted "DD/MM/YYYY HH:MM:SS" (time always
            set to "00:00:00:)
        end_date: Ending datetime, formatted identical to start_date
    """
    url = os.environ.get("SUPABASE_BIDDING_DASHBOARD_URL")
    key = os.environ.get("SUPABASE_BIDDING_DASHBOARD_KEY")
    supabase = create_client(url, key)
    data = (
        supabase.table("demand_data")
        .select("*")
        .gt("settlementdate", start_date)
        .lte("settlementdate", end_date)
        .execute()
    )
    data = pd.DataFrame(data.data)
    data.columns = data.columns.str.upper()
    return data


def aggregate_bids(regions, start_date, end_date, resolution):
    """
    Function to query bidding data from supabase. Data is filter according to the regions and time window provided, it
    is then aggregated into a set of predefined bins. Data can queried at hourly or 5 minute resolution. If a hourly
    resolution is chosen only bid for 5 minute interval ending on the hour are returned. For this function to run the
    supabase url and key need to be configured as environment variables labeled SUPABASE_BIDDING_DASHBOARD_URL and
    SUPABASE_BIDDING_DASHBOARD_KEY respectively.

    Examples
    --------

    >>> aggregate_bids(
    ... ['QLD', 'NSW', 'SA'],
    ... "2019/01/21 00:00:00",
    ... "2019/01/21 01:00:00",
    ... 'hourly')
          interval_datetime        bin_name  bidvolume
    0   2019-01-21T00:00:00     [500, 1000)    510.000
    1   2019-01-21T01:00:00   [5000, 10000)     10.000
    2   2019-01-21T00:00:00  [10000, 15500)   6144.000
    3   2019-01-21T01:00:00   [-1000, -100)  10084.762
    4   2019-01-21T01:00:00      [100, 200)    735.000
    5   2019-01-21T01:00:00      [300, 500)    800.000
    6   2019-01-21T01:00:00       [-100, 0)    677.408
    7   2019-01-21T00:00:00       [50, 100)   2070.000
    8   2019-01-21T01:00:00         [0, 50)   2870.107
    9   2019-01-21T00:00:00         [0, 50)   3363.297
    10  2019-01-21T01:00:00      [200, 300)   1490.000
    11  2019-01-21T00:00:00   [-1000, -100)  10220.425
    12  2019-01-21T01:00:00  [10000, 15500)   6563.000
    13  2019-01-21T00:00:00   [5000, 10000)     10.000
    14  2019-01-21T01:00:00    [1000, 5000)    508.000
    15  2019-01-21T00:00:00    [1000, 5000)    506.000
    16  2019-01-21T00:00:00      [300, 500)    805.000
    17  2019-01-21T00:00:00      [100, 200)    375.000
    18  2019-01-21T00:00:00      [200, 300)    960.000
    19  2019-01-21T01:00:00       [50, 100)   1289.000
    20  2019-01-21T00:00:00       [-100, 0)    726.576
    21  2019-01-21T01:00:00     [500, 1000)    510.000


    >>> aggregate_bids(
    ... ['QLD', 'NSW', 'SA'],
    ... "2019/01/21 00:00:00",
    ... "2019/01/21 00:05:00",
    ... '5-min')
          interval_datetime        bin_name  bidvolume
    0   2019-01-21T00:05:00      [300, 500)    780.000
    1   2019-01-21T00:05:00      [100, 200)    375.000
    2   2019-01-21T00:05:00    [1000, 5000)    508.000
    3   2019-01-21T00:05:00  [10000, 15500)   6499.000
    4   2019-01-21T00:05:00     [500, 1000)    510.000
    5   2019-01-21T00:00:00     [500, 1000)    510.000
    6   2019-01-21T00:05:00       [-100, 0)    729.197
    7   2019-01-21T00:00:00  [10000, 15500)   6144.000
    8   2019-01-21T00:05:00         [0, 50)   3056.215
    9   2019-01-21T00:00:00       [50, 100)   2070.000
    10  2019-01-21T00:05:00      [200, 300)   1220.000
    11  2019-01-21T00:00:00         [0, 50)   3363.297
    12  2019-01-21T00:00:00   [-1000, -100)  10220.425
    13  2019-01-21T00:05:00   [5000, 10000)     10.000
    14  2019-01-21T00:00:00   [5000, 10000)     10.000
    15  2019-01-21T00:00:00    [1000, 5000)    506.000
    16  2019-01-21T00:05:00   [-1000, -100)  10222.561
    17  2019-01-21T00:00:00      [300, 500)    805.000
    18  2019-01-21T00:00:00      [100, 200)    375.000
    19  2019-01-21T00:00:00      [200, 300)    960.000
    20  2019-01-21T00:00:00       [-100, 0)    726.576
    21  2019-01-21T00:05:00       [50, 100)   1730.000


    Arguments:
        regions: list[str] regions to aggregate should only be QLD, NSW, VIC, SA or TAS.
        start_date: Initial datetime, formatted "DD/MM/YYYY HH:MM:SS" (time always
            set to "00:00:00:)
        end_date: Ending datetime, formatted identical to start_date
        resolution: str 'hourly' or '5-min'
    """

    url = os.environ.get("SUPABASE_BIDDING_DASHBOARD_URL")
    key = os.environ.get("SUPABASE_BIDDING_DASHBOARD_KEY")
    supabase = create_client(url, key)
    data = supabase.rpc(
        "aggregate_bids",
        {
            "regions": regions,
            "start_datetime": start_date,
            "end_datetime": end_date,
            "resolution": resolution,
        },
    ).execute()
    data = pd.DataFrame(data.data)
    data.columns = data.columns.str.upper()
    return data


def duid_bids(duids, start_date, end_date, resolution):
    """
    Function to query bidding data from supabase. Data is filter according to the regions and time window provided,
    and returned on a duid basis. Data can queryed at hourly or 5 minute resolution. If a hourly resolution is chosen
    only bid for 5 minute interval ending on the hour are returned. For this function to run the supabase url and key
    need to be configured as environment variables labeled SUPABASE_BIDDING_DASHBOARD_URL and
    SUPABASE_BIDDING_DASHBOARD_KEY respectively.

    Examples
    --------

    >>> duid_bids(
    ... ['AGLHAL'],
    ... "2019/01/21 00:00:00",
    ... "2019/01/21 01:00:00",
    ... 'hourly')
         interval_datetime    duid  bidband  bidvolume  bidprice
    0  2019-01-21T01:00:00  AGLHAL       10        110  13600.02
    1  2019-01-21T00:00:00  AGLHAL        7         60    562.31
    2  2019-01-21T01:00:00  AGLHAL        7         60    562.31
    3  2019-01-21T00:00:00  AGLHAL       10        110  13600.02


    >>> duid_bids(
    ... ['AGLHAL'],
    ... "2019/01/21 00:00:00",
    ... "2019/01/21 00:05:00",
    ... '5-min')
         interval_datetime    duid  bidband  bidvolume  bidprice
    0  2019-01-21T00:00:00  AGLHAL        7         60    562.31
    1  2019-01-21T00:00:00  AGLHAL       10        110  13600.02
    2  2019-01-21T00:05:00  AGLHAL        7         60    562.31
    3  2019-01-21T00:05:00  AGLHAL       10        110  13600.02

    Arguments:
        duids: list[str] of duids to return in result.
        start_date: Initial datetime, formatted "DD/MM/YYYY HH:MM:SS" (time always
            set to "00:00:00:)
        end_date: Ending datetime, formatted identical to start_date
        resolution: str 'hourly' or '5-min'
    """
    url = os.environ.get("SUPABASE_BIDDING_DASHBOARD_URL")
    key = os.environ.get("SUPABASE_BIDDING_DASHBOARD_KEY")
    supabase = create_client(url, key)
    data = supabase.rpc(
        "get_bids_by_unit",
        {
            "duids": duids,
            "start_datetime": start_date,
            "end_datetime": end_date,
            "resolution": resolution,
        },
    ).execute()
    data = pd.DataFrame(data.data)
    data.columns = data.columns.str.upper()
    return data


def stations_and_duids_in_regions_and_time_window(regions, start_date, end_date):
    """
    Function to query units from given regions with bids available in the given time window. Data returned is DUIDs and
    corresponding Station Names. For this function to run the supabase url and key need to be configured as environment
    variables labeled SUPABASE_BIDDING_DASHBOARD_URL and SUPABASE_BIDDING_DASHBOARD_KEY respectively.

    Examples
    --------

    >>> stations_and_duids_in_regions_and_time_window(
    ... ['NSW'],
    ... "2019/01/21 00:00:00",
    ... "2019/01/21 01:00:00")
             DUID                STATION NAME
    0     ANGAST1      Angaston Power Station
    1       ARWF1            Ararat Wind Farm
    2       BDL01    Bairnsdale Power Station
    3       BDL02    Bairnsdale Power Station
    4    BALDHWF1        Bald Hills Wind Farm
    ..        ...                         ...
    239     YWPS1  Yallourn 'W' Power Station
    240     YWPS2  Yallourn 'W' Power Station
    241     YWPS3  Yallourn 'W' Power Station
    242     YWPS4  Yallourn 'W' Power Station
    243  YARWUN_1        Yarwun Power Station
    <BLANKLINE>
    [244 rows x 2 columns]

    Arguments:
        duids: list[str] of duids to return in result.
        start_date: Initial datetime, formatted "DD/MM/YYYY HH:MM:SS" (time always
            set to "00:00:00:)
        end_date: Ending datetime, formatted identical to start_date
    Returns:
        pd dataframe
    """
    url = os.environ.get("SUPABASE_BIDDING_DASHBOARD_URL")
    key = os.environ.get("SUPABASE_BIDDING_DASHBOARD_KEY")
    supabase = create_client(url, key)
    data = supabase.rpc(
        "get_duids_and_staions_in_regions_and_time_window",
        {
            "regions": regions,
            "start_datetime": start_date,
            "end_datetime": end_date,
        },
    ).execute()
    data = pd.DataFrame(data.data)
    data.columns = data.columns.str.upper()
    return data


def get_aggregated_dispatch_data_by_region(regions, start_date, end_date, resolution):
    """
    Function to query dispatch data from supabase. Data is filter according to the regions and time window provided,
    and returned on a duid basis. Data can queryed at hourly or 5 minute resolution. If a hourly resolution is chosen
    only bid for 5 minute interval ending on the hour are returned. For this function to run the supabase url and key
    need to be configured as environment variables labeled SUPABASE_BIDDING_DASHBOARD_URL and
    SUPABASE_BIDDING_DASHBOARD_KEY respectively.

    Examples
    --------

    >>> duid_bids(
    ... ['AGLHAL'],
    ... "2019/01/21 00:00:00",
    ... "2019/01/21 01:00:00",
    ... 'hourly')
         interval_datetime    duid  bidband  bidvolume  bidprice
    0  2019-01-21T01:00:00  AGLHAL       10        110  13600.02
    1  2019-01-21T00:00:00  AGLHAL        7         60    562.31
    2  2019-01-21T01:00:00  AGLHAL        7         60    562.31
    3  2019-01-21T00:00:00  AGLHAL       10        110  13600.02


    >>> duid_bids(
    ... ['AGLHAL'],
    ... "2019/01/21 00:00:00",
    ... "2019/01/21 00:05:00",
    ... '5-min')
         interval_datetime    duid  bidband  bidvolume  bidprice
    0  2019-01-21T00:00:00  AGLHAL        7         60    562.31
    1  2019-01-21T00:00:00  AGLHAL       10        110  13600.02
    2  2019-01-21T00:05:00  AGLHAL        7         60    562.31
    3  2019-01-21T00:05:00  AGLHAL       10        110  13600.02

    Arguments:
        duids: list[str] of duids to return in result.
        start_date: Initial datetime, formatted "DD/MM/YYYY HH:MM:SS" (time always
            set to "00:00:00:)
        end_date: Ending datetime, formatted identical to start_date
        resolution: str 'hourly' or '5-min'
    """
    url = os.environ.get("SUPABASE_BIDDING_DASHBOARD_URL")
    key = os.environ.get("SUPABASE_BIDDING_DASHBOARD_KEY")
    supabase = create_client(url, key)
    data = supabase.rpc(
        "aggregate_dispatch_data_regions",
        {
            "regions": regions,
            "start_datetime": start_date,
            "end_datetime": end_date,
            "resolution": resolution,
        },
    ).execute()
    data = pd.DataFrame(data.data)
    data.columns = data.columns.str.upper()
    return data


def get_aggregated_dispatch_data_by_duids(duids, start_date, end_date, resolution):
    """
    Function to query dispatch data from supabase. Data is filter according to the duids and time window provided,
    and returned on a duid basis. Data can queryed at hourly or 5 minute resolution. If a hourly resolution is chosen
    only bid for 5 minute interval ending on the hour are returned. For this function to run the supabase url and key
    need to be configured as environment variables labeled SUPABASE_BIDDING_DASHBOARD_URL and
    SUPABASE_BIDDING_DASHBOARD_KEY respectively.

    Examples
    --------

    >>> duid_bids(
    ... ['AGLHAL'],
    ... "2019/01/21 00:00:00",
    ... "2019/01/21 01:00:00",
    ... 'hourly')
         interval_datetime    duid  bidband  bidvolume  bidprice
    0  2019-01-21T01:00:00  AGLHAL       10        110  13600.02
    1  2019-01-21T00:00:00  AGLHAL        7         60    562.31
    2  2019-01-21T01:00:00  AGLHAL        7         60    562.31
    3  2019-01-21T00:00:00  AGLHAL       10        110  13600.02


    >>> duid_bids(
    ... ['AGLHAL'],
    ... "2019/01/21 00:00:00",
    ... "2019/01/21 00:05:00",
    ... '5-min')
         interval_datetime    duid  bidband  bidvolume  bidprice
    0  2019-01-21T00:00:00  AGLHAL        7         60    562.31
    1  2019-01-21T00:00:00  AGLHAL       10        110  13600.02
    2  2019-01-21T00:05:00  AGLHAL        7         60    562.31
    3  2019-01-21T00:05:00  AGLHAL       10        110  13600.02

    Arguments:
        duids: list[str] of duids to return in result.
        start_date: Initial datetime, formatted "DD/MM/YYYY HH:MM:SS" (time always
            set to "00:00:00:)
        end_date: Ending datetime, formatted identical to start_date
        resolution: str 'hourly' or '5-min'
    """
    url = os.environ.get("SUPABASE_BIDDING_DASHBOARD_URL")
    key = os.environ.get("SUPABASE_BIDDING_DASHBOARD_KEY")
    supabase = create_client(url, key)
    data = supabase.rpc(
        "aggregate_dispatch_data_duids",
        {
            "duids": duids,
            "start_datetime": start_date,
            "end_datetime": end_date,
            "resolution": resolution,
        },
    ).execute()
    data = pd.DataFrame(data.data)
    data.columns = data.columns.str.upper()
    return data


if __name__ == "__main__":
    from time import time

    t0 = time()
    data = aggregate_bids(
        ["QLD", "NSW", "SA", "VIC", "TAS"],
        "2019/01/21 00:00:00",
        "2019/01/30 00:00:00",
        "hourly",
    )
    print(time() - t0)
    print(data)

    t0 = time()
    data = aggregate_bids(
        ["QLD", "NSW", "SA", "VIC", "TAS"],
        "2019/01/21 00:00:00",
        "2019/01/22 00:00:00",
        "5-min",
    )
    print(time() - t0)
    print(data)

    t0 = time()
    data = duid_bids(
        ["AGLHAL", "ARWF1"], "2019/01/21 00:00:00", "2019/01/30 00:00:00", "hourly"
    )
    print(time() - t0)
    print(data)
