import os

import postgrest

postgrest.constants.DEFAULT_POSTGREST_CLIENT_TIMEOUT = (
    15000  # Change supabase client timeout
)

import pandas as pd
from supabase import create_client

pd.set_option("display.width", None)


def region_data(start_time, end_time):
    """
    Query demand and price data from supabase. For this function to run the supabase url and key need to be configured
    as environment variables labeled SUPABASE_BIDDING_DASHBOARD_URL and SUPABASE_BIDDING_DASHBOARD_KEY
    respectively.

    Examples:
    >>> region_data("2022/01/01 00:00:00", "2022/01/02 00:00:00")

    Args:
        start_time: Initial datetime, formatted "DD/MM/YYYY HH:MM:SS"
        end_time: Ending datetime, formatted identical to start_time

    Returns:
        pd.DataFrame with columns SETTLEMENTDATE, REGIONID, TOTALDEMAND (demand to be meet by schedualed and
        semischedualed generators, not including schedualed loads), and RRP (energy price at regional reference node).
    """
    url = os.environ.get("SUPABASE_BIDDING_DASHBOARD_URL")
    key = os.environ.get("SUPABASE_BIDDING_DASHBOARD_KEY")
    supabase = create_client(url, key)
    data = (
        supabase.table("demand_data")
        .select("*")
        .gte("settlementdate", start_time)
        .lte("settlementdate", end_time)
        .execute()
    )
    data = pd.DataFrame(data.data)
    data.columns = data.columns.str.upper()
    return data


def aggregate_bids(
    regions,
    start_time,
    end_time,
    resolution,
    raw_adjusted="adjusted",
    tech_types=[],
    dispatch_type="Generator",
):
    """
    TODO
    Function to query bidding data from supabase. Data is filter according to the regions and time window provided, it
    is then aggregated into a set of predefined bins. Data can queried at hourly or 5 minute resolution. If a hourly
    resolution is chosen only bid for 5 minute interval ending on the hour are returned. For this function to run the
    supabase url and key need to be configured as environment variables labeled SUPABASE_BIDDING_DASHBOARD_URL and
    SUPABASE_BIDDING_DASHBOARD_KEY respectively.

    Examples:

    >>> aggregate_bids(
    ... ['QLD', 'NSW', 'SA'],
    ... "2020/01/21 00:00:00",
    ... "2020/01/21 01:00:00",
    ... 'hourly')
          INTERVAL_DATETIME        BIN_NAME  BIDVOLUME
    0   2020-01-21T00:00:00   [5000, 10000)     20.000
    1   2020-01-21T01:00:00         [0, 50)   3390.080
    2   2020-01-21T00:00:00         [0, 50)   3716.120
    3   2020-01-21T00:00:00     [500, 1000)    737.000
    4   2020-01-21T01:00:00       [50, 100)   1588.000
    5   2020-01-21T01:00:00    [1000, 5000)    993.000
    6   2020-01-21T00:00:00      [300, 500)    633.000
    7   2020-01-21T01:00:00   [5000, 10000)     20.000
    8   2020-01-21T00:00:00       [-100, 0)      6.746
    9   2020-01-21T01:00:00       [-100, 0)      7.614
    10  2020-01-21T00:00:00      [200, 300)   1540.000
    11  2020-01-21T00:00:00       [50, 100)   2163.000
    12  2020-01-21T01:00:00   [-1000, -100)  10366.600
    13  2020-01-21T00:00:00      [100, 200)    220.000
    14  2020-01-21T01:00:00     [500, 1000)    737.000
    15  2020-01-21T01:00:00  [10000, 15500)   4270.000
    16  2020-01-21T00:00:00   [-1000, -100)  10451.800
    17  2020-01-21T01:00:00      [100, 200)    160.000
    18  2020-01-21T00:00:00    [1000, 5000)    991.000
    19  2020-01-21T00:00:00  [10000, 15500)   3683.000
    20  2020-01-21T01:00:00      [200, 300)   2100.000
    21  2020-01-21T01:00:00      [300, 500)    783.000


    >>> aggregate_bids(
    ... ['QLD', 'NSW', 'SA'],
    ... "2020/01/21 00:00:00",
    ... "2020/01/21 00:05:00",
    ... '5-min')
          INTERVAL_DATETIME        BIN_NAME  BIDVOLUME
    0   2020-01-21T00:00:00   [5000, 10000)     20.000
    1   2020-01-21T00:00:00         [0, 50)   3716.120
    2   2020-01-21T00:05:00      [200, 300)   1550.000
    3   2020-01-21T00:00:00     [500, 1000)    737.000
    4   2020-01-21T00:05:00  [10000, 15500)   4101.000
    5   2020-01-21T00:05:00       [50, 100)   2174.000
    6   2020-01-21T00:05:00      [300, 500)    783.000
    7   2020-01-21T00:00:00      [300, 500)    633.000
    8   2020-01-21T00:00:00       [-100, 0)      6.746
    9   2020-01-21T00:00:00      [200, 300)   1540.000
    10  2020-01-21T00:00:00       [50, 100)   2163.000
    11  2020-01-21T00:05:00      [100, 200)    175.000
    12  2020-01-21T00:05:00         [0, 50)   3460.720
    13  2020-01-21T00:00:00      [100, 200)    220.000
    14  2020-01-21T00:00:00   [-1000, -100)  10451.800
    15  2020-01-21T00:00:00    [1000, 5000)    991.000
    16  2020-01-21T00:05:00   [-1000, -100)  10220.100
    17  2020-01-21T00:05:00    [1000, 5000)    993.000
    18  2020-01-21T00:05:00       [-100, 0)      5.658
    19  2020-01-21T00:00:00  [10000, 15500)   3683.000
    20  2020-01-21T00:05:00   [5000, 10000)     20.000
    21  2020-01-21T00:05:00     [500, 1000)    737.000


    Args:
        regions: list[str] regions to aggregate should only be QLD, NSW, VIC, SA or TAS.
        start_time: Initial datetime, formatted "DD/MM/YYYY HH:MM:SS"
        end_time: Ending datetime, formatted identical to start_time
        resolution: str 'hourly' or '5-min'

    Returns:
        pd.DataFrame with columns INTERVAL_DATETIME, BIN_NAME (upper and lower limits of price bin) and
        BIDVOLUME (total volume bid by units within price bin).
    """

    url = os.environ.get("SUPABASE_BIDDING_DASHBOARD_URL")
    key = os.environ.get("SUPABASE_BIDDING_DASHBOARD_KEY")
    supabase = create_client(url, key)
    data = supabase.rpc(
        "aggregate_bids_v2",
        {
            "regions": regions,
            "start_timetime": start_time,
            "end_timetime": end_time,
            "resolution": resolution,
            "dispatch_type": dispatch_type,
            "adjusted": raw_adjusted,
            "tech_types": tech_types,
        },
    ).execute()
    data = pd.DataFrame(data.data)
    data.columns = data.columns.str.upper()
    return data


def duid_bids(duids, start_time, end_time, resolution):
    """
    Function to query bidding data from supabase. Data is filter according to the regions and time window provided,
    and returned on a duid basis. Data can queryed at hourly or 5 minute resolution. If a hourly resolution is chosen
    only bid for 5 minute interval ending on the hour are returned. For this function to run the supabase url and key
    need to be configured as environment variables labeled SUPABASE_BIDDING_DASHBOARD_URL and
    SUPABASE_BIDDING_DASHBOARD_KEY respectively.

    Examples:

    >>> duid_bids(
    ... ['AGLHAL'],
    ... "2019/01/21 00:00:00",
    ... "2019/01/21 01:00:00",
    ... 'hourly')
         INTERVAL_DATETIME    DUID  BIDBAND  BIDVOLUME  BIDPRICE
    0  2019-01-21T01:00:00  AGLHAL       10        110  13600.02
    1  2019-01-21T00:00:00  AGLHAL        7         60    562.31
    2  2019-01-21T01:00:00  AGLHAL        7         60    562.31
    3  2019-01-21T00:00:00  AGLHAL       10        110  13600.02


    >>> duid_bids(
    ... ['AGLHAL'],
    ... "2019/01/21 00:00:00",
    ... "2019/01/21 00:05:00",
    ... '5-min')
         INTERVAL_DATETIME    DUID  BIDBAND  BIDVOLUME  BIDPRICE
    0  2019-01-21T00:00:00  AGLHAL        7         60    562.31
    1  2019-01-21T00:00:00  AGLHAL       10        110  13600.02
    2  2019-01-21T00:05:00  AGLHAL        7         60    562.31
    3  2019-01-21T00:05:00  AGLHAL       10        110  13600.02

    Args:
        duids: list[str] of duids to return in result.
        start_time: Initial datetime, formatted "DD/MM/YYYY HH:MM:SS"
        end_time: Ending datetime, formatted identical to start_time
        resolution: str 'hourly' or '5-min'

    Returns:
        pd.DataFrame with columns INTERVAL_DATETIME, DUID, BIDBAND, BIDVOLUME, and BIDPRICE
    """
    url = os.environ.get("SUPABASE_BIDDING_DASHBOARD_URL")
    key = os.environ.get("SUPABASE_BIDDING_DASHBOARD_KEY")
    supabase = create_client(url, key)
    data = supabase.rpc(
        "get_bids_by_unit",
        {
            "duids": duids,
            "start_timetime": start_time,
            "end_timetime": end_time,
            "resolution": resolution,
        },
    ).execute()
    data = pd.DataFrame(data.data)
    data.columns = data.columns.str.upper()
    return data


def stations_and_duids_in_regions_and_time_window(
    regions, start_date, end_date, tech_types=[], dispatch_type="Generator"
):
    """
    TODO
    Function to query units from given regions with bids available in the given time window. Data returned is DUIDs and
    corresponding Station Names. For this function to run the supabase url and key need to be configured as environment
    variables labeled SUPABASE_BIDDING_DASHBOARD_URL and SUPABASE_BIDDING_DASHBOARD_KEY respectively.

    Examples:

    >>> stations_and_duids_in_regions_and_time_window(
    ... ['NSW', 'VIC'],
    ... "2020/01/21 00:00:00",
    ... "2020/01/21 01:00:00")
             DUID                                    STATION NAME
    0       APPIN                               Appin Power Plant
    1     GUTHEGA                           Guthega Power Station
    2       HDWF1                             Hornsdale Wind Farm
    3    BLUEGSF1                           Blue Grass Solar Farm
    4     CRWASF1                               Corowa Solar Farm
    ..        ...                                             ...
    462  WOODLWN1                              Woodlawn Wind Farm
    463  WOOLNTH1  Woolnorth Studland Bay / Bluff Point Wind Farm
    464  WOOLGSF1                             Woolooga Solar Farm
    465  WYANGALA                        Wyangala A Power Station
    466   YENDWF1                                Yendon Wind Farm
    <BLANKLINE>
    [467 rows x 2 columns]

    Args:
        duids: list[str] of duids to return in result.
        start_time: Initial datetime, formatted "DD/MM/YYYY HH:MM:SS"
        end_time: Ending datetime, formatted identical to start_time

    Returns:
        pd.DataFrame with columns DUID and STATION NAME
    """
    url = os.environ.get("SUPABASE_BIDDING_DASHBOARD_URL")
    key = os.environ.get("SUPABASE_BIDDING_DASHBOARD_KEY")
    supabase = create_client(url, key)
    data = supabase.rpc(
        "get_duids_and_staions_in_regions_and_time_window_v2",
        {
            "regions": regions,
            "start_datetime": start_date,
            "end_datetime": end_date,
            "dispatch_type": dispatch_type,
            "tech_types": tech_types,
        },
    ).execute()
    data = pd.DataFrame(data.data)
    data.columns = data.columns.str.upper()
    return data


def get_aggregated_dispatch_data(regions, start_time, end_time, resolution):
    """
    Function to query dispatch data from supabase. Data is filter according to the regions and time window provided,
    and returned on a duid basis. Data can queryed at hourly or 5 minute resolution. If a hourly resolution is chosen
    only bid for 5 minute interval ending on the hour are returned. For this function to run the supabase url and key
    need to be configured as environment variables labeled SUPABASE_BIDDING_DASHBOARD_URL and
    SUPABASE_BIDDING_DASHBOARD_KEY respectively.

    Examples:

    >>> get_aggregated_dispatch_data(
    ... ['NSW'],
    ... "2020/01/21 00:00:00",
    ... "2020/01/21 01:00:00",
    ... 'hourly')
         INTERVAL_DATETIME  AVAILABILITY  ...  PASAAVAILABILITY  MAXAVAIL
    0  2020-01-21T01:00:00       11259.1  ...             14168     12986
    1  2020-01-21T00:00:00       10973.5  ...             14096     12794
    <BLANKLINE>
    [2 rows x 10 columns]


    >>> get_aggregated_dispatch_data(
    ... ['NSW'],
    ... "2020/01/21 00:00:00",
    ... "2020/01/21 00:05:00",
    ... '5-min')
         INTERVAL_DATETIME  AVAILABILITY  ...  PASAAVAILABILITY  MAXAVAIL
    0  2020-01-21T00:00:00       10973.5  ...             14096     12794
    1  2020-01-21T00:05:00       11021.5  ...             14134     12832
    <BLANKLINE>
    [2 rows x 10 columns]

    Arguments:
        duids: list[str] of duids to return in result.
        start_time: Initial datetime, formatted "DD/MM/YYYY HH:MM:SS"
        end_time: Ending datetime, formatted identical to start_time
        resolution: str 'hourly' or '5-min'

    Returns:
        pd.DataFrame containing columns INTERVAL_DATETIME, ASBIDRAMPUPMAXAVAIL (upper dispatch limit based
        on as bid ramp rate), ASBIDRAMPDOWNMINAVAIL (lower dispatch limit based on as bid ramp rate), RAMPUPMAXAVAIL (
        upper dispatch limit based lesser of as bid and telemetry ramp rates), RAMPDOWNMINAVAIL (lower dispatch limit based
        lesser of as bid and telemetry ramp rates), AVAILABILITY, TOTALCLEARED (as for after_dispatch_metrics),
        PASAAVAILABILITY, MAXAVAIL (as for as_bid_metrics), and FINALMW (the unit operating level at the end of the dispatch
        interval).
    """
    url = os.environ.get("SUPABASE_BIDDING_DASHBOARD_URL")
    key = os.environ.get("SUPABASE_BIDDING_DASHBOARD_KEY")
    supabase = create_client(url, key)
    data = supabase.rpc(
        "aggregate_dispatch_data",
        {
            "regions": regions,
            "start_timetime": start_time,
            "end_timetime": end_time,
            "resolution": resolution,
            "dispatch_type": "Generator",
            "tech_types": [],
        },
    ).execute()
    data = pd.DataFrame(data.data)
    data.columns = data.columns.str.upper()
    return data


def get_aggregated_dispatch_data_by_duids(duids, start_time, end_time, resolution):
    """
    Function to query dispatch data from supabase. Data is filter according to the duids and time window provided,
    and returned on a duid basis. Data can queryed at hourly or 5 minute resolution. If an hourly resolution is chosen
    only bid for 5 minute interval ending on the hour are returned. For this function to run the supabase url and key
    need to be configured as environment variables labeled SUPABASE_BIDDING_DASHBOARD_URL and
    SUPABASE_BIDDING_DASHBOARD_KEY respectively.

    Examples:

    >>> get_aggregated_dispatch_data_by_duids(
    ... ['AGLHAL'],
    ... "2019/01/21 00:00:00",
    ... "2019/01/21 01:00:00",
    ... 'hourly')
         interval_datetime    duid  bidband  bidvolume  bidprice
    0  2019-01-21T01:00:00  AGLHAL       10        110  13600.02
    1  2019-01-21T00:00:00  AGLHAL        7         60    562.31
    2  2019-01-21T01:00:00  AGLHAL        7         60    562.31
    3  2019-01-21T00:00:00  AGLHAL       10        110  13600.02


    >>> get_aggregated_dispatch_data_by_duids(
    ... ['AGLHAL'],
    ... "2019/01/21 00:00:00",
    ... "2019/01/21 00:05:00",
    ... '5-min')
         interval_datetime    duid  bidband  bidvolume  bidprice
    0  2019-01-21T00:00:00  AGLHAL        7         60    562.31
    1  2019-01-21T00:00:00  AGLHAL       10        110  13600.02
    2  2019-01-21T00:05:00  AGLHAL        7         60    562.31
    3  2019-01-21T00:05:00  AGLHAL       10        110  13600.02

    Args:
        duids: list[str] of duids to return in result.
        start_time: Initial datetime, formatted "DD/MM/YYYY HH:MM:SS"
        end_time: Ending datetime, formatted identical to start_time
        resolution: str 'hourly' or '5-min'

    Returns:
        pd.DataFrame containing columns INTERVAL_DATETIME, ASBIDRAMPUPMAXAVAIL (upper dispatch limit based
        on as bid ramp rate), ASBIDRAMPDOWNMINAVAIL (lower dispatch limit based on as bid ramp rate), RAMPUPMAXAVAIL (
        upper dispatch limit based lesser of as bid and telemetry ramp rates), RAMPDOWNMINAVAIL (lower dispatch limit based
        lesser of as bid and telemetry ramp rates), AVAILABILITY, TOTALCLEARED (as for after_dispatch_metrics),
        PASAAVAILABILITY, MAXAVAIL (as for as_bid_metrics), and FINALMW (the unit operating level at the end of the dispatch
        interval).
    """
    url = os.environ.get("SUPABASE_BIDDING_DASHBOARD_URL")
    key = os.environ.get("SUPABASE_BIDDING_DASHBOARD_KEY")
    supabase = create_client(url, key)
    data = supabase.rpc(
        "aggregate_dispatch_data_duids",
        {
            "duids": duids,
            "start_timetime": start_time,
            "end_timetime": end_time,
            "resolution": resolution,
        },
    ).execute()
    data = pd.DataFrame(data.data)
    data.columns = data.columns.str.upper()
    return data


def get_aggregated_vwap(regions, start_time, end_time):
    """
    Function to query aggregated Volume Weighted Average Price from supabase. Data is filter according to the regions
    and time window provided. Data can queryed at hourly or 5 minute resolution. Prices are weighted by demand in each
    region selected. For this function to run the supabase url and key need to be configured as environment variables
    labeled SUPABASE_BIDDING_DASHBOARD_URL and SUPABASE_BIDDING_DASHBOARD_KEY respectively.

    Examples:

    >>> get_aggregated_vwap(
    ... ['NSW1'],
    ... "2020/01/21 00:00:00",
    ... "2020/01/21 01:00:00")
             SETTLEMENTDATE     PRICE
    0   2020-01-21T00:45:00  59.18295
    1   2020-01-21T00:25:00  48.54982
    2   2020-01-21T00:55:00  57.18075
    3   2020-01-21T00:20:00  58.86935
    4   2020-01-21T00:50:00  56.94314
    5   2020-01-21T00:10:00  59.97000
    6   2020-01-21T00:35:00  54.01522
    7   2020-01-21T01:00:00  46.76269
    8   2020-01-21T00:30:00  48.54982
    9   2020-01-21T00:40:00  59.97000
    10  2020-01-21T00:15:00  59.97000
    11  2020-01-21T00:00:00  48.52000
    12  2020-01-21T00:05:00  60.72276

    Args:
        regions: list[str] of region to aggregate.
        start_time: Initial datetime, formatted "DD/MM/YYYY HH:MM:SS"
        end_time: Ending datetime, formatted identical to start_time

    Returns:
        pd.DataFrame with column SETTLEMENTDATE and PRICE
    """
    url = os.environ.get("SUPABASE_BIDDING_DASHBOARD_URL")
    key = os.environ.get("SUPABASE_BIDDING_DASHBOARD_KEY")
    supabase = create_client(url, key)
    data = supabase.rpc(
        "aggregate_prices",
        {
            "regions": regions,
            "start_timetime": start_time,
            "end_timetime": end_time,
        },
    ).execute()
    data = pd.DataFrame(data.data)
    data.columns = data.columns.str.upper()
    return data


def unit_types():
    """
    Function to query distinct unit types from supabase. For this function to run the supabase url and key need to be
    configured as environment variables labeled SUPABASE_BIDDING_DASHBOARD_URL and SUPABASE_BIDDING_DASHBOARD_KEY
    respectively.

    Examples:

    >>> unit_types()
                     UNIT TYPE
    0                     CCGT
    1                    Solar
    2                     Wind
    3   Pump Storage Discharge
    4           Battery Charge
    5                   Engine
    6                  Bagasse
    7              Gas Thermal
    8                    Hydro
    9      Pump Storage Charge
    10              Black Coal
    11       Battery Discharge
    12                    OCGT
    13              Brown Coal
    14      Run of River Hydro

    Returns:
        pd.DataFrame column UNIT TYPE (this is the unit type as determined by the function
        :py:func:`nem_bidding_dashboard.preprocessing.tech_namer_by_row`)
    """
    url = os.environ.get("SUPABASE_BIDDING_DASHBOARD_URL")
    key = os.environ.get("SUPABASE_BIDDING_DASHBOARD_KEY")
    supabase = create_client(url, key)
    data = supabase.rpc("distinct_unit_types", {}).execute()
    data = pd.DataFrame(data.data)
    data.columns = data.columns.str.upper()
    return data


if __name__ == "__main__":
    from time import time

    duids = stations_and_duids_in_regions_and_time_window(
        ["NSW"], "2020/01/21 00:00:00", "2020/01/30 00:00:00"
    )
    print(duids)

    t0 = time()
    data = aggregate_bids(
        ["QLD", "NSW", "SA", "VIC", "TAS"],
        "2020/01/21 00:00:00",
        "2020/01/22 00:00:00",
        "hourly",
    )
    print(time() - t0)
    print(data)

    t0 = time()
    data = aggregate_bids(
        ["QLD", "NSW", "SA", "VIC", "TAS"],
        "2020/01/21 00:00:00",
        "2020/01/22 00:00:00",
        "5-min",
    )
    print(time() - t0)
    print(data)

    t0 = time()
    data = duid_bids(
        ["AGLHAL", "ARWF1"], "2020/01/21 00:00:00", "2020/01/30 00:00:00", "hourly"
    )
    print(time() - t0)
    print(data)
