from datetime import datetime, timedelta

import numpy as np
import pandas as pd

from nem_bidding_dashboard import defaults, fetch_and_preprocess

pd.set_option("display.width", None)


def region_demand(regions, start_time, end_time, raw_data_cache):
    """
    Query demand and price data from the raw data cache. To aggregate demand data is summed.

    Examples:

    >>> region_demand(
    ... regions=['NSW'],
    ... start_time="2022/01/01 01:00:00",
    ... end_time="2022/01/01 01:30:00",
    ... raw_data_cache="D:/nemosis_data_cache")
            SETTLEMENTDATE  TOTALDEMAND
    0  2022-01-01 01:05:00      6631.21
    1  2022-01-01 01:10:00      6655.52
    2  2022-01-01 01:15:00      6496.85
    3  2022-01-01 01:20:00      6520.86
    4  2022-01-01 01:25:00      6439.22
    5  2022-01-01 01:30:00      6429.13

    Arguments:
        regions: regions to aggregate demand and price from
        start_time: Initial datetime, formatted "DD/MM/YYYY HH:MM:SS"
        end_time: Ending datetime, formatted identical to start_time
        raw_data_cache: Filepath to directory for caching files downloaded from AEMO

    Returns:
        pd.DataFrame with columns SETTLEMENTDATE and TOTALDEMAND (demand to be meet by schedualed and
        semischedualed generators, not including schedualed loads)
    """
    data = fetch_and_preprocess.region_data(start_time, end_time, raw_data_cache)
    data = data.loc[data["REGIONID"].isin(regions), :]
    data = data.groupby("SETTLEMENTDATE", as_index=False).agg({"TOTALDEMAND": "sum"})
    return (
        data.loc[:, ["SETTLEMENTDATE", "TOTALDEMAND"]]
        .sort_values(["SETTLEMENTDATE"])
        .reset_index(drop=True)
    )


def aggregate_bids(
    regions,
    start_time,
    end_time,
    resolution,
    adjusted,
    tech_types,
    dispatch_type,
    raw_data_cache,
):
    """
    Function to query and aggregate bidding data from raw data cache database. Data is filter according to the regions,
    dispatch type, tech types and time window provided, it is then aggregated into a set of predefined bins.
    Data can be queried at hourly or 5 minute resolution. If an hourly resolution is chosen only bid for 5 minute
    interval ending on the hour are returned.

    Examples:

    >>> aggregate_bids(
    ... ['QLD', 'NSW', 'SA'],
    ... "2022/01/01 01:00:00",
    ... "2022/01/01 02:00:00",
    ... 'hourly',
    ... 'adjusted',
    ... [],
    ... 'Generator',
    ... 'D:/nemosis_data_cache')
          INTERVAL_DATETIME        BIN_NAME   BIDVOLUME
    0   2022-01-01 02:00:00   [-1000, -100)  9158.02765
    1   2022-01-01 02:00:00       [-100, 0)   299.74402
    2   2022-01-01 02:00:00         [0, 50)  1142.00000
    3   2022-01-01 02:00:00       [50, 100)  1141.00000
    4   2022-01-01 02:00:00      [100, 200)   918.00000
    5   2022-01-01 02:00:00      [200, 300)  1138.00000
    6   2022-01-01 02:00:00      [300, 500)   920.00000
    7   2022-01-01 02:00:00     [500, 1000)   273.00000
    8   2022-01-01 02:00:00    [1000, 5000)   210.00000
    9   2022-01-01 02:00:00   [5000, 10000)   125.00000
    10  2022-01-01 02:00:00  [10000, 15500)  7009.00000


    >>> aggregate_bids(
    ... ['QLD', 'NSW', 'SA'],
    ... "2022/01/01 01:00:00",
    ... "2022/01/01 01:05:00",
    ... '5-min',
    ... 'adjusted',
    ... [],
    ... 'Generator',
    ... 'D:/nemosis_data_cache')
          INTERVAL_DATETIME        BIN_NAME   BIDVOLUME
    0   2022-01-01 01:05:00   [-1000, -100)  9642.26127
    1   2022-01-01 01:05:00       [-100, 0)   361.94456
    2   2022-01-01 01:05:00         [0, 50)  1348.00000
    3   2022-01-01 01:05:00       [50, 100)  1415.00000
    4   2022-01-01 01:05:00      [100, 200)   912.00000
    5   2022-01-01 01:05:00      [200, 300)  1188.00000
    6   2022-01-01 01:05:00      [300, 500)   903.00000
    7   2022-01-01 01:05:00     [500, 1000)   272.00000
    8   2022-01-01 01:05:00    [1000, 5000)   210.00000
    9   2022-01-01 01:05:00   [5000, 10000)   125.00000
    10  2022-01-01 01:05:00  [10000, 15500)  6853.00000


    Arguments:
        regions: list[str] regions to aggregate should only be QLD, NSW, VIC, SA or TAS.
        start_time: Initial datetime, formatted "DD/MM/YYYY HH:MM:SS"
        end_time: Ending datetime, formatted identical to start_time
        resolution: str 'hourly' or '5-min'
        dispatch_type: str 'Generator' or 'Load'
        adjusted: str which bid data to use aggregate 'raw' or 'adjusted'. Adjusted bid data has been
            adjusted down so the total bid does not exceed the unit availability.
        tech_types: list[str] the technology types to filter for e.g. Solar, Black Coal, CCGT. An empty list
            will result in no filtering by technology
        raw_data_cache: Filepath to directory for caching files downloaded from AEMO

    Returns:
        pd.DataFrame with columns INTERVAL_DATETIME, BIN_NAME (upper and lower limits of price bin) and
        BIDVOLUME (total volume bid by units within price bin).
    """
    bids = fetch_and_preprocess.bid_data(start_time, end_time, raw_data_cache)

    print(bids["BIDVOLUME"].sum())

    if resolution == "hourly":
        bids = bids[bids["INTERVAL_DATETIME"].str[14:16] == "00"].copy()

    print(bids["BIDVOLUME"].sum())

    unit_info = fetch_and_preprocess.duid_info(raw_data_cache)

    unit_info = unit_info[unit_info["REGION"].isin(regions)].copy()
    unit_info = unit_info[unit_info["DISPATCH TYPE"] == dispatch_type].copy()

    print(bids["BIDVOLUME"].sum())

    if tech_types:
        unit_info = unit_info[unit_info["UNIT TYPE"].isin(tech_types)].copy()
    print(bids["BIDVOLUME"].sum())
    bids = bids[bids["DUID"].isin(unit_info["DUID"])].copy()

    print(unit_info.drop_duplicates()["DUID"].shape)
    print(bids["BIDVOLUME"].sum())

    bins = fetch_and_preprocess.define_and_return_price_bins()

    bids["d"] = 1
    bins["d"] = 1

    print(bids["BIDVOLUME"].sum())

    bids = pd.merge(bids, bins, on="d")
    bids = bids.drop(columns=["d"])

    bids = bids[
        (bids["BIDPRICE"] >= bids["LOWER_EDGE"])
        & (bids["BIDPRICE"] < bids["UPPER_EDGE"])
    ].copy()

    if adjusted == "raw":
        bids = bids.groupby(["INTERVAL_DATETIME", "BIN_NAME"], as_index=False).agg(
            {"BIDVOLUME": "sum"}
        )
    elif adjusted == "adjusted":
        bids = bids.groupby(["INTERVAL_DATETIME", "BIN_NAME"], as_index=False).agg(
            {"BIDVOLUMEADJUSTED": "sum"}
        )
        bids = bids.rename(columns={"BIDVOLUMEADJUSTED": "BIDVOLUME"})

    bids["BIN_NAME"] = bids["BIN_NAME"].astype("category")
    bids["BIN_NAME"] = bids["BIN_NAME"].cat.set_categories(defaults.bid_order)
    bids = bids.sort_values(["INTERVAL_DATETIME", "BIN_NAME"]).reset_index(drop=True)
    bids["BIN_NAME"] = bids["BIN_NAME"].astype(str)
    bids["BIDVOLUME"] = bids["BIDVOLUME"].astype(float)
    return bids


def duid_bids(duids, start_time, end_time, resolution, adjusted, raw_data_cache):
    """
    Function to query bidding data from a raw data cache. Data is filter according to the duids and time window
    provided, and returned on a duid basis. Data can queryed at hourly or 5 minute resolution. If an hourly resolution
    is chosen only bid for 5 minute interval ending on the hour are returned.

    Examples:

    >>> duid_bids(
    ... ['AGLHAL', 'BASTYAN'],
    ... "2022/01/01 01:00:00",
    ... "2022/01/01 02:00:00",
    ... 'hourly',
    ... 'adjusted',
    ... 'D:/nemosis_data_cache')
         INTERVAL_DATETIME     DUID  BIDBAND  BIDVOLUME  BIDPRICE
    0  2022-01-01 02:00:00   AGLHAL        7       32.0    557.39
    1  2022-01-01 02:00:00   AGLHAL       10      121.0  14541.30
    2  2022-01-01 02:00:00  BASTYAN        2       53.0    -55.64
    3  2022-01-01 02:00:00  BASTYAN        4       28.0     -0.91
    4  2022-01-01 02:00:00  BASTYAN       10        0.0  14021.86


    >>> duid_bids(
    ... ['AGLHAL', 'BASTYAN'],
    ... "2022/01/01 01:00:00",
    ... "2022/01/01 01:05:00",
    ... '5-min',
    ... 'adjusted',
    ... 'D:/nemosis_data_cache')
         INTERVAL_DATETIME     DUID  BIDBAND  BIDVOLUME  BIDPRICE
    0  2022-01-01 01:05:00   AGLHAL        7       32.0    557.39
    1  2022-01-01 01:05:00   AGLHAL       10      121.0  14541.30
    2  2022-01-01 01:05:00  BASTYAN        2       53.0    -55.64
    3  2022-01-01 01:05:00  BASTYAN        4       28.0     -0.91
    4  2022-01-01 01:05:00  BASTYAN       10        0.0  14021.86

    Arguments:
        duids: list[str] of duids to return in result.
        start_time: Initial datetime, formatted "DD/MM/YYYY HH:MM:SS"
        end_time: Ending datetime, formatted identical to start_time
        resolution: str 'hourly' or '5-min'
        adjusted: str which bid data to use aggregate 'raw' or 'adjusted'. Adjusted bid data has been
            adjusted down so the total bid does not exceed the unit availability.
        raw_data_cache: Filepath to directory for caching files downloaded from AEMO

    Returns:
        pd.DataFrame with columns INTERVAL_DATETIME, DUID, BIDBAND, BIDVOLUME, and BIDPRICE
    """
    bids = fetch_and_preprocess.bid_data(start_time, end_time, raw_data_cache)
    bids = bids[bids["DUID"].isin(duids)].copy()

    if resolution == "hourly":
        bids = bids[bids["INTERVAL_DATETIME"].str[14:16] == "00"].copy()

    if adjusted == "adjusted":
        bids = bids.loc[
            :, ["INTERVAL_DATETIME", "DUID", "BIDBAND", "BIDVOLUMEADJUSTED", "BIDPRICE"]
        ]
        bids = bids.rename(columns={"BIDVOLUMEADJUSTED": "BIDVOLUME"})
    elif adjusted == "raw":
        bids = bids.loc[
            :, ["INTERVAL_DATETIME", "DUID", "BIDBAND", "BIDVOLUME", "BIDPRICE"]
        ]
    bids["BIDVOLUME"] = bids["BIDVOLUME"].astype(float)
    return bids.sort_values(["INTERVAL_DATETIME", "DUID", "BIDBAND"]).reset_index(
        drop=True
    )


def stations_and_duids_in_regions_and_time_window(
    regions, start_time, end_time, dispatch_type, tech_types, raw_data_cache
):
    """
    Function to query units from given regions with bids available in the given time window, with the given dispatch
    and technology type. Data returned is DUIDs and corresponding Station Names.

    Examples:

    >>> stations_and_duids_in_regions_and_time_window(
    ... ['NSW'],
    ... "2022/01/01 01:00:00",
    ... "2022/01/01 02:00:00",
    ... "Generator",
    ... [],
    ... 'D:/nemosis_data_cache')
            DUID             STATION NAME
    0   BANGOWF1      Bango 973 Wind Farm
    1   BANGOWF2      Bango 999 Wind Farm
    2   BERYLSF1         Beryl Solar Farm
    3   BLOWERNG  Blowering Power Station
    4   BOCORWF1      Boco Rock Wind Farm
    ..       ...                      ...
    69  WALGRVG1         Wallgrove BESS 1
    70   WELLSF1    Wellington Solar Farm
    71  WOODLWN1       Woodlawn Wind Farm
    72     WRSF1    White Rock Solar Farm
    73     WRWF1     White Rock Wind Farm
    <BLANKLINE>
    [74 rows x 2 columns]

    Arguments:
        regions: list[str] regions to aggregate should only be QLD, NSW, VIC, SA or TAS.
        start_time: Initial datetime, formatted "DD/MM/YYYY HH:MM:SS"
        end_time: Ending datetime, formatted identical to start_time
        dispatch_type: str 'Generator' or 'Load'
        tech_types: list[str] the technology types to filter for e.g. Solar, Black Coal, CCGT. An empty list
            will result in no filtering by technology
        raw_data_cache: Filepath to directory for caching files downloaded from AEMO

    Returns:
        pd.DataFrame with columns DUID and STATION NAME
    """
    bids = fetch_and_preprocess.bid_data(start_time, end_time, raw_data_cache)

    duids = bids["DUID"].unique()

    unit_info = fetch_and_preprocess.duid_info(raw_data_cache)

    unit_info = unit_info[unit_info["REGION"].isin(regions)].copy()
    unit_info = unit_info[unit_info["DISPATCH TYPE"] == dispatch_type].copy()

    if tech_types:
        unit_info = unit_info[unit_info["UNIT TYPE"].isin(tech_types)].copy()

    unit_info = unit_info[unit_info["DUID"].isin(duids)].copy()

    return (
        unit_info.loc[:, ["DUID", "STATION NAME"]]
        .sort_values("DUID")
        .reset_index(drop=True)
    )


def get_aggregated_dispatch_data(
    column_name,
    regions,
    start_time,
    end_time,
    resolution,
    dispatch_type,
    tech_types,
    raw_data_cache,
):
    """
    Function to query dispatch data from a raw data cache. Data is filter according to the regions, time window,
    dispatch type, and technology type  provided, and returned on a SETTLEMENTDATE basis. Data can queryed at hourly or
    5 minute resolution. If an hourly resolution is chosen only data for 5 minute interval ending on the hour are
    returned.

    Examples:

    >>> get_aggregated_dispatch_data(
    ... 'AVAILABILITY',
    ... ['NSW'],
    ... "2022/01/01 01:00:00",
    ... "2022/01/01 02:00:00",
    ... 'hourly',
    ... 'Generator',
    ... [],
    ... 'D:/nemosis_data_cache')
         INTERVAL_DATETIME  COLUMNVALUES
    0  2022-01-01 02:00:00   10402.47408


    >>> get_aggregated_dispatch_data(
    ... 'AVAILABILITY',
    ... ['NSW'],
    ... "2022/01/01 01:00:00",
    ... "2022/01/01 01:05:00",
    ... '5-min',
    ... 'Generator',
    ... [],
    ... 'D:/nemosis_data_cache')
         INTERVAL_DATETIME  COLUMNVALUES
    0  2022-01-01 01:05:00   10440.10679

    Arguments:
        column_name: str, which column of dispatch data to aggregate and return. Should be one of NTERVAL_DATETIME,
            ASBIDRAMPUPMAXAVAIL (upper dispatch limit based on as bid ramp rate, when aggregated unit contribution cannot
            exceed MAXAVAIL), ASBIDRAMPDOWNMINAVAIL (lower dispatch limit based on as bid ramp rate, when aggregated unit
            contribution cannot be less than zero), RAMPUPMAXAVAIL (upper dispatch limit based lesser of as bid and
            telemetry ramp rates, when aggregated unit contribution cannot exceed AVAILABILITY), RAMPDOWNMINAVAIL (lower
            dispatch limit based lesser of as bid and telemetry ramp rates, when aggregated unit contribution cannot be less
            than zero), AVAILABILITY, TOTALCLEARED (as for after_dispatch_metrics), PASAAVAILABILITY, MAXAVAIL (as for
            as_bid_metrics), and FINALMW (the unit operating level at the end of the dispatch interval).
        regions: list[str] regions to aggregate should only be QLD, NSW, VIC, SA or TAS.
        start_time: Initial datetime, formatted "DD/MM/YYYY HH:MM:SS"
        end_time: Ending datetime, formatted identical to start_time
        resolution: str 'hourly' or '5-min'
        dispatch_type: str 'Generator' or 'Load'
        tech_types: list[str] the technology types to filter for e.g. Solar, Black Coal, CCGT. An empty list
            will result in no filtering by technology
        raw_data_cache: Filepath to directory for caching files downloaded from AEMO

    Returns:
        pd.DataFrame containing columns INTERVAL_DATETIME, COLUMNVALUES (aggregate of column specified in input)
    """
    if resolution == "hourly":
        end_time_dt = datetime.strptime(end_time, "%Y/%m/%d %H:%M:%S") + timedelta(
            hours=1
        )
    else:
        end_time_dt = datetime.strptime(end_time, "%Y/%m/%d %H:%M:%S") + timedelta(
            minutes=5
        )
    end_time = datetime.strftime(end_time_dt, "%Y/%m/%d %H:%M:%S")

    dispatch = fetch_and_preprocess.unit_dispatch(start_time, end_time, raw_data_cache)

    if resolution == "hourly":
        dispatch = dispatch[dispatch["INTERVAL_DATETIME"].str[14:16] == "00"].copy()

    unit_info = fetch_and_preprocess.duid_info(raw_data_cache)

    unit_info = unit_info[unit_info["REGION"].isin(regions)].copy()
    unit_info = unit_info[unit_info["DISPATCH TYPE"] == dispatch_type].copy()

    if tech_types:
        unit_info = unit_info[unit_info["UNIT TYPE"].isin(tech_types)].copy()

    dispatch = dispatch[dispatch["DUID"].isin(unit_info["DUID"])].copy()

    dispatch["ASBIDRAMPUPMAXAVAIL"] = np.where(
        dispatch["ASBIDRAMPUPMAXAVAIL"] > dispatch["MAXAVAIL"],
        dispatch["MAXAVAIL"],
        dispatch["ASBIDRAMPUPMAXAVAIL"],
    )

    dispatch["ASBIDRAMPDOWNMINAVAIL"] = np.where(
        dispatch["ASBIDRAMPDOWNMINAVAIL"] < 0.0, 0.0, dispatch["ASBIDRAMPDOWNMINAVAIL"]
    )

    dispatch["RAMPUPMAXAVAIL"] = np.where(
        dispatch["RAMPUPMAXAVAIL"] > dispatch["AVAILABILITY"],
        dispatch["AVAILABILITY"],
        dispatch["RAMPUPMAXAVAIL"],
    )

    dispatch["RAMPDOWNMINAVAIL"] = np.where(
        dispatch["RAMPDOWNMINAVAIL"] < 0.0, 0.0, dispatch["RAMPDOWNMINAVAIL"]
    )

    dispatch = dispatch.groupby("INTERVAL_DATETIME", as_index=False).agg(
        {
            "AVAILABILITY": "sum",
            "TOTALCLEARED": "sum",
            "FINALMW": "sum",
            "ASBIDRAMPUPMAXAVAIL": "sum",
            "ASBIDRAMPDOWNMINAVAIL": "sum",
            "RAMPUPMAXAVAIL": "sum",
            "RAMPDOWNMINAVAIL": "sum",
            "PASAAVAILABILITY": "sum",
            "MAXAVAIL": "sum",
        }
    )

    dispatch = dispatch.loc[:, ["INTERVAL_DATETIME", column_name]]
    dispatch.columns = ["INTERVAL_DATETIME", "COLUMNVALUES"]
    dispatch["COLUMNVALUES"] = dispatch["COLUMNVALUES"].astype(float)
    return dispatch.sort_values(["INTERVAL_DATETIME"]).reset_index(drop=True)


def get_aggregated_dispatch_data_by_duids(
    column_name, duids, start_time, end_time, resolution, raw_data_cache
):
    """
    Function to query dispatch data from a raw data cahce. Data is filter according to the duids and time window
    provided, and returned on a duid basis. Data can queryed at hourly or 5 minute resolution. If a hourly resolution is
    chosen only bid for 5 minute interval ending on the hour are returned.

    Examples:

    >>> get_aggregated_dispatch_data_by_duids(
    ... 'AVAILABILITY',
    ... ['AGLHAL', 'BASTYAN'],
    ... "2022/01/01 01:00:00",
    ... "2022/01/01 02:00:00",
    ... 'hourly',
    ... 'D:/nemosis_data_cache')
         INTERVAL_DATETIME  COLUMNVALUES
    0  2022-01-01 02:00:00         234.0


    >>> get_aggregated_dispatch_data_by_duids(
    ... 'AVAILABILITY',
    ... ['AGLHAL', 'BASTYAN'],
    ... "2022/01/01 01:00:00",
    ... "2022/01/01 01:05:00",
    ... '5-min',
    ... 'D:/nemosis_data_cache')
         INTERVAL_DATETIME  COLUMNVALUES
    0  2022-01-01 01:05:00         234.0


    Arguments:
        column_name: str, which column of dispatch data to aggregate and return. Should be one of NTERVAL_DATETIME,
            ASBIDRAMPUPMAXAVAIL (upper dispatch limit based on as bid ramp rate, when aggregated unit contribution cannot
            exceed MAXAVAIL), ASBIDRAMPDOWNMINAVAIL (lower dispatch limit based on as bid ramp rate, when aggregated unit
            contribution cannot be less than zero), RAMPUPMAXAVAIL (upper dispatch limit based lesser of as bid and
            telemetry ramp rates, when aggregated unit contribution cannot exceed AVAILABILITY), RAMPDOWNMINAVAIL (lower
            dispatch limit based lesser of as bid and telemetry ramp rates, when aggregated unit contribution cannot be less
            than zero), AVAILABILITY, TOTALCLEARED (as for after_dispatch_metrics), PASAAVAILABILITY, MAXAVAIL (as for
            as_bid_metrics), and FINALMW (the unit operating level at the end of the dispatch interval).
        duids: list[str] of duids to return in result.
        start_time: Initial datetime, formatted "DD/MM/YYYY HH:MM:SS"
        end_time: Ending datetime, formatted identical to start_time
        resolution: str 'hourly' or '5-min'
        raw_data_cache: Filepath to directory for caching files downloaded from AEMO

    Returns:
        pd.DataFrame containing columns INTERVAL_DATETIME, COLUMNVALUES (aggregate of column specified in input)
    """
    if resolution == "hourly":
        end_time_dt = datetime.strptime(end_time, "%Y/%m/%d %H:%M:%S") + timedelta(
            hours=1
        )
    else:
        end_time_dt = datetime.strptime(end_time, "%Y/%m/%d %H:%M:%S") + timedelta(
            minutes=5
        )
    end_time = datetime.strftime(end_time_dt, "%Y/%m/%d %H:%M:%S")

    dispatch = fetch_and_preprocess.unit_dispatch(start_time, end_time, raw_data_cache)

    dispatch = dispatch[dispatch["DUID"].isin(duids)].copy()

    if resolution == "hourly":
        dispatch = dispatch[dispatch["INTERVAL_DATETIME"].str[14:16] == "00"].copy()

    dispatch["ASBIDRAMPUPMAXAVAIL"] = np.where(
        dispatch["ASBIDRAMPUPMAXAVAIL"] > dispatch["MAXAVAIL"],
        dispatch["MAXAVAIL"],
        dispatch["ASBIDRAMPUPMAXAVAIL"],
    )

    dispatch["ASBIDRAMPDOWNMINAVAIL"] = np.where(
        dispatch["ASBIDRAMPDOWNMINAVAIL"] < 0.0, 0.0, dispatch["ASBIDRAMPDOWNMINAVAIL"]
    )

    dispatch["RAMPUPMAXAVAIL"] = np.where(
        dispatch["RAMPUPMAXAVAIL"] > dispatch["AVAILABILITY"],
        dispatch["AVAILABILITY"],
        dispatch["RAMPUPMAXAVAIL"],
    )

    dispatch["RAMPDOWNMINAVAIL"] = np.where(
        dispatch["RAMPDOWNMINAVAIL"] < 0.0, 0.0, dispatch["RAMPDOWNMINAVAIL"]
    )

    dispatch = dispatch.groupby("INTERVAL_DATETIME", as_index=False).agg(
        {
            "AVAILABILITY": "sum",
            "TOTALCLEARED": "sum",
            "FINALMW": "sum",
            "ASBIDRAMPUPMAXAVAIL": "sum",
            "ASBIDRAMPDOWNMINAVAIL": "sum",
            "RAMPUPMAXAVAIL": "sum",
            "RAMPDOWNMINAVAIL": "sum",
            "PASAAVAILABILITY": "sum",
            "MAXAVAIL": "sum",
        }
    )

    dispatch = dispatch.loc[:, ["INTERVAL_DATETIME", column_name]]
    dispatch.columns = ["INTERVAL_DATETIME", "COLUMNVALUES"]
    dispatch["COLUMNVALUES"] = dispatch["COLUMNVALUES"].astype(float)
    return dispatch.sort_values(["INTERVAL_DATETIME"]).reset_index(drop=True)


def get_aggregated_vwap(regions, start_time, end_time, raw_data_cache):
    """
    Function to query and aggregate price data from the raw data cache. To aggregate price data volume weighted
    averaged.

    Examples:

    >>> get_aggregated_vwap(
    ... regions=['NSW'],
    ... start_time="2022/01/01 01:00:00",
    ... end_time="2022/01/01 02:00:00",
    ... raw_data_cache="D:/nemosis_data_cache")
             SETTLEMENTDATE      PRICE
    0   2022-01-01 01:05:00  107.80005
    1   2022-01-01 01:10:00  107.80005
    2   2022-01-01 01:15:00   91.92056
    3   2022-01-01 01:20:00  107.80005
    4   2022-01-01 01:25:00   91.37289
    5   2022-01-01 01:30:00   91.38851
    6   2022-01-01 01:35:00   92.14760
    7   2022-01-01 01:40:00  100.27929
    8   2022-01-01 01:45:00   91.90742
    9   2022-01-01 01:50:00  100.30000
    10  2022-01-01 01:55:00   85.00000
    11  2022-01-01 02:00:00   85.00005

    Arguments:
        regions: list[str] of region to aggregate.
        start_time: Initial datetime, formatted "DD/MM/YYYY HH:MM:SS"
        end_time: Ending datetime, formatted identical to start_time
        raw_data_cache: Filepath to directory for caching files downloaded from AEMO

    Returns:
        pd.DataFrame with columns SETTLEMENTDATE, TOTALDEMAND and RRP (volume weighted avergae of energy price at
        regional reference nodes).
    """

    data = fetch_and_preprocess.region_data(start_time, end_time, raw_data_cache)
    data = data.loc[data["REGIONID"].isin(regions), :]
    data["pricebydemand"] = data["RRP"] * data["TOTALDEMAND"]
    data = data.groupby("SETTLEMENTDATE", as_index=False).agg(
        {"pricebydemand": "sum", "TOTALDEMAND": "sum"}
    )
    data["PRICE"] = data["pricebydemand"] / data["TOTALDEMAND"]
    return (
        data.loc[:, ["SETTLEMENTDATE", "PRICE"]]
        .sort_values(["SETTLEMENTDATE"])
        .reset_index(drop=True)
    )


def unit_types(raw_data_cache, dispatch_type, regions):
    """
    Function to query distinct unit types from raw data cache.

    Examples:

    >>> unit_types(
    ... 'D:/nemosis_data_cache',
    ... 'Generator',
    ... ['NSW'])
                UNIT TYPE
    0             Bagasse
    1   Battery Discharge
    2          Black Coal
    3                CCGT
    4              Engine
    5               Hydro
    6                OCGT
    7  Run of River Hydro
    8               Solar
    9                Wind

    Args:
        dispatch_type: str 'Generator' or 'Load'
        regions: list[str] regions to aggregate should only be QLD, NSW, VIC, SA or TAS.
        raw_data_cache: Filepath to directory for caching files downloaded from AEMO

    Returns:
        pd.DataFrame column UNIT TYPE (this is the unit type as determined by the function
        :py:func:`nem_bidding_dashboard.preprocessing.tech_namer_by_row`)
    """
    data = fetch_and_preprocess.duid_info(raw_data_cache)
    data = data[
        (data["DISPATCH TYPE"] == dispatch_type) & (data["REGION"].isin(regions))
    ]
    data = data.loc[:, ["UNIT TYPE"]].drop_duplicates()
    return data.sort_values("UNIT TYPE").reset_index(drop=True)
