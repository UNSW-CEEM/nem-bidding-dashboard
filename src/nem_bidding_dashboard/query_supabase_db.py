import os

import postgrest

postgrest.constants.DEFAULT_POSTGREST_CLIENT_TIMEOUT = (
    15000  # Change supabase client timeout
)

import pandas as pd
from supabase import create_client

from nem_bidding_dashboard import defaults, input_validation

pd.set_option("display.width", None)


def region_demand(start_time, end_time, regions):
    """
    Query demand data from supabase. To aggregate demand data is summed. For this function to run the supabase url and
    key need to be configured as environment variables labeled SUPABASE_BIDDING_DASHBOARD_URL and
    SUPABASE_BIDDING_DASHBOARD_KEY respectively.

    Examples:

    >>> region_demand("2022/01/01 01:00:00","2022/01/01 01:30:00",['NSW'])
            SETTLEMENTDATE  TOTALDEMAND
    0  2022-01-01 01:05:00      6631.21
    1  2022-01-01 01:10:00      6655.52
    2  2022-01-01 01:15:00      6496.85
    3  2022-01-01 01:20:00      6520.86
    4  2022-01-01 01:25:00      6439.22
    5  2022-01-01 01:30:00      6429.13

    Args:
        start_time: Initial datetime, formatted "DD/MM/YYYY HH:MM:SS"
        end_time: Ending datetime, formatted identical to start_time
        regions: list[str] regions to aggregate should only be QLD, NSW, VIC, SA or TAS.

    Returns:
        pd.DataFrame with columns SETTLEMENTDATE, REGIONID, and TOTALDEMAND (demand to be meet by schedualed and
        semischedualed generators, not including schedualed loads)
    """
    input_validation.validate_region_demand_args(start_time, end_time, regions)
    url = os.environ.get("SUPABASE_BIDDING_DASHBOARD_URL")
    key = os.environ.get("SUPABASE_BIDDING_DASHBOARD_KEY")
    supabase = create_client(url, key)
    data = supabase.rpc(
        "aggregate_demand",
        {"regions": regions, "start_datetime": start_time, "end_datetime": end_time},
    ).execute()
    data = pd.DataFrame(data.data)
    if data.empty:
        data = pd.DataFrame(
            {
                "SETTLEMENTDATE": pd.Series(dtype="str"),
                "TOTALDEMAND": pd.Series(dtype="float"),
            }
        )
    data.columns = data.columns.str.upper()
    data["SETTLEMENTDATE"] = data["SETTLEMENTDATE"].str.replace("T", " ")
    return data.sort_values("SETTLEMENTDATE").reset_index(drop=True)


def aggregate_bids(
    start_time, end_time, regions, dispatch_type, tech_types, resolution, adjusted
):
    """
    Function to query bidding data from supabase. Data is filtered according to the regions, dispatch type, tech types
    and time window provided, it is then aggregated into a set of predefined bins. Data can queried at hourly or
    5 minute resolution. If a hourly resolution is chosen only bid for 5 minute interval ending on the hour are
    returned. For this function to run the supabase url and key need to be configured as environment variables labeled
    SUPABASE_BIDDING_DASHBOARD_URL and SUPABASE_BIDDING_DASHBOARD_KEY respectively.

    Examples:

    >>> aggregate_bids(
    ... "2022/01/01 01:00:00",
    ... "2022/01/01 02:00:00",
    ... ['QLD', 'NSW', 'SA'],
    ... 'Generator',
    ... [],
    ... 'hourly',
    ... 'adjusted')
          INTERVAL_DATETIME        BIN_NAME  BIDVOLUME
    0   2022-01-01 02:00:00   [-1000, -100)   9158.030
    1   2022-01-01 02:00:00       [-100, 0)    299.744
    2   2022-01-01 02:00:00         [0, 50)   1142.000
    3   2022-01-01 02:00:00       [50, 100)   1141.000
    4   2022-01-01 02:00:00      [100, 200)    918.000
    5   2022-01-01 02:00:00      [200, 300)   1138.000
    6   2022-01-01 02:00:00      [300, 500)    920.000
    7   2022-01-01 02:00:00     [500, 1000)    273.000
    8   2022-01-01 02:00:00    [1000, 5000)    210.000
    9   2022-01-01 02:00:00   [5000, 10000)    125.000
    10  2022-01-01 02:00:00  [10000, 15500)   7009.000


    >>> aggregate_bids(
    ... "2022/01/01 01:00:00",
    ... "2022/01/01 01:05:00",
    ... ['QLD', 'NSW', 'SA'],
    ... 'Generator',
    ... [],
    ... '5-min',
    ... 'adjusted')
          INTERVAL_DATETIME        BIN_NAME  BIDVOLUME
    0   2022-01-01 01:05:00   [-1000, -100)   9642.260
    1   2022-01-01 01:05:00       [-100, 0)    361.945
    2   2022-01-01 01:05:00         [0, 50)   1348.000
    3   2022-01-01 01:05:00       [50, 100)   1415.000
    4   2022-01-01 01:05:00      [100, 200)    912.000
    5   2022-01-01 01:05:00      [200, 300)   1188.000
    6   2022-01-01 01:05:00      [300, 500)    903.000
    7   2022-01-01 01:05:00     [500, 1000)    272.000
    8   2022-01-01 01:05:00    [1000, 5000)    210.000
    9   2022-01-01 01:05:00   [5000, 10000)    125.000
    10  2022-01-01 01:05:00  [10000, 15500)   6853.000


    Args:
        regions: list[str] regions to aggregate should only be QLD, NSW, VIC, SA or TAS.
        start_time: Initial datetime, formatted "DD/MM/YYYY HH:MM:SS"
        end_time: Ending datetime, formatted identical to start_time
        resolution: str 'hourly' or '5-min'
        dispatch_type: str 'Generator' or 'Load'
        adjusted: str which bid data to use aggregate 'raw' or 'adjusted'. Adjusted bid data has been
            adjusted down so the total bid does not exceed the unit availability.
        tech_types: list[str] the technology types to filter for e.g. Solar, Black Coal, CCGT. An empty list
            will result in no filtering by technology

    Returns:
        pd.DataFrame with columns INTERVAL_DATETIME, BIN_NAME (upper and lower limits of price bin) and
        BIDVOLUME (total volume bid by units within price bin).
    """
    input_validation.validate_aggregate_bids_args(
        regions, start_time, end_time, resolution, adjusted, tech_types, dispatch_type
    )
    url = os.environ.get("SUPABASE_BIDDING_DASHBOARD_URL")
    key = os.environ.get("SUPABASE_BIDDING_DASHBOARD_KEY")
    supabase = create_client(url, key)
    data = supabase.rpc(
        "aggregate_bids_v2",
        {
            "regions": regions,
            "start_datetime": start_time,
            "end_datetime": end_time,
            "resolution": resolution,
            "dispatch_type": dispatch_type,
            "adjusted": adjusted,
            "tech_types": tech_types,
        },
    ).execute()
    data = pd.DataFrame(data.data)
    if data.empty:
        data = pd.DataFrame(
            {
                "INTERVAL_DATETIME": pd.Series(dtype="str"),
                "BIN_NAME": pd.Series(dtype="str"),
                "BIDVOLUME": pd.Series(dtype="float"),
            }
        )
    data.columns = data.columns.str.upper()
    data["BIN_NAME"] = data["BIN_NAME"].astype("category")
    data["BIN_NAME"] = data["BIN_NAME"].cat.set_categories(defaults.bid_order)
    data = data.sort_values(["INTERVAL_DATETIME", "BIN_NAME"]).reset_index(drop=True)
    data["BIN_NAME"] = data["BIN_NAME"].astype(str)
    data["BIDVOLUME"] = data["BIDVOLUME"].astype(float)
    data["INTERVAL_DATETIME"] = data["INTERVAL_DATETIME"].str.replace("T", " ")
    return data


def duid_bids(start_time, end_time, duids, resolution, adjusted):
    """
    Function to query bidding data from supabase. Data is filter according to the DUID list and time window provided,
    and returned on a duid basis. Data can queryed at hourly or 5 minute resolution. If an hourly resolution is chosen
    only bid for 5 minute interval ending on the hour are returned. For this function to run the supabase url and key
    need to be configured as environment variables labeled SUPABASE_BIDDING_DASHBOARD_URL and
    SUPABASE_BIDDING_DASHBOARD_KEY respectively.

    Examples:

    >>> duid_bids(
    ... "2022/01/01 01:00:00",
    ... "2022/01/01 02:00:00",
    ... ['AGLHAL', 'BASTYAN'],
    ... 'hourly',
    ... 'adjusted')
         INTERVAL_DATETIME     DUID  BIDBAND  BIDVOLUME  BIDPRICE
    0  2022-01-01 02:00:00   AGLHAL        7         32    557.39
    1  2022-01-01 02:00:00   AGLHAL       10        121  14541.30
    2  2022-01-01 02:00:00  BASTYAN        2         53    -55.64
    3  2022-01-01 02:00:00  BASTYAN        4         28     -0.91
    4  2022-01-01 02:00:00  BASTYAN       10          0  14021.90


    >>> duid_bids("2022/01/01 01:00:00",
    ... "2022/01/01 02:00:00",
    ... ['AGLHAL', 'BASTYAN'],
    ... 'hourly',
    ... 'adjusted')
         INTERVAL_DATETIME     DUID  BIDBAND  BIDVOLUME  BIDPRICE
    0  2022-01-01 02:00:00   AGLHAL        7         32    557.39
    1  2022-01-01 02:00:00   AGLHAL       10        121  14541.30
    2  2022-01-01 02:00:00  BASTYAN        2         53    -55.64
    3  2022-01-01 02:00:00  BASTYAN        4         28     -0.91
    4  2022-01-01 02:00:00  BASTYAN       10          0  14021.90

    Args:
        duids: list[str] of duids to return in result.
        start_time: Initial datetime, formatted "DD/MM/YYYY HH:MM:SS"
        end_time: Ending datetime, formatted identical to start_time
        resolution: str 'hourly' or '5-min'
        adjusted: str which bid data to use aggregate 'raw' or 'adjusted'. Adjusted bid data has been
            adjusted down so the total bid does not exceed the unit availability.

    Returns:
        pd.DataFrame with columns INTERVAL_DATETIME, DUID, BIDBAND, BIDVOLUME, and BIDPRICE
    """
    input_validation.validate_duid_bids_args(
        duids, start_time, end_time, resolution, adjusted
    )
    url = os.environ.get("SUPABASE_BIDDING_DASHBOARD_URL")
    key = os.environ.get("SUPABASE_BIDDING_DASHBOARD_KEY")
    supabase = create_client(url, key)
    data = supabase.rpc(
        "get_bids_by_unit_v2",
        {
            "duids": duids,
            "start_datetime": start_time,
            "end_datetime": end_time,
            "resolution": resolution,
            "adjusted": adjusted,
        },
    ).execute()
    data = pd.DataFrame(data.data)
    if data.empty:
        data = pd.DataFrame(
            {
                "INTERVAL_DATETIME": pd.Series(dtype="str"),
                "DUID": pd.Series(dtype="str"),
                "BIDBAND": pd.Series(dtype="int64"),
                "BIDVOLUME": pd.Series(dtype="float"),
                "BIDPRICE": pd.Series(dtype="float"),
            }
        )
    data.columns = data.columns.str.upper()
    data["INTERVAL_DATETIME"] = data["INTERVAL_DATETIME"].str.replace("T", " ")
    return data.sort_values(["INTERVAL_DATETIME", "DUID", "BIDBAND"]).reset_index(
        drop=True
    )


def stations_and_duids_in_regions_and_time_window(
    start_time, end_time, regions, dispatch_type, tech_types
):
    """
    Function to query units from given regions with bids available in the given time window. Data returned is DUIDs and
    corresponding Station Names. For this function to run the supabase url and key need to be configured as environment
    variables labeled SUPABASE_BIDDING_DASHBOARD_URL and SUPABASE_BIDDING_DASHBOARD_KEY respectively.

    Examples:

    >>> stations_and_duids_in_regions_and_time_window(
    ... "2022/01/01 01:00:00",
    ... "2022/01/01 02:00:00",
    ... "Generator",
    ... [])
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


    Args:
        regions: list[str] regions to filter, should only be QLD, NSW, VIC, SA or TAS.
        start_time: Initial datetime, formatted "DD/MM/YYYY HH:MM:SS"
        end_time: Ending datetime, formatted identical to start_time
        dispatch_type: str 'Generator' or 'Load'
        tech_types: list[str] the technology types to filter for e.g. Solar, Black Coal, CCGT. An empty list
            will result in no filtering by technology

    Returns:
        pd.DataFrame with columns DUID and STATION NAME
    """
    input_validation.validate_stations_and_duids_in_regions_and_time_window_args(
        regions, start_time, end_time, dispatch_type, tech_types
    )
    url = os.environ.get("SUPABASE_BIDDING_DASHBOARD_URL")
    key = os.environ.get("SUPABASE_BIDDING_DASHBOARD_KEY")
    supabase = create_client(url, key)
    data = supabase.rpc(
        "get_duids_and_staions_in_regions_and_time_window_v2",
        {
            "regions": regions,
            "start_datetime": start_time,
            "end_datetime": end_time,
            "dispatch_type": dispatch_type,
            "tech_types": tech_types,
        },
    ).execute()
    data = pd.DataFrame(data.data)
    data.columns = data.columns.str.upper()
    if data.empty:
        data = pd.DataFrame(columns=["DUID", "STATION NAME"])
    return data.sort_values("DUID").reset_index(drop=True)


def aggregated_dispatch_data(
    column_name, start_time, end_time, regions, dispatch_type, tech_types, resolution
):
    """
    Function to query dispatch and aggregate data from a postgres database. Data is filter according to the regions,
    time window, dispatch type, and technology type  provided. Data can queryed at hourly or 5 minute resolution.
    If an hourly resolution is chosen only data for 5 minute interval ending on the hour are returned. For this function
    to run the supabase url and key need to be configured as environment variables labeled
    SUPABASE_BIDDING_DASHBOARD_URL and SUPABASE_BIDDING_DASHBOARD_KEY respectively.

    Examples:

    >>> aggregated_dispatch_data(
    ... 'AVAILABILITY',
    ... "2022/01/01 01:00:00",
    ... "2022/01/01 02:00:00",
    ... ['NSW'],
    ... 'Generator',
    ... [],
    ... 'hourly')
         INTERVAL_DATETIME  COLUMNVALUES
    0  2022-01-01 02:00:00       10402.5


    >>> aggregated_dispatch_data(
    ... 'AVAILABILITY',
    ... "2022/01/01 01:00:00",
    ... "2022/01/01 01:05:00",
    ... ['NSW'],
    ... 'Generator',
    ... [],
    ... '5-min')
         INTERVAL_DATETIME  COLUMNVALUES
    0  2022-01-01 01:05:00       10440.1

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

    Returns:
        pd.DataFrame containing columns INTERVAL_DATETIME, COLUMNVALUES (aggregate of column specified in input)
    """
    input_validation.validate_get_aggregated_dispatch_data_args(
        column_name,
        regions,
        start_time,
        end_time,
        resolution,
        dispatch_type,
        tech_types,
    )
    url = os.environ.get("SUPABASE_BIDDING_DASHBOARD_URL")
    key = os.environ.get("SUPABASE_BIDDING_DASHBOARD_KEY")
    supabase = create_client(url, key)
    data = supabase.rpc(
        "aggregate_dispatch_data_v2",
        {
            "column_name": column_name.lower(),
            "regions": regions,
            "start_datetime": start_time,
            "end_datetime": end_time,
            "resolution": resolution,
            "dispatch_type": dispatch_type,
            "tech_types": tech_types,
        },
    ).execute()
    data = pd.DataFrame(data.data)
    if not data.empty:
        data.columns = data.columns.str.upper()
        data["INTERVAL_DATETIME"] = data["INTERVAL_DATETIME"].str.replace("T", " ")
    else:
        data = pd.DataFrame(columns=["INTERVAL_DATETIME", "COLUMNVALUES"])
    data["COLUMNVALUES"] = data["COLUMNVALUES"].astype(float)
    data = data.sort_values("INTERVAL_DATETIME").reset_index(drop=True)
    return data


def aggregated_dispatch_data_by_duids(
    column_name, start_time, end_time, duids, resolution
):
    """
    Function to query dispatch data from supabase. Data is filter according to the duids and time window provided,
    and returned on a duid basis. Data can queryed at hourly or 5 minute resolution. If an hourly resolution is chosen
    only bid for 5 minute interval ending on the hour are returned. For this function to run the supabase url and key
    need to be configured as environment variables labeled SUPABASE_BIDDING_DASHBOARD_URL and
    SUPABASE_BIDDING_DASHBOARD_KEY respectively.

    Examples:

    >>> aggregated_dispatch_data_by_duids(
    ... 'AVAILABILITY',
    ... "2022/01/01 01:00:00",
    ... "2022/01/01 02:00:00",
    ... ['AGLHAL', 'BASTYAN'],
    ... 'hourly')
         INTERVAL_DATETIME  COLUMNVALUES
    0  2022-01-01 02:00:00           234


    >>> aggregated_dispatch_data_by_duids(
    ... 'AVAILABILITY',
    ... "2022/01/01 01:00:00",
    ... "2022/01/01 01:05:00",
    ... ['AGLHAL', 'BASTYAN'],
    ... '5-min')
         INTERVAL_DATETIME  COLUMNVALUES
    0  2022-01-01 01:05:00           234

    Args:
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

    Returns:
        pd.DataFrame containing columns INTERVAL_DATETIME, COLUMNVALUES (aggregate of column specified in input)
    """
    input_validation.validate_get_aggregated_dispatch_data_by_duids_args(
        column_name, duids, start_time, end_time, resolution
    )
    url = os.environ.get("SUPABASE_BIDDING_DASHBOARD_URL")
    key = os.environ.get("SUPABASE_BIDDING_DASHBOARD_KEY")
    supabase = create_client(url, key)
    data = supabase.rpc(
        "aggregate_dispatch_data_duids_v2",
        {
            "column_name": column_name.lower(),
            "duids": duids,
            "start_datetime": start_time,
            "end_datetime": end_time,
            "resolution": resolution,
        },
    ).execute()
    data = pd.DataFrame(data.data)
    if data.empty:
        data = pd.DataFrame(columns=["INTERVAL_DATETIME", "COLUMNVALUES"])
    data.columns = data.columns.str.upper()
    data["INTERVAL_DATETIME"] = data["INTERVAL_DATETIME"].str.replace("T", " ")
    data["COLUMNVALUES"] = data["COLUMNVALUES"].astype(float)
    return data.sort_values(["INTERVAL_DATETIME"]).reset_index(drop=True)


def aggregated_vwap(start_time, end_time, regions):
    """
    Function to query aggregated Volume Weighted Average Price from supabase. Data is filter according to the regions
    and time window provided. Data can queryed at hourly or 5 minute resolution. Prices are weighted by demand in each
    region selected. For this function to run the supabase url and key need to be configured as environment variables
    labeled SUPABASE_BIDDING_DASHBOARD_URL and SUPABASE_BIDDING_DASHBOARD_KEY respectively.

    Examples:

    >>> aggregated_vwap(
    ... "2022/01/01 01:00:00",
    ... "2022/01/01 02:00:00",
    ... ['NSW'])
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

    Args:
        regions: list[str] of region to aggregate.
        start_time: Initial datetime, formatted "DD/MM/YYYY HH:MM:SS"
        end_time: Ending datetime, formatted identical to start_time

    Returns:
        pd.DataFrame with column SETTLEMENTDATE and PRICE
    """
    input_validation.validate_region_demand_args(start_time, end_time, regions)
    url = os.environ.get("SUPABASE_BIDDING_DASHBOARD_URL")
    key = os.environ.get("SUPABASE_BIDDING_DASHBOARD_KEY")
    supabase = create_client(url, key)
    data = supabase.rpc(
        "aggregate_prices",
        {
            "regions": regions,
            "start_datetime": start_time,
            "end_datetime": end_time,
        },
    ).execute()
    data = pd.DataFrame(data.data)
    if data.empty:
        data = pd.DataFrame(
            {
                "SETTLEMENTDATE": pd.Series(dtype="str"),
                "PRICE": pd.Series(dtype="float"),
            }
        )
    data.columns = data.columns.str.upper()
    data["SETTLEMENTDATE"] = data["SETTLEMENTDATE"].str.replace("T", " ")
    return data.sort_values("SETTLEMENTDATE").reset_index(drop=True)


def unit_types(regions, dispatch_type):
    """
    Function to query distinct unit types from supabase. For this function to run the supabase url and key need to be
    configured as environment variables labeled SUPABASE_BIDDING_DASHBOARD_URL and SUPABASE_BIDDING_DASHBOARD_KEY
    respectively.

    Examples:

    >>> unit_types(
    ... ['NSW'],
    ... 'Generator')
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

    Returns:
        pd.DataFrame column UNIT TYPE (this is the unit type as determined by the function
        :py:func:`nem_bidding_dashboard.preprocessing.tech_namer_by_row`)
    """
    input_validation.validate_unit_types_args(dispatch_type, regions)
    url = os.environ.get("SUPABASE_BIDDING_DASHBOARD_URL")
    key = os.environ.get("SUPABASE_BIDDING_DASHBOARD_KEY")
    supabase = create_client(url, key)
    data = supabase.rpc(
        "distinct_unit_types_v3", {"dispatch_type": dispatch_type, "regions": regions}
    ).execute()
    data = pd.DataFrame(data.data)
    data.columns = data.columns.str.upper()
    if data.empty:
        data = pd.DataFrame(columns=["UNIT TYPE"])
    return data.sort_values("UNIT TYPE").reset_index(drop=True)


if __name__ == "__main__":
    pass
