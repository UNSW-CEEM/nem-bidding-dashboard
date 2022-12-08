import pandas as pd

from nem_bidding_dashboard.postgres_helpers import run_query_return_dataframe

pd.set_option('display.width', None)


def region_data(connection_string, regions, start_time, end_time):
    """
    Function to query demand and price data from a postgres database. To aggregate demand data is
    summed and price data volume weighted averaged.

    Examples:

    >>> from nem_bidding_dashboard import postgres_helpers

    >>> con_string = postgres_helpers.build_connection_string(
    ... hostname='localhost',
    ... dbname='bidding_dashboard_db',
    ... username='bidding_dashboard_maintainer',
    ... password='1234abcd',
    ... port=5433)

    >>> region_data(
    ... con_string,
    ... regions=['QLD', 'NSW', 'VIC', 'SA', 'TAS'],
    ... start_time="2020/01/01 00:00:00",
    ... end_time="2020/01/01 00:10:00")
           SETTLEMENTDATE REGIONID  TOTALDEMAND       RRP
    0 2020-01-01 00:05:00      NSW      7245.31  49.00916
    1 2020-01-01 00:05:00      QLD      6095.75  50.81148
    2 2020-01-01 00:05:00       SA      1466.53  68.00000
    3 2020-01-01 00:05:00      TAS      1010.06  81.79115
    4 2020-01-01 00:05:00      VIC      4267.32  65.67826
    5 2020-01-01 00:10:00      NSW      7233.57  49.00916
    6 2020-01-01 00:10:00      QLD      6072.18  51.14369
    7 2020-01-01 00:10:00       SA      1422.33  68.00000
    8 2020-01-01 00:10:00      TAS      1005.45  81.80000
    9 2020-01-01 00:10:00      VIC      4224.61  64.83524

    Arguments:
        connection_string: str for connecting to PostgresSQL database, the function :py:func:`nem_bidding_dashboard.postgres_helpers.build_connection_string`
            can be used to build a properly formated connection string, or alternative any string that matches the
            format allowed by `PostgresSQL <https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-CONNSTRING>`_
            can be used
        start_time: Initial datetime, formatted "DD/MM/YYYY HH:MM:SS"
        end_time: Ending datetime, formatted identical to start_time
        raw_data_cache: Filepath to directory for caching files downloaded from AEMO

    Returns:
        pd.DataFrame with columns SETTLEMENTDATE, TOTALDEMAND (demand to be meet by schedualed and
        semischedualed generators, not including schedualed loads), and RRP (volume weighted avergae  of energy price at
        regional reference nodes).
    """
    regions = ['"{r}"'.format(r=r) for r in regions]
    regions = ", ".join(regions)
    query = """SELECT * FROM aggregate_prices(
                '{{{regions}}}',
                (timestamp '{start_time}'),
                (timestamp '{end_time}')
                )"""
    query = query.format(regions=regions, start_time=start_time, end_time=end_time)
    data = run_query_return_dataframe(connection_string, query)
    return data


def aggregate_bids(connection_string, regions, start_time, end_time, resolution, dispatch_type, adjusted, tech_types):
    """
    Function to query and aggregate bidding data from postgres database. Data is filter according to the regions,
    dispatch type, tech types and time window provided, it is then aggregated into a set of predefined bins.
    Data can be queried at hourly or 5 minute resolution. If an hourly resolution is chosen only bid for 5 minute
    interval ending on the hour are returned.

    Examples:

    >>> from nem_bidding_dashboard import postgres_helpers

    >>> con_string = postgres_helpers.build_connection_string(
    ... hostname='localhost',
    ... dbname='bidding_dashboard_db',
    ... username='bidding_dashboard_maintainer',
    ... password='1234abcd',
    ... port=5433)

    >>> aggregate_bids(
    ... con_string,
    ... ['QLD', 'NSW', 'SA'],
    ... "2020/01/01 00:00:00",
    ... "2020/01/01 01:00:00",
    ... 'hourly',
    ... 'Generator',
    ... 'adjusted',
    ... [])
         INTERVAL_DATETIME        BIN_NAME  BIDVOLUME
    0  2020-01-01 01:00:00    [1000, 5000)   1004.000
    1  2020-01-01 01:00:00      [100, 200)    300.000
    2  2020-01-01 01:00:00       [50, 100)   1788.000
    3  2020-01-01 01:00:00   [-1000, -100)   9672.090
    4  2020-01-01 01:00:00      [200, 300)   1960.000
    5  2020-01-01 01:00:00         [0, 50)   4810.708
    6  2020-01-01 01:00:00       [-100, 0)      7.442
    7  2020-01-01 01:00:00      [300, 500)    157.000
    8  2020-01-01 01:00:00     [500, 1000)    728.000
    9  2020-01-01 01:00:00  [10000, 15500)   4359.000
    10 2020-01-01 01:00:00   [5000, 10000)     20.000


    >>> aggregate_bids(
    ... con_string,
    ... ['QLD', 'NSW', 'SA'],
    ... "2020/01/01 00:00:00",
    ... "2020/01/01 00:05:00",
    ... '5-min',
    ... 'Generator',
    ... 'adjusted',
    ... [])
         INTERVAL_DATETIME        BIN_NAME  BIDVOLUME
    0  2020-01-01 00:05:00         [0, 50)   5046.485
    1  2020-01-01 00:05:00      [200, 300)   1270.000
    2  2020-01-01 00:05:00       [50, 100)   1773.000
    3  2020-01-01 00:05:00   [5000, 10000)     20.000
    4  2020-01-01 00:05:00       [-100, 0)      1.680
    5  2020-01-01 00:05:00      [100, 200)      0.000
    6  2020-01-01 00:05:00      [300, 500)    157.000
    7  2020-01-01 00:05:00    [1000, 5000)   1004.000
    8  2020-01-01 00:05:00  [10000, 15500)   4279.000
    9  2020-01-01 00:05:00   [-1000, -100)  10020.556
    10 2020-01-01 00:05:00     [500, 1000)    728.000


    Arguments:
        connection_string: str for connecting to PostgresSQL database, the function :py:func:`nem_bidding_dashboard.postgres_helpers.build_connection_string`
            can be used to build a properly formated connection string, or alternative any string that matches the
            format allowed by `PostgresSQL <https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-CONNSTRING>`_
            can be used
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
    regions = ['"{r}"'.format(r=r) for r in regions]
    regions = ", ".join(regions)
    tech_types = ['"{t}"'.format(t=t) for t in tech_types]
    tech_types = ", ".join(tech_types)
    query = f"""SELECT * FROM aggregate_bids_v2(
                '{{{regions}}}',
                (timestamp '{start_time}'),
                (timestamp '{end_time}'),
                '{resolution}',
                '{dispatch_type}',
                '{adjusted}',
                '{{{tech_types}}}'
                )"""
    query = query.format(
        regions=regions, start_time=start_time, end_time=end_time, resolution=resolution, dispatch_type=dispatch_type,
        adjusted=adjusted, tech_types=tech_types
    )
    data = run_query_return_dataframe(connection_string, query)
    return data


def duid_bids(connection_string, duids, start_time, end_time, resolution, adjusted):
    """
    Function to query bidding data from a postgres database. Data is filter according to the regions and time window
    provided, and returned on a duid basis. Data can queryed at hourly or 5 minute resolution. If an hourly resolution
    is chosen only bid for 5 minute interval ending on the hour are returned.

    Examples:

    >>> from nem_bidding_dashboard import postgres_helpers

    >>> con_string = postgres_helpers.build_connection_string(
    ... hostname='localhost',
    ... dbname='bidding_dashboard_db',
    ... username='bidding_dashboard_maintainer',
    ... password='1234abcd',
    ... port=5433)

    >>> duid_bids(
    ... con_string,
    ... ['AGLHAL'],
    ... "2020/01/01 00:00:00",
    ... "2020/01/01 01:00:00",
    ... 'hourly',
    ... 'adjusted')
        INTERVAL_DATETIME    DUID  BIDBAND  BIDVOLUME  BIDPRICE
    0 2020-01-01 01:00:00  AGLHAL        7       60.0    564.22
    1 2020-01-01 01:00:00  AGLHAL       10      195.0  13646.22


    >>> duid_bids(
    ... con_string,
    ... ['AGLHAL'],
    ... "2020/01/01 00:00:00",
    ... "2020/01/01 00:05:00",
    ... '5-min',
    ... 'adjusted')
        INTERVAL_DATETIME    DUID  BIDBAND  BIDVOLUME  BIDPRICE
    0 2020-01-01 00:05:00  AGLHAL        7       60.0    564.22
    1 2020-01-01 00:05:00  AGLHAL       10      195.0  13646.22

    Arguments:
        connection_string: str for connecting to PostgresSQL database, the function :py:func:`nem_bidding_dashboard.postgres_helpers.build_connection_string`
            can be used to build a properly formated connection string, or alternative any string that matches the
            format allowed by `PostgresSQL <https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-CONNSTRING>`_
            can be used
        duids: list[str] of duids to return in result.
        start_time: Initial datetime, formatted "DD/MM/YYYY HH:MM:SS"
        end_time: Ending datetime, formatted identical to start_time
        resolution: str 'hourly' or '5-min'
        adjusted: str which bid data to use aggregate 'raw' or 'adjusted'. Adjusted bid data has been
            adjusted down so the total bid does not exceed the unit availability.

    Returns:
        pd.DataFrame with columns INTERVAL_DATETIME, DUID, BIDBAND, BIDVOLUME, and BIDPRICE
    """
    duids = ['"{d}"'.format(d=d) for d in duids]
    duids = ", ".join(duids)
    query = """SELECT * FROM get_bids_by_unit_v2(
                '{{{duids}}}',
                (timestamp '{start_time}'),
                (timestamp '{end_time}'),
                '{resolution}'
                '{adjusted}')"""
    query = query.format(
        duids=duids, start_time=start_time, end_time=end_time, resolution=resolution, adjusted=adjusted
    )
    data = run_query_return_dataframe(connection_string, query)
    return data


def available_stations_and_duids(
    connection_string, regions, start_time, end_time, dispatch_type, tech_types
):
    """
    Function to query units from given regions with bids available in the given time window, with the the given dispatch
    and technology type. Data returned is DUIDs and corresponding Station Names.

    Examples:

    >>> from nem_bidding_dashboard import postgres_helpers

    >>> con_string = postgres_helpers.build_connection_string(
    ... hostname='localhost',
    ... dbname='bidding_dashboard_db',
    ... username='bidding_dashboard_maintainer',
    ... password='1234abcd',
    ... port=5433)

    >>> stations_and_duids_in_regions_and_time_window(
    ... con_string,
    ... ['NSW'],
    ... "2020/01/01 00:00:00",
    ... "2020/01/01 00:05:00",
    ... "Generator",
    ... [])
              DUID                    STATION NAME
    0        ARWF1                Ararat Wind Farm
    1     KABANWF1                 Kaban Wind Farm
    2    DIAPURWF1                Diapur Wind Farm
    3      ADPBA1G     Adelaide Desalination Plant
    4       ADPMH1     Adelaide Desalination Plant
    ..         ...                             ...
    462   YWNGAHYD  Yarrawonga Hydro Power Station
    463   YARWUN_1            Yarwun Power Station
    464     YATSF1              Yatpool Solar Farm
    465     YAWWF1                Yawong Wind Farm
    466    YENDWF1                Yendon Wind Farm
    <BLANKLINE>
    [467 rows x 2 columns]

    Arguments:
        connection_string: str for connecting to PostgresSQL database, the function :py:func:`nem_bidding_dashboard.postgres_helpers.build_connection_string`
            can be used to build a properly formated connection string, or alternative any string that matches the
            format allowed by `PostgresSQL <https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-CONNSTRING>`_
            can be used
        duids: list[str] of duids to return in result.
        start_time: Initial datetime, formatted "DD/MM/YYYY HH:MM:SS"
        end_time: Ending datetime, formatted identical to start_time
        dispatch_type: str 'Generator' or 'Load'
        tech_types: list[str] the technology types to filter for e.g. Solar, Black Coal, CCGT. An empty list
            will result in no filtering by technology

    Returns:
        pd.DataFrame with columns DUID and STATION NAME
    """
    regions = ['"{r}"'.format(r=r) for r in regions]
    regions = ", ".join(regions)
    tech_types = ['"{t}"'.format(t=t) for t in tech_types]
    tech_types = ", ".join(tech_types)
    query = f"""SELECT * FROM get_duids_and_stations(
                '{{{regions}}}',
                (timestamp '{start_time}'),
                (timestamp '{end_time}'),
                '{dispatch_type}',
                '{{{tech_types}}}'
                )"""
    query = query.format(regions=regions, start_time=start_time, end_time=end_time, dispatch_type=dispatch_type,
                         tech_types=tech_types)
    data = run_query_return_dataframe(connection_string, query)
    return data


def get_aggregated_dispatch_data(
    connection_string, regions, start_time, end_time, resolution, dispatch_type, tech_types
):
    """
    Function to query dispatch data from a postgres database. Data is filter according to the regions, time window,
    dispatch type, and technology type  provided, and returned on a SETTLEMENTDATE basis. Data can queryed at hourly or
    5 minute resolution. If an hourly resolution is chosen only data for 5 minute interval ending on the hour are
    returned.

    Examples:

    >>> from nem_bidding_dashboard import postgres_helpers

    >>> con_string = postgres_helpers.build_connection_string(
    ... hostname='localhost',
    ... dbname='bidding_dashboard_db',
    ... username='bidding_dashboard_maintainer',
    ... password='1234abcd',
    ... port=5433)

    >>> get_aggregated_dispatch_data(
    ... con_string,
    ... ['NSW'],
    ... "2020/01/01 00:00:00",
    ... "2020/01/01 01:00:00",
    ... 'hourly',
    ... 'Generator',
    ... [])
        INTERVAL_DATETIME  AVAILABILITY  ...  PASAAVAILABILITY  MAXAVAIL
    0 2020-01-01 01:00:00      12312.53  ...           15561.0   13915.0
    <BLANKLINE>
    [1 rows x 10 columns]


    >>> get_aggregated_dispatch_data(
    ... con_string,
    ... ['NSW'],
    ... "2020/01/01 00:00:00",
    ... "2020/01/01 00:05:00",
    ... '5-min',
    ... 'Generator',
    ... [])
        INTERVAL_DATETIME  AVAILABILITY  ...  PASAAVAILABILITY  MAXAVAIL
    0 2020-01-01 00:05:00     11571.306  ...           15561.0   13095.0
    <BLANKLINE>
    [1 rows x 10 columns]

    Arguments:
        connection_string: str for connecting to PostgresSQL database, the function :py:func:`nem_bidding_dashboard.postgres_helpers.build_connection_string`
            can be used to build a properly formated connection string, or alternative any string that matches the
            format allowed by `PostgresSQL <https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-CONNSTRING>`_
            can be used
        duids: list[str] of duids to return in result.
        start_time: Initial datetime, formatted "DD/MM/YYYY HH:MM:SS"
        end_time: Ending datetime, formatted identical to start_time
        resolution: str 'hourly' or '5-min'
        dispatch_type: str 'Generator' or 'Load'
        tech_types: list[str] the technology types to filter for e.g. Solar, Black Coal, CCGT. An empty list
            will result in no filtering by technology

    Returns:
        pd.DataFrame containing columns INTERVAL_DATETIME, ASBIDRAMPUPMAXAVAIL (upper dispatch limit based
        on as bid ramp rate), ASBIDRAMPDOWNMINAVAIL (lower dispatch limit based on as bid ramp rate), RAMPUPMAXAVAIL (
        upper dispatch limit based lesser of as bid and telemetry ramp rates), RAMPDOWNMINAVAIL (lower dispatch limit based
        lesser of as bid and telemetry ramp rates), AVAILABILITY, TOTALCLEARED (as for after_dispatch_metrics),
        PASAAVAILABILITY, MAXAVAIL (as for as_bid_metrics), and FINALMW (the unit operating level at the end of the dispatch
        interval).
    """
    regions = ['"{r}"'.format(r=r) for r in regions]
    regions = ", ".join(regions)
    tech_types = ['"{t}"'.format(t=t) for t in tech_types]
    tech_types = ", ".join(tech_types)
    query = f"""SELECT * FROM aggregate_dispatch_data(
                '{{{regions}}}',
                (timestamp '{start_time}'),
                (timestamp '{end_time}'),
                '{resolution}',
                '{dispatch_type}',
                '{{{tech_types}}}'
                )"""
    query = query.format(
        regions=regions, start_time=start_time, end_time=end_time, resolution=resolution, dispatch_type=dispatch_type,
        tech_types=tech_types
    )
    data = run_query_return_dataframe(connection_string, query)
    return data


def get_aggregated_dispatch_data_by_duids(
    connection_string, duids, start_time, end_time, resolution
):
    """
    Function to query dispatch data from a postgres database. Data is filter according to the duids and time window
    provided, and returned on a duid basis. Data can queryed at hourly or 5 minute resolution. If a hourly resolution is
    chosen only bid for 5 minute interval ending on the hour are returned.

    Examples:

    >>> from nem_bidding_dashboard import postgres_helpers

    >>> con_string = postgres_helpers.build_connection_string(
    ... hostname='localhost',
    ... dbname='bidding_dashboard_db',
    ... username='bidding_dashboard_maintainer',
    ... password='1234abcd',
    ... port=5433)

    >>> get_aggregated_dispatch_data_by_duids(
    ... con_string,
    ... ['AGLHAL'],
    ... "2020/01/01 00:00:00",
    ... "2020/01/01 01:00:00",
    ... 'hourly')
        INTERVAL_DATETIME  AVAILABILITY  ...  PASAAVAILABILITY  MAXAVAIL
    0 2020-01-01 01:00:00         181.0  ...             181.0     181.0
    <BLANKLINE>
    [1 rows x 10 columns]


    >>> get_aggregated_dispatch_data_by_duids(
    ... con_string,
    ... ['AGLHAL'],
    ... "2020/01/01 00:00:00",
    ... "2020/01/01 00:05:00",
    ... '5-min')
        INTERVAL_DATETIME  AVAILABILITY  ...  PASAAVAILABILITY  MAXAVAIL
    0 2020-01-01 00:05:00         181.0  ...             181.0     181.0
    <BLANKLINE>
    [1 rows x 10 columns]

    Arguments:
        connection_string: str for connecting to PostgresSQL database, the function :py:func:`nem_bidding_dashboard.postgres_helpers.build_connection_string`
            can be used to build a properly formated connection string, or alternative any string that matches the
            format allowed by `PostgresSQL <https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-CONNSTRING>`_
            can be used
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
    duids = ['"{d}"'.format(d=d) for d in duids]
    duids = ", ".join(duids)
    query = """SELECT * FROM aggregate_dispatch_data_duids(
                '{{{duids}}}',
                (timestamp '{start_time}'),
                (timestamp '{end_time}'),
                '{resolution}'
                )"""
    query = query.format(
        duids=duids, start_time=start_time, end_time=end_time, resolution=resolution
    )
    data = run_query_return_dataframe(connection_string, query)
    return data


def unit_types(connection_string):
    """
    Function to query distinct unit types from postgres database.

    Examples:

    >>> from nem_bidding_dashboard import postgres_helpers

    >>> con_string = postgres_helpers.build_connection_string(
    ... hostname='localhost',
    ... dbname='bidding_dashboard_db',
    ... username='bidding_dashboard_maintainer',
    ... password='1234abcd',
    ... port=5433)

    >>> unit_types(con_string)
                     UNIT TYPE
    0                     CCGT
    1                     Wind
    2                    Solar
    3   Pump Storage Discharge
    4           Battery Charge
    5                   Engine
    6              Gas Thermal
    7                  Bagasse
    8                    Hydro
    9      Pump Storage Charge
    10              Black Coal
    11       Battery Discharge
    12                    OCGT
    13              Brown Coal
    14      Run of River Hydro

    Args:
        connection_string: str for connecting to PostgresSQL database, the function :py:func:`nem_bidding_dashboard.postgres_helpers.build_connection_string`
            can be used to build a properly formated connection string, or alternative any string that matches the
            format allowed by `PostgresSQL <https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-CONNSTRING>`_
            can be used

    Returns:
        pd.DataFrame column UNIT TYPE (this is the unit type as determined by the function
        :py:func:`nem_bidding_dashboard.preprocessing.tech_namer_by_row`)
    """
    query = """SELECT * FROM distinct_unit_types()"""
    data = run_query_return_dataframe(connection_string, query)
    return data
