import pandas as pd
import psycopg
from psycopg.rows import dict_row


def run_query_return_dataframe(connection_string, query):
    """
    Examples
    --------
    >>> from nem_bidding_dashboard import postgress_helpers

    >>> con_string = postgress_helpers.build_connection_string(
    ... hostname='localhost',
    ... dbname='bidding_dashboard_db',
    ... username='bidding_dashboard_maintainer',
    ... password='1234abcd',
    ... port=5433)

    >>> create_db_tables_and_functions(con_string, "select * from duid_info limit 10;")
           DUID REGION  ...           UNIT TYPE                 STATION NAME
    0   ADPBA1G     SA  ...   Battery Discharge  Adelaide Desalination Plant
    1   ADPBA1L     SA  ...      Battery Charge  Adelaide Desalination Plant
    2    ADPMH1     SA  ...  Run of River Hydro  Adelaide Desalination Plant
    3    ADPPV3     SA  ...               Solar  Adelaide Desalination Plant
    4    ADPPV2     SA  ...               Solar  Adelaide Desalination Plant
    5    ADPPV1     SA  ...               Solar  Adelaide Desalination Plant
    6  AGLSITA1    NSW  ...              Engine              Agl Kemps Creek
    7   ANGAST1     SA  ...              Engine       Angaston Power Station
    8     APPIN    NSW  ...              Engine            Appin Power Plant
    9     ARWF1    VIC  ...                Wind             Ararat Wind Farm
    <BLANKLINE>
    [10 rows x 7 columns]
    """
    with psycopg.connect(connection_string) as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(query)
            data = cur.fetchall()
    data = pd.DataFrame(data)
    data.columns = data.columns.str.upper()
    return data


def region_data(connection_string, start_date, end_date):
    """
    Function to query demand data from supabase. For this function to run the supabase url and key need to be configured
    as environment variables labeled SUPABASE_BIDDING_DASHBOARD_URL and SUPABASE_BIDDING_DASHBOARD_KEY
    respectively.

    Examples
    --------
    >>> from nem_bidding_dashboard import postgress_helpers

    >>> con_string = postgress_helpers.build_connection_string(
    ... hostname='localhost',
    ... dbname='bidding_dashboard_db',
    ... username='bidding_dashboard_maintainer',
    ... password='1234abcd',
    ... port=5433)

    >>> region_data(
    ... con_string,
    ... start_date="2020/01/01 00:00:00",
    ... end_date="2020/01/01 00:10:00")
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
        start_date: Initial datetime, formatted "DD/MM/YYYY HH:MM:SS" (time always
            set to "00:00:00:)
        end_date: Ending datetime, formatted identical to start_date
    """
    query = "select * from demand_data where settlementdate >= '{start_date}' and settlementdate <= '{end_date}'"
    query = query.format(start_date=start_date, end_date=end_date)
    data = run_query_return_dataframe(connection_string, query)
    return data


def aggregate_bids(connection_string, regions, start_date, end_date, resolution):
    """
    Function to query bidding data from supabase. Data is filter according to the regions and time window provided, it
    is then aggregated into a set of predefined bins. Data can queried at hourly or 5 minute resolution. If a hourly
    resolution is chosen only bid for 5 minute interval ending on the hour are returned. For this function to run the
    supabase url and key need to be configured as environment variables labeled SUPABASE_BIDDING_DASHBOARD_URL and
    SUPABASE_BIDDING_DASHBOARD_KEY respectively.

    Examples
    --------
    >>> from nem_bidding_dashboard import postgress_helpers

    >>> con_string = postgress_helpers.build_connection_string(
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
    ... 'hourly')
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
    ... '5-min')
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
        regions: list[str] regions to aggregate should only be QLD, NSW, VIC, SA or TAS.
        start_date: Initial datetime, formatted "DD/MM/YYYY HH:MM:SS" (time always
            set to "00:00:00:)
        end_date: Ending datetime, formatted identical to start_date
        resolution: str 'hourly' or '5-min'
    """
    regions = ['"{r}"'.format(r=r) for r in regions]
    regions = ", ".join(regions)
    query = """SELECT * FROM aggregate_bids_v2(
                '{{{regions}}}',
                (timestamp '{start_date}'),
                (timestamp '{end_date}'),
                '{resolution}',
                'Generator',
                'adjusted',
                '{{}}'
                )"""
    query = query.format(
        regions=regions, start_date=start_date, end_date=end_date, resolution=resolution
    )
    data = run_query_return_dataframe(connection_string, query)
    return data


def duid_bids(connection_string, duids, start_date, end_date, resolution):
    """
    Function to query bidding data from supabase. Data is filter according to the regions and time window provided,
    and returned on a duid basis. Data can queryed at hourly or 5 minute resolution. If a hourly resolution is chosen
    only bid for 5 minute interval ending on the hour are returned. For this function to run the supabase url and key
    need to be configured as environment variables labeled SUPABASE_BIDDING_DASHBOARD_URL and
    SUPABASE_BIDDING_DASHBOARD_KEY respectively.

    Examples
    --------
    >>> from nem_bidding_dashboard import postgress_helpers

    >>> con_string = postgress_helpers.build_connection_string(
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
    ... 'hourly')
        INTERVAL_DATETIME    DUID  BIDBAND  BIDVOLUME  BIDPRICE
    0 2020-01-01 01:00:00  AGLHAL        7       60.0    564.22
    1 2020-01-01 01:00:00  AGLHAL       10      195.0  13646.22


    >>> duid_bids(
    ... con_string,
    ... ['AGLHAL'],
    ... "2020/01/01 00:00:00",
    ... "2020/01/01 00:05:00",
    ... '5-min')
        INTERVAL_DATETIME    DUID  BIDBAND  BIDVOLUME  BIDPRICE
    0 2020-01-01 00:05:00  AGLHAL        7       60.0    564.22
    1 2020-01-01 00:05:00  AGLHAL       10      195.0  13646.22

    Arguments:
        duids: list[str] of duids to return in result.
        start_date: Initial datetime, formatted "DD/MM/YYYY HH:MM:SS" (time always
            set to "00:00:00:)
        end_date: Ending datetime, formatted identical to start_date
        resolution: str 'hourly' or '5-min'
    """
    duids = ['"{d}"'.format(d=d) for d in duids]
    duids = ", ".join(duids)
    query = """SELECT * FROM get_bids_by_unit_v2(
                '{{{duids}}}',
                (timestamp '{start_date}'),
                (timestamp '{end_date}'),
                '{resolution}'
                'adjusted')"""
    query = query.format(
        duids=duids, start_date=start_date, end_date=end_date, resolution=resolution
    )
    data = run_query_return_dataframe(connection_string, query)
    return data


def stations_and_duids_in_regions_and_time_window(
    connection_string, regions, start_date, end_date
):
    """
    Function to query units from given regions with bids available in the given time window. Data returned is DUIDs and
    corresponding Station Names. For this function to run the supabase url and key need to be configured as environment
    variables labeled SUPABASE_BIDDING_DASHBOARD_URL and SUPABASE_BIDDING_DASHBOARD_KEY respectively.

    Examples
    --------
    >>> from nem_bidding_dashboard import postgress_helpers

    >>> con_string = postgress_helpers.build_connection_string(
    ... hostname='localhost',
    ... dbname='bidding_dashboard_db',
    ... username='bidding_dashboard_maintainer',
    ... password='1234abcd',
    ... port=5433)

    >>> stations_and_duids_in_regions_and_time_window(
    ... con_string,
    ... ['NSW'],
    ... "2020/01/01 00:00:00",
    ... "2020/01/01 00:05:00",)
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
        duids: list[str] of duids to return in result.
        start_date: Initial datetime, formatted "DD/MM/YYYY HH:MM:SS" (time always
            set to "00:00:00:)
        end_date: Ending datetime, formatted identical to start_date
    Returns:
        pd dataframe
    """
    regions = ['"{r}"'.format(r=r) for r in regions]
    regions = ", ".join(regions)
    query = """SELECT * FROM get_duids_and_stations(
                '{{{regions}}}',
                (timestamp '{start_date}'),
                (timestamp '{end_date}'),
                'Generator',
                '{{}}'
                )"""
    query = query.format(regions=regions, start_date=start_date, end_date=end_date)
    data = run_query_return_dataframe(connection_string, query)
    return data


def get_aggregated_dispatch_data(
    connection_string, regions, start_date, end_date, resolution
):
    """
    Function to query dispatch data from supabase. Data is filter according to the regions and time window provided,
    and returned on a duid basis. Data can queryed at hourly or 5 minute resolution. If a hourly resolution is chosen
    only bid for 5 minute interval ending on the hour are returned. For this function to run the supabase url and key
    need to be configured as environment variables labeled SUPABASE_BIDDING_DASHBOARD_URL and
    SUPABASE_BIDDING_DASHBOARD_KEY respectively.

    Examples
    --------
    >>> from nem_bidding_dashboard import postgress_helpers

    >>> con_string = postgress_helpers.build_connection_string(
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
    ... 'hourly')
        INTERVAL_DATETIME  AVAILABILITY  ...  PASAAVAILABILITY  MAXAVAIL
    0 2020-01-01 01:00:00      12312.53  ...           15561.0   13915.0
    <BLANKLINE>
    [1 rows x 10 columns]


    >>> get_aggregated_dispatch_data(
    ... con_string,
    ... ['NSW'],
    ... "2020/01/01 00:00:00",
    ... "2020/01/01 00:05:00",
    ... '5-min')
        INTERVAL_DATETIME  AVAILABILITY  ...  PASAAVAILABILITY  MAXAVAIL
    0 2020-01-01 00:05:00     11571.306  ...           15561.0   13095.0
    <BLANKLINE>
    [1 rows x 10 columns]

    Arguments:
        duids: list[str] of duids to return in result.
        start_date: Initial datetime, formatted "DD/MM/YYYY HH:MM:SS" (time always
            set to "00:00:00:)
        end_date: Ending datetime, formatted identical to start_date
        resolution: str 'hourly' or '5-min'
    """
    regions = ['"{r}"'.format(r=r) for r in regions]
    regions = ", ".join(regions)
    query = """SELECT * FROM aggregate_dispatch_data(
                '{{{regions}}}',
                (timestamp '{start_date}'),
                (timestamp '{end_date}'),
                '{resolution}',
                'Generator',
                '{{}}'
                )"""
    query = query.format(
        regions=regions, start_date=start_date, end_date=end_date, resolution=resolution
    )
    data = run_query_return_dataframe(connection_string, query)
    return data


def get_aggregated_dispatch_data_by_duids(
    connection_string, duids, start_date, end_date, resolution
):
    """
    Function to query dispatch data from supabase. Data is filter according to the duids and time window provided,
    and returned on a duid basis. Data can queryed at hourly or 5 minute resolution. If a hourly resolution is chosen
    only bid for 5 minute interval ending on the hour are returned. For this function to run the supabase url and key
    need to be configured as environment variables labeled SUPABASE_BIDDING_DASHBOARD_URL and
    SUPABASE_BIDDING_DASHBOARD_KEY respectively.

    Examples
    --------
    >>> from nem_bidding_dashboard import postgress_helpers

    >>> con_string = postgress_helpers.build_connection_string(
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
        duids: list[str] of duids to return in result.
        start_date: Initial datetime, formatted "DD/MM/YYYY HH:MM:SS" (time always
            set to "00:00:00:)
        end_date: Ending datetime, formatted identical to start_date
        resolution: str 'hourly' or '5-min'
    """
    duids = ['"{d}"'.format(d=d) for d in duids]
    duids = ", ".join(duids)
    query = """SELECT * FROM aggregate_dispatch_data_duids(
                '{{{duids}}}',
                (timestamp '{start_date}'),
                (timestamp '{end_date}'),
                '{resolution}'
                )"""
    query = query.format(
        duids=duids, start_date=start_date, end_date=end_date, resolution=resolution
    )
    data = run_query_return_dataframe(connection_string, query)
    return data


def get_aggregated_vwap(connection_string, regions, start_date, end_date):
    """
    Function to query aggregated Volume Weighted Average Price from supabase. Data is filter according to the regions
    and time window provided. Data can queryed at hourly or 5 minute resolution. Prices are weighted by demand in each
    region selected For this function to run the supabase url and key need to be configured as environment variables
    labeled SUPABASE_BIDDING_DASHBOARD_URL and SUPABASE_BIDDING_DASHBOARD_KEY respectively.

    Examples
    --------
    >>> from nem_bidding_dashboard import postgress_helpers

    >>> con_string = postgress_helpers.build_connection_string(
    ... hostname='localhost',
    ... dbname='bidding_dashboard_db',
    ... username='bidding_dashboard_maintainer',
    ... password='1234abcd',
    ... port=5433)

    >>> get_aggregated_vwap(
    ... con_string,
    ... ['NSW'],
    ... "2020/01/01 00:00:00",
    ... "2020/01/01 01:00:00")
            SETTLEMENTDATE     PRICE
    0  2020-01-01 00:40:00  56.91382
    1  2020-01-01 00:45:00  49.01042
    2  2020-01-01 00:50:00  49.01042
    3  2020-01-01 00:30:00  48.50000
    4  2020-01-01 00:20:00  49.01000
    5  2020-01-01 00:25:00  48.50000
    6  2020-01-01 00:10:00  49.00916
    7  2020-01-01 00:35:00  50.84205
    8  2020-01-01 00:05:00  49.00916
    9  2020-01-01 00:55:00  48.50000
    10 2020-01-01 01:00:00  48.50000
    11 2020-01-01 00:15:00  49.01042

    Arguments:
        regions: list[str] of region to aggregate.
        start_date: Initial datetime, formatted "DD/MM/YYYY HH:MM:SS" (time always
            set to "00:00:00:)
        end_date: Ending datetime, formatted identical to start_date
    """
    regions = ['"{r}"'.format(r=r) for r in regions]
    regions = ", ".join(regions)
    query = """SELECT * FROM aggregate_prices(
                '{{{regions}}}',
                (timestamp '{start_date}'),
                (timestamp '{end_date}')
                )"""
    query = query.format(regions=regions, start_date=start_date, end_date=end_date)
    data = run_query_return_dataframe(connection_string, query)
    return data


def unit_types(connection_string):
    """
    Function to query distinct unit types from supabase. For this function to run the supabase url and key need to be
    configured as environment variables labeled SUPABASE_BIDDING_DASHBOARD_URL and SUPABASE_BIDDING_DASHBOARD_KEY
    respectively.

    Examples
    --------
    >>> from nem_bidding_dashboard import postgress_helpers

    >>> con_string = postgress_helpers.build_connection_string(
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

    """
    query = """SELECT * FROM distinct_unit_types()"""
    data = run_query_return_dataframe(connection_string, query)
    return data
