import pandas as pd
import numpy as np

from nem_bidding_dashboard import fetch_and_preprocess

pd.set_option('display.width', None)


def region_data(regions, start_time, end_time, raw_data_cache):
    """
    Function to query and aggregate demand and price data from the raw data cache. To aggregate demand data is
    summed and price data volume weighted averaged.

    Examples:

    >>> region_data(
    ... regions=['NSW', 'QLD'],
    ... start_time="2020/01/01 00:00:00",
    ... end_time="2020/01/01 00:10:00",
    ... raw_data_cache="D:/nemosis_data_cache")
            SETTLEMENTDATE  TOTALDEMAND        RRP
    0  2020-01-01 00:05:00     13341.06  49.832670
    1  2020-01-01 00:10:00     13305.75  49.983269

    Arguments:
        regions: regions to aggregate demand and price from
        start_time: Initial datetime, formatted "DD/MM/YYYY HH:MM:SS"
        end_time: Ending datetime, formatted identical to start_time
        raw_data_cache: Filepath to directory for caching files downloaded from AEMO

    Returns:
        pd.DataFrame with columns SETTLEMENTDATE, TOTALDEMAND (demand to be meet by schedualed and
        semischedualed generators, not including schedualed loads), and RRP (volume weighted avergae  of energy price at
        regional reference nodes).
    """

    data = fetch_and_preprocess.region_data(start_time, end_time, raw_data_cache)
    data = data.loc[data['REGIONID'].isin(regions), :]
    data['pricebydemand'] = data['RRP'] * data['TOTALDEMAND']
    data = data.groupby('SETTLEMENTDATE', as_index=False).agg(
        {'pricebydemand': 'sum', 'TOTALDEMAND': 'sum'})
    data['RRP'] = data['pricebydemand'] / data['TOTALDEMAND']
    return data.loc[:, ['SETTLEMENTDATE', 'TOTALDEMAND', 'RRP']]


def aggregate_bids(regions, start_time, end_time, resolution, dispatch_type, adjusted, tech_types, raw_data_cache):
    """
    Function to query and aggregate bidding data from raw data cache database. Data is filter according to the regions,
    dispatch type, tech types and time window provided, it is then aggregated into a set of predefined bins.
    Data can be queried at hourly or 5 minute resolution. If an hourly resolution is chosen only bid for 5 minute
    interval ending on the hour are returned.

    Examples:

    >>> aggregate_bids(
    ... ['QLD', 'NSW', 'SA'],
    ... "2020/01/01 00:00:00",
    ... "2020/01/01 01:00:00",
    ... 'hourly',
    ... 'Generator',
    ... 'adjusted',
    ... [],
    ... 'D:/nemosis_data_cache')
          INTERVAL_DATETIME        BIN_NAME  BIDVOLUME
    0   2020-01-01 01:00:00       [-100, 0)      7.442
    1   2020-01-01 01:00:00   [-1000, -100)   9672.090
    2   2020-01-01 01:00:00         [0, 50)   4810.708
    3   2020-01-01 01:00:00      [100, 200)    300.000
    4   2020-01-01 01:00:00    [1000, 5000)   1004.000
    5   2020-01-01 01:00:00  [10000, 15500)   4359.000
    6   2020-01-01 01:00:00      [200, 300)   1960.000
    7   2020-01-01 01:00:00      [300, 500)    157.000
    8   2020-01-01 01:00:00       [50, 100)   1788.000
    9   2020-01-01 01:00:00     [500, 1000)    728.000
    10  2020-01-01 01:00:00   [5000, 10000)     20.000


    >>> aggregate_bids(
    ... ['QLD', 'NSW', 'SA'],
    ... "2020/01/01 00:00:00",
    ... "2020/01/01 00:05:00",
    ... '5-min',
    ... 'Generator',
    ... 'adjusted',
    ... [],
    ... 'D:/nemosis_data_cache')
          INTERVAL_DATETIME        BIN_NAME  BIDVOLUME
    0   2020-01-01 00:05:00       [-100, 0)      1.680
    1   2020-01-01 00:05:00   [-1000, -100)  10020.555
    2   2020-01-01 00:05:00         [0, 50)   5046.485
    3   2020-01-01 00:05:00      [100, 200)      0.000
    4   2020-01-01 00:05:00    [1000, 5000)   1004.000
    5   2020-01-01 00:05:00  [10000, 15500)   4279.000
    6   2020-01-01 00:05:00      [200, 300)   1270.000
    7   2020-01-01 00:05:00      [300, 500)    157.000
    8   2020-01-01 00:05:00       [50, 100)   1773.000
    9   2020-01-01 00:05:00     [500, 1000)    728.000
    10  2020-01-01 00:05:00   [5000, 10000)     20.000


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

    if resolution == 'hourly':
        bids = bids[bids['INTERVAL_DATETIME'].str[14:16] == '00'].copy()

    unit_info = fetch_and_preprocess.duid_info(raw_data_cache)

    unit_info = unit_info[unit_info['REGION'].isin(regions)].copy()
    unit_info = unit_info[unit_info['DISPATCH TYPE'] == dispatch_type].copy()

    if tech_types:
        unit_info = unit_info[unit_info['UNIT TYPE'].isin(tech_types)].copy()

    bids = bids[bids['DUID'].isin(unit_info['DUID'])].copy()

    bins = fetch_and_preprocess.define_and_return_price_bins()

    bids['d'] = 1
    bins['d'] = 1

    bids = pd.merge(bids, bins, on='d')
    bids = bids.drop(columns=['d'])

    bids = bids[(bids['BIDPRICE'] >= bids['LOWER_EDGE']) &
                (bids['BIDPRICE'] < bids['UPPER_EDGE'])].copy()

    if adjusted == 'raw':
        bids = bids.groupby(['INTERVAL_DATETIME', 'BIN_NAME'], as_index=False).agg({
            'BIDVOLUME': 'sum'
        })
    elif adjusted == 'adjusted':
        bids = bids.groupby(['INTERVAL_DATETIME', 'BIN_NAME'], as_index=False).agg({
            'BIDVOLUMEADJUSTED': 'sum'
        })
        bids = bids.rename(columns={'BIDVOLUMEADJUSTED': 'BIDVOLUME'})

    return bids


def duid_bids(duids, start_time, end_time, resolution, adjusted, raw_data_cache):
    """
    Function to query bidding data from a raw data cache. Data is filter according to the duids and time window
    provided, and returned on a duid basis. Data can queryed at hourly or 5 minute resolution. If an hourly resolution
    is chosen only bid for 5 minute interval ending on the hour are returned.

    Examples:

    >>> duid_bids(
    ... ['AGLHAL'],
    ... "2020/01/01 00:00:00",
    ... "2020/01/01 01:00:00",
    ... 'hourly',
    ... 'adjusted',
    ... 'D:/nemosis_data_cache')
            INTERVAL_DATETIME    DUID  BIDBAND  BIDVOLUME  BIDPRICE
    3748  2020-01-01 01:00:00  AGLHAL        7       60.0    564.22
    5705  2020-01-01 01:00:00  AGLHAL       10      121.0  13646.22


    >>> duid_bids(
    ... ['AGLHAL'],
    ... "2020/01/01 00:00:00",
    ... "2020/01/01 00:05:00",
    ... '5-min',
    ... 'adjusted',
    ... 'D:/nemosis_data_cache')
           INTERVAL_DATETIME    DUID  BIDBAND  BIDVOLUME  BIDPRICE
    314  2020-01-01 00:05:00  AGLHAL        7       60.0    564.22
    471  2020-01-01 00:05:00  AGLHAL       10      121.0  13646.22

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
    bids = bids[bids['DUID'].isin(duids)].copy()

    if resolution == 'hourly':
        bids = bids[bids['INTERVAL_DATETIME'].str[14:16] == '00'].copy()

    if adjusted == 'adjusted':
        bids = bids.loc[:, ['INTERVAL_DATETIME', 'DUID', 'BIDBAND', 'BIDVOLUMEADJUSTED', 'BIDPRICE']]
        bids = bids.rename(columns={'BIDVOLUMEADJUSTED': 'BIDVOLUME'})
    elif adjusted == 'raw':
        bids = bids.loc[:, ['INTERVAL_DATETIME', 'DUID', 'BIDBAND', 'BIDVOLUME', 'BIDPRICE']]

    return bids


def available_stations_and_duids(
        regions, start_time, end_time, dispatch_type, tech_types, raw_data_cache
):
    """
    Function to query units from given regions with bids available in the given time window, with the the given dispatch
    and technology type. Data returned is DUIDs and corresponding Station Names.

    Examples:

    >>> available_stations_and_duids(
    ... ['NSW'],
    ... "2020/01/01 00:00:00",
    ... "2020/01/01 00:05:00",
    ... "Generator",
    ... [],
    ... 'D:/nemosis_data_cache')
             DUID                                       STATION NAME
    35       BW01                            Bayswater Power Station
    36       BW02                            Bayswater Power Station
    37       BW03                            Bayswater Power Station
    38       BW04                            Bayswater Power Station
    46   BERYLSF1                                   Beryl Solar Farm
    47   BLOWERNG                            Blowering Power Station
    49   BOCORWF1                                Boco Rock Wind Farm
    51     BODWF1                                Bodangora Wind Farm
    69   BROKENH1                            Broken Hill Solar Plant
    125  COLEASF1                             Coleambally Solar Farm
    129       CG1                             Colongra Power Station
    130       CG2                             Colongra Power Station
    131       CG3                             Colongra Power Station
    132       CG4                             Colongra Power Station
    144  CROOKWF2                              Crookwell 2 Wind Farm
    175      ER01                              Eraring Power Station
    176      ER02                              Eraring Power Station
    177      ER03                              Eraring Power Station
    178      ER04                              Eraring Power Station
    182  FINLYSF1                                  Finley Solar Farm
    207  GULLRSF1                            Gullen Range Solar Farm
    208  GULLRWF1                             Gullen Range Wind Farm
    212  GUNNING1                                  Gunning Wind Farm
    213   GUTHEGA                              Guthega Power Station
    243   HUMENSW                     Hume (NSW) Hydro Power Station
    292      LD01                              Liddell Power Station
    293      LD02                              Liddell Power Station
    294      LD04                              Liddell Power Station
    297  LIMOSF21                             Limondale Solar Farm 2
    312   MANSLR1                                Manildra solar Farm
    330  MOREESF1                                   Moree Solar Farm
    351       MP1                             Mt Piper Power Station
    352       MP2                             Mt Piper Power Station
    365  NEVERSF1                               Nevertire Solar Farm
    370   NYNGAN1                                 Nyngan Solar Plant
    382    PARSF1                                  Parkes Solar Farm
    426   SAPHWF1                                 Sapphire Wind Farm
    429     SHGEN  Shoalhaven Power Station (Bendeela And Kangaro...
    433     STWF1                                Silverton Wind Farm
    434   SITHE01                         Smithfield Energy Facility
    465    TALWA1                           Tallawarra Power Station
    468  TARALGA1                                  Taralga Wind Farm
    496    TUMUT3                              Tumut 3 Power Station
    498  UPPTUMUT                                Tumut Power Station
    501   URANQ11                           Uranquinty Power Station
    502   URANQ12                           Uranquinty Power Station
    503   URANQ13                           Uranquinty Power Station
    504   URANQ14                           Uranquinty Power Station
    506       VP5                      Vales Point "B" Power Station
    507       VP6                      Vales Point "B" Power Station
    540     WRSF1                              White Rock Solar Farm
    541     WRWF1                               White Rock Wind Farm
    564  WOODLWN1                                 Woodlawn Wind Farm

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

    duids = bids['DUID'].unique()

    unit_info = fetch_and_preprocess.duid_info(raw_data_cache)

    unit_info = unit_info[unit_info['REGION'].isin(regions)].copy()
    unit_info = unit_info[unit_info['DISPATCH TYPE'] == dispatch_type].copy()

    if tech_types:
        unit_info = unit_info[unit_info['UNIT TYPE'].isin(tech_types)].copy()

    unit_info = unit_info[unit_info['DUID'].isin(duids)].copy()

    return unit_info.loc[:, ['DUID', 'STATION NAME']]


def get_aggregated_dispatch_data(
    regions, start_time, end_time, resolution, dispatch_type, tech_types, raw_data_cache
):
    """
    Function to query dispatch data from a raw data cache. Data is filter according to the regions, time window,
    dispatch type, and technology type  provided, and returned on a SETTLEMENTDATE basis. Data can queryed at hourly or
    5 minute resolution. If an hourly resolution is chosen only data for 5 minute interval ending on the hour are
    returned.

    Examples:

    >>> get_aggregated_dispatch_data(
    ... ['NSW'],
    ... "2020/01/01 00:00:00",
    ... "2020/01/01 01:05:00",
    ... 'hourly',
    ... 'Generator',
    ... [],
    ... 'D:/nemosis_data_cache')
        INTERVAL_DATETIME  AVAILABILITY  TOTALCLEARED     FINALMW  ASBIDRAMPUPMAXAVAIL  ASBIDRAMPDOWNMINAVAIL  RAMPUPMAXAVAIL  RAMPDOWNMINAVAIL  PASAAVAILABILITY  MAXAVAIL
    0  2020-01-01 01:00:00      12312.53    7300.30377  7314.30919          11230.36694             6622.32486     9810.956343        6622.84736           15561.0     13915


    >>> get_aggregated_dispatch_data(
    ... ['NSW'],
    ... "2020/01/01 00:00:00",
    ... "2020/01/01 00:10:00",
    ... '5-min',
    ... 'Generator',
    ... [],
    ... 'D:/nemosis_data_cache')
         INTERVAL_DATETIME  AVAILABILITY  TOTALCLEARED     FINALMW  ASBIDRAMPUPMAXAVAIL  ASBIDRAMPDOWNMINAVAIL  RAMPUPMAXAVAIL  RAMPDOWNMINAVAIL  PASAAVAILABILITY  MAXAVAIL
    0  2020-01-01 00:05:00     11571.306    7754.96901  7752.93848          11411.63319             6848.25917    10038.922683        6898.84417           15561.0     13095

    Arguments:
        regions: list[str] regions to aggregate should only be QLD, NSW, VIC, SA or TAS.
        start_time: Initial datetime, formatted "DD/MM/YYYY HH:MM:SS"
        end_time: Ending datetime, formatted identical to start_time
        resolution: str 'hourly' or '5-min'
        dispatch_type: str 'Generator' or 'Load'
        tech_types: list[str] the technology types to filter for e.g. Solar, Black Coal, CCGT. An empty list
            will result in no filtering by technology
        raw_data_cache: Filepath to directory for caching files downloaded from AEMO

    Returns:
        pd.DataFrame containing columns INTERVAL_DATETIME, ASBIDRAMPUPMAXAVAIL (upper dispatch limit based
        on as bid ramp rate), ASBIDRAMPDOWNMINAVAIL (lower dispatch limit based on as bid ramp rate), RAMPUPMAXAVAIL (
        upper dispatch limit based lesser of as bid and telemetry ramp rates), RAMPDOWNMINAVAIL (lower dispatch limit based
        lesser of as bid and telemetry ramp rates), AVAILABILITY, TOTALCLEARED (as for after_dispatch_metrics),
        PASAAVAILABILITY, MAXAVAIL (as for as_bid_metrics), and FINALMW (the unit operating level at the end of the dispatch
        interval).
    """
    dispatch = fetch_and_preprocess.unit_dispatch(start_time, end_time, raw_data_cache)

    if resolution == 'hourly':
        dispatch = dispatch[dispatch['INTERVAL_DATETIME'].str[14:16] == '00'].copy()

    unit_info = fetch_and_preprocess.duid_info(raw_data_cache)

    unit_info = unit_info[unit_info['REGION'].isin(regions)].copy()
    unit_info = unit_info[unit_info['DISPATCH TYPE'] == dispatch_type].copy()

    if tech_types:
        unit_info = unit_info[unit_info['UNIT TYPE'].isin(tech_types)].copy()

    dispatch = dispatch[dispatch['DUID'].isin(unit_info['DUID'])].copy()

    dispatch['ASBIDRAMPUPMAXAVAIL'] = np.where(dispatch['ASBIDRAMPUPMAXAVAIL'] > dispatch['MAXAVAIL'],
                                               dispatch['MAXAVAIL'],
                                               dispatch['ASBIDRAMPUPMAXAVAIL'])

    dispatch['ASBIDRAMPDOWNMINAVAIL'] = np.where(dispatch['ASBIDRAMPDOWNMINAVAIL'] < 0.0,
                                               0.0,
                                               dispatch['ASBIDRAMPDOWNMINAVAIL'])

    dispatch['RAMPUPMAXAVAIL'] = np.where(dispatch['RAMPUPMAXAVAIL'] > dispatch['AVAILABILITY'],
                                               dispatch['AVAILABILITY'],
                                               dispatch['RAMPUPMAXAVAIL'])

    dispatch['RAMPDOWNMINAVAIL'] = np.where(dispatch['RAMPDOWNMINAVAIL'] < 0.0,
                                            0.0,
                                            dispatch['RAMPDOWNMINAVAIL'])

    dispatch = dispatch.groupby('INTERVAL_DATETIME', as_index=False).agg({
        "AVAILABILITY": "sum", "TOTALCLEARED": "sum", "FINALMW": "sum", "ASBIDRAMPUPMAXAVAIL": "sum",
        "ASBIDRAMPDOWNMINAVAIL": "sum", "RAMPUPMAXAVAIL": "sum", "RAMPDOWNMINAVAIL": "sum",
        "PASAAVAILABILITY": "sum", "MAXAVAIL": "sum"
    })

    return dispatch


def get_aggregated_dispatch_data_by_duids(
    duids, start_time, end_time, resolution, raw_data_cache
):
    """
    Function to query dispatch data from a raw data cahce. Data is filter according to the duids and time window
    provided, and returned on a duid basis. Data can queryed at hourly or 5 minute resolution. If a hourly resolution is
    chosen only bid for 5 minute interval ending on the hour are returned.

    Examples:

    >>> get_aggregated_dispatch_data_by_duids(
    ... ['AGLHAL'],
    ... "2020/01/01 00:00:00",
    ... "2020/01/01 01:05:00",
    ... 'hourly',
    ... 'D:/nemosis_data_cache')
         INTERVAL_DATETIME  AVAILABILITY  TOTALCLEARED  FINALMW  ASBIDRAMPUPMAXAVAIL  ASBIDRAMPDOWNMINAVAIL  RAMPUPMAXAVAIL  RAMPDOWNMINAVAIL  PASAAVAILABILITY  MAXAVAIL
    0  2020-01-01 01:00:00         181.0           0.0      0.0                 60.0                    0.0            60.0               0.0             181.0       181


    >>> get_aggregated_dispatch_data_by_duids(
    ... ['AGLHAL'],
    ... "2020/01/01 00:00:00",
    ... "2020/01/01 00:10:00",
    ... '5-min',
    ... 'D:/nemosis_data_cache')
        INTERVAL_DATETIME  AVAILABILITY  TOTALCLEARED  FINALMW  ASBIDRAMPUPMAXAVAIL  ASBIDRAMPDOWNMINAVAIL  RAMPUPMAXAVAIL  RAMPDOWNMINAVAIL  PASAAVAILABILITY  MAXAVAIL
    0  2020-01-01 00:05:00         181.0           0.0      0.0                 60.0                    0.0            60.0               0.0             181.0       181


    Arguments:
        duids: list[str] of duids to return in result.
        start_time: Initial datetime, formatted "DD/MM/YYYY HH:MM:SS"
        end_time: Ending datetime, formatted identical to start_time
        resolution: str 'hourly' or '5-min'
        raw_data_cache: Filepath to directory for caching files downloaded from AEMO

    Returns:
        pd.DataFrame containing columns INTERVAL_DATETIME, ASBIDRAMPUPMAXAVAIL (upper dispatch limit based
        on as bid ramp rate), ASBIDRAMPDOWNMINAVAIL (lower dispatch limit based on as bid ramp rate), RAMPUPMAXAVAIL (
        upper dispatch limit based lesser of as bid and telemetry ramp rates), RAMPDOWNMINAVAIL (lower dispatch limit based
        lesser of as bid and telemetry ramp rates), AVAILABILITY, TOTALCLEARED (as for after_dispatch_metrics),
        PASAAVAILABILITY, MAXAVAIL (as for as_bid_metrics), and FINALMW (the unit operating level at the end of the dispatch
        interval).
    """

    dispatch = fetch_and_preprocess.unit_dispatch(start_time, end_time, raw_data_cache)

    dispatch = dispatch[dispatch['DUID'].isin(duids)].copy()

    if resolution == 'hourly':
        dispatch = dispatch[dispatch['INTERVAL_DATETIME'].str[14:16] == '00'].copy()

    dispatch['ASBIDRAMPUPMAXAVAIL'] = np.where(dispatch['ASBIDRAMPUPMAXAVAIL'] > dispatch['MAXAVAIL'],
                                               dispatch['MAXAVAIL'],
                                               dispatch['ASBIDRAMPUPMAXAVAIL'])

    dispatch['ASBIDRAMPDOWNMINAVAIL'] = np.where(dispatch['ASBIDRAMPDOWNMINAVAIL'] < 0.0,
                                               0.0,
                                               dispatch['ASBIDRAMPDOWNMINAVAIL'])

    dispatch['RAMPUPMAXAVAIL'] = np.where(dispatch['RAMPUPMAXAVAIL'] > dispatch['AVAILABILITY'],
                                               dispatch['AVAILABILITY'],
                                               dispatch['RAMPUPMAXAVAIL'])

    dispatch['RAMPDOWNMINAVAIL'] = np.where(dispatch['RAMPDOWNMINAVAIL'] < 0.0,
                                            0.0,
                                            dispatch['RAMPDOWNMINAVAIL'])

    dispatch = dispatch.groupby('INTERVAL_DATETIME', as_index=False).agg({
        "AVAILABILITY": "sum", "TOTALCLEARED": "sum", "FINALMW": "sum", "ASBIDRAMPUPMAXAVAIL": "sum",
        "ASBIDRAMPDOWNMINAVAIL": "sum", "RAMPUPMAXAVAIL": "sum", "RAMPDOWNMINAVAIL": "sum",
        "PASAAVAILABILITY": "sum", "MAXAVAIL": "sum"
    })

    return dispatch


def unit_types(raw_data_cache):
    """
    Function to query distinct unit types from raw data cache.

    Examples:

    >>> unit_types('D:/nemosis_data_cache')
                       UNIT TYPE
    0         Battery Discharge
    1            Battery Charge
    4        Run of River Hydro
    5                     Solar
    8                    Engine
    11                     Wind
    14                     OCGT
    24                    Hydro
    27                     CCGT
    35               Black Coal
    66                  Bagasse
    302              Brown Coal
    366             Gas Thermal
    423  Pump Storage Discharge
    431     Pump Storage Charge

    Args:
        raw_data_cache: Filepath to directory for caching files downloaded from AEMO

    Returns:
        pd.DataFrame column UNIT TYPE (this is the unit type as determined by the function
        :py:func:`nem_bidding_dashboard.preprocessing.tech_namer_by_row`)
    """
    data = fetch_and_preprocess.duid_info(raw_data_cache)
    data = data.loc[:, ['UNIT TYPE']].drop_duplicates()
    return data
