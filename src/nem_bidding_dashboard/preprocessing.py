import datetime

import numpy as np
import pandas as pd

pd.set_option("display.width", None)


def stack_unit_bids(volume_bids, price_bids):
    """
    Combine volume and price components of energy market offers and reformat them such that each price quantity pair
    is on a separate row of the dataframe.

    Examples:

    >>> volume_bids = pd.DataFrame(
    ... columns=["INTERVAL_DATETIME", "SETTLEMENTDATE", "DUID", "BANDAVAIL1", "BANDAVAIL2", "BANDAVAIL3",
    ...          "BANDAVAIL4", "BANDAVAIL5", "BANDAVAIL6", "BANDAVAIL7", "BANDAVAIL8", "BANDAVAIL9", "BANDAVAIL10"],
    ... data=[('2020/01/01 00:45:00', '2019/12/31 00:00:00', 'AGLHAL', 1, 2, 3, 4, 5, 6, 7, 8, 9, 10)])

    >>> volume_bids
         INTERVAL_DATETIME       SETTLEMENTDATE  ... BANDAVAIL9  BANDAVAIL10
    0  2020/01/01 00:45:00  2019/12/31 00:00:00  ...          9           10
    <BLANKLINE>
    [1 rows x 13 columns]

    >>> price_bids = pd.DataFrame(
    ... columns=["SETTLEMENTDATE", "DUID", "PRICEBAND1", "PRICEBAND2", "PRICEBAND3", "PRICEBAND4", "PRICEBAND5",
    ...          "PRICEBAND6", "PRICEBAND7", "PRICEBAND8", "PRICEBAND9", "PRICEBAND10"],
    ... data=[('2019/12/31 00:00:00', 'AGLHAL', 1, 2, 3, 4, 5, 6, 7, 8, 9, 10)])

    >>> price_bids
            SETTLEMENTDATE    DUID  PRICEBAND1  ...  PRICEBAND8  PRICEBAND9  PRICEBAND10
    0  2019/12/31 00:00:00  AGLHAL           1  ...           8           9           10
    <BLANKLINE>
    [1 rows x 12 columns]

    >>> stack_unit_bids(volume_bids, price_bids)
         INTERVAL_DATETIME    DUID  BIDBAND  BIDVOLUME  BIDPRICE
    0  2020/01/01 00:45:00  AGLHAL        1          1         1
    1  2020/01/01 00:45:00  AGLHAL        2          2         2
    2  2020/01/01 00:45:00  AGLHAL        3          3         3
    3  2020/01/01 00:45:00  AGLHAL        4          4         4
    4  2020/01/01 00:45:00  AGLHAL        5          5         5
    5  2020/01/01 00:45:00  AGLHAL        6          6         6
    6  2020/01/01 00:45:00  AGLHAL        7          7         7
    7  2020/01/01 00:45:00  AGLHAL        8          8         8
    8  2020/01/01 00:45:00  AGLHAL        9          9         9
    9  2020/01/01 00:45:00  AGLHAL       10         10        10

    Arguments:
        volume_bids: pd dataframe containing quantity of bids on a 5 minutely basis. Should have columns
            INTERVAL_DATETIME, SETTLEMENTDATE, DUID, BANDAVAIL1 . . . . BANDAVAIL10
        price_bids: pd dataframe containing quantity of bids on a daily basis. Should have columns
            SETTLEMENTDATE, DUID, PRICEBAND1 . . . . PRICEBAND10
    Returns:
        pd dataframe containing matched quantity and price pairs on a 5 minutely basis with bid bands on row basis.
        Should have columns INTERVAL_DATETIME, DUID, BIDBAND, BIDVOLUME and BIDPRICE
    """
    volume_bids = pd.melt(
        volume_bids,
        id_vars=["INTERVAL_DATETIME", "SETTLEMENTDATE", "DUID"],
        value_vars=[
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
        ],
        var_name="BIDBAND",
        value_name="BIDVOLUME",
    )
    price_bids = pd.melt(
        price_bids,
        id_vars=["SETTLEMENTDATE", "DUID"],
        value_vars=[
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
        var_name="BIDBAND",
        value_name="BIDPRICE",
    )
    price_bids["BIDBAND"] = pd.to_numeric(price_bids["BIDBAND"].str[9:])
    volume_bids["BIDBAND"] = pd.to_numeric(volume_bids["BIDBAND"].str[9:])
    bids = pd.merge(volume_bids, price_bids, on=["SETTLEMENTDATE", "BIDBAND", "DUID"])
    return bids.loc[
        :, ["INTERVAL_DATETIME", "DUID", "BIDBAND", "BIDVOLUME", "BIDPRICE"]
    ]


def adjust_bids_for_availability(stacked_bids, unit_availability):
    """
    Adjust bid volumes where the unit availability would restrict a bid from actually being fully dispatched.
    Starting from the highest bid bands bid volumes are adjusted down until the total bid volume is equal to the
    availability, if the total bid volume is already equal to or less than availability no adjustments are made.

    Examples:

    >>> bid_data = pd.DataFrame(
    ... columns=["INTERVAL_DATETIME", "DUID", "BIDBAND", "BIDVOLUME", "BIDPRICE"],
    ... data=[('2020/01/01 00:45:00', 'AGLHAL', 1, 50, 120),
    ...       ('2020/01/01 00:45:00', 'AGLHAL', 2, 50, 200)])

    >>> bid_data
         INTERVAL_DATETIME    DUID  BIDBAND  BIDVOLUME  BIDPRICE
    0  2020/01/01 00:45:00  AGLHAL        1         50       120
    1  2020/01/01 00:45:00  AGLHAL        2         50       200

    >>> unit_availability = pd.DataFrame(
    ... columns=["SETTLEMENTDATE", "DUID", "AVAILABILITY"],
    ... data=[('2020/01/01 00:45:00', 'AGLHAL', 80),])

    >>> unit_availability
            SETTLEMENTDATE    DUID  AVAILABILITY
    0  2020/01/01 00:45:00  AGLHAL            80

    >>> adjust_bids_for_availability(bid_data, unit_availability)
         INTERVAL_DATETIME    DUID  BIDBAND  BIDVOLUME  BIDVOLUMEADJUSTED  BIDPRICE
    0  2020/01/01 00:45:00  AGLHAL        1         50                 50       120
    1  2020/01/01 00:45:00  AGLHAL        2         50                 30       200

    Arguments:
        stacked_bids: pd.dataframe containing matched quantity and price pairs on a 5 minutely basis with bid bands on
            row basis. Should have columns INTERVAL_DATETIME, DUID, BIDBAND, BIDVOLUME and BIDPRICE
        unit_availability: pd.dataframe containing unit availability values on 5 minutely basis. Should have columns
            SETTLEMENTDATE, DUID, AVAILABILITY
    Returns:
        pd.dataframe containing matched quantity and price pairs on a 5 minutely basis with bid bands on
        row basis. An extra column is added in which did volumes have been adjusted so total bid volume doesn't exceed
        unit availability. Should have columns INTERVAL_DATETIME, DUID, BIDBAND, BIDVOLUME, BIDVOLUMEADJUSTED and
        BIDPRICE
    """

    bids = stacked_bids.sort_values("BIDBAND")
    bids["BIDVOLUMECUMULATIVE"] = bids.groupby(
        ["DUID", "INTERVAL_DATETIME"], as_index=False
    )["BIDVOLUME"].cumsum()
    availability = unit_availability.rename(
        {"SETTLEMENTDATE": "INTERVAL_DATETIME"}, axis=1
    )
    bids = pd.merge(bids, availability, "left", on=["INTERVAL_DATETIME", "DUID"])
    bids["SPAREBIDVOLUME"] = (
        bids["AVAILABILITY"] - bids["BIDVOLUMECUMULATIVE"]
    ) + bids["BIDVOLUME"]
    bids["SPAREBIDVOLUME"] = np.where(
        bids["SPAREBIDVOLUME"] < 0, 0, bids["SPAREBIDVOLUME"]
    )
    bids["BIDVOLUMEADJUSTED"] = bids[["BIDVOLUME", "SPAREBIDVOLUME"]].min(axis=1)
    return bids.loc[
        :,
        [
            "INTERVAL_DATETIME",
            "DUID",
            "BIDBAND",
            "BIDVOLUME",
            "BIDVOLUMEADJUSTED",
            "BIDPRICE",
        ],
    ]


def remove_number_from_region_names(region_column, data):
    """
    Removes the trailing 1 from region names in the specified column. Names in input data are expected to be of the
    format NSW1, QLD1 etc. and the names in the output format will be NSW, QLD etc.

    Examples:

    >>> region_data = pd.DataFrame({
    ... 'region': ['QLD1', 'TAS1'],
    ... 'dummy_values': [55.7, 102.9]})

    >>> region_data
      region  dummy_values
    0   QLD1          55.7
    1   TAS1         102.9

    >>> remove_number_from_region_names('region', region_data)
      region  dummy_values
    0    QLD          55.7
    1    TAS         102.9

    Arguments:
        region_column: str the name of the column containing the region names.
        data: pd dataframe with a column called the value of region_column.
    Returns:
        pd dataframe with names in region_column modified.
    """
    data.loc[:, region_column] = data[region_column].str[:-1]
    return data


def tech_namer_by_row(fuel, tech_descriptor, dispatch_type):
    """
    Create a name for generation and loads using custom logic that considers the fuel type, technology descriptor, and
    dispatch type supplied by AEMO in the NEM Registration and Exemption List.xls file. See the
    `source code <https://github.com/UNSW-CEEM/nem-bidding-dashboard/blob/6468e6b5506c778d12b3d3653af1cf11bd28807b/src/nem_bidding_dashboard/preprocessing.py#L123>`_
    for logic that this function applies:

    Examples:

    >>> tech_namer_by_row("Natural Gas", "Steam Sub-Critical", "Generator")
    'Gas Thermal'

    >>> tech_namer_by_row("solar", "PV - Tracking", "Generator")
    'Solar'

    >>> tech_namer_by_row("-", "Battery", "Load")
    'Battery Charge'


    Arguments:
        fuel: str should be the value from the column 'Fuel Source - Descriptor'
        tech_descriptor: str should be the value from the column 'Technology Type - Descriptor'
        dispatch_type: str should be the value from the column 'Dispatch Type'

    Returns:
        str a value categorising the technology of the generation or load unit
    """
    name = fuel
    if fuel in ["Solar", "Wind", "Black Coal", "Brown Coal"]:
        pass
    elif fuel == "solar":
        name = "Solar"
    elif tech_descriptor in ["Battery", "Battery and Inverter"]:
        if dispatch_type == "Load":
            name = "Battery Charge"
        else:
            name = "Battery Discharge"
    elif tech_descriptor in ["Hydro - Gravity"]:
        name = "Hydro"
    elif tech_descriptor in ["Hydro - Gravity", "Run of River"]:
        name = "Run of River Hydro"
    elif tech_descriptor == "Pump Storage":
        if dispatch_type == "Load":
            name = "Pump Storage Charge"
        else:
            name = "Pump Storage Discharge"
    elif tech_descriptor == "-" and fuel == "-" and dispatch_type == "Load":
        name = "Pump Storage Charge"
    elif tech_descriptor == "Open Cycle Gas turbines (OCGT)":
        name = "OCGT"
    elif tech_descriptor == "Combined Cycle Gas Turbine (CCGT)":
        name = "CCGT"
    elif (
        fuel in ["Natural Gas / Fuel Oil", "Natural Gas"]
        and tech_descriptor == "Steam Sub-Critical"
    ):
        name = "Gas Thermal"
    elif isinstance(tech_descriptor, str) and "Engine" in tech_descriptor:
        name = "Engine"
    return name


def tech_namer(duid_info):
    """
    Create a name for generation and load unit using custom logic applied to the fuel type, technology descriptor, and
    dispatch type supplied by AEMO in the NEM Registration and Exemption List.xls file. See the
    `source code <https://github.com/UNSW-CEEM/nem-bidding-dashboard/blob/6468e6b5506c778d12b3d3653af1cf11bd28807b/src/nem_bidding_dashboard/preprocessing.py#L123>`_
    for logic that this function applies:

    Arguments:
        duid_info: pd dataframe with columns 'FUEL SOURCE - DESCRIPTOR', 'TECHNOLOGY TYPE - DESCRIPTOR' and
            'DISPATCH TYPE'
    Returns:
        pd dataframe with additional column 'UNIT_TYPE'
    """
    duid_info["UNIT TYPE"] = duid_info.apply(
        lambda x: tech_namer_by_row(
            x["FUEL SOURCE - DESCRIPTOR"],
            x["TECHNOLOGY TYPE - DESCRIPTOR"],
            x["DISPATCH TYPE"],
        ),
        axis=1,
    )
    return duid_info


def hard_code_fix_fuel_source_and_tech_errors(duid_data):
    """
    Where NA values occur in columns FUEL SOURCE - DESCRIPTOR or TECHNOLOGY TYPE - DESCRIPTOR replace these with the
    value '-'. This function is used to clean data before passing to
    :py:func:`nem_bidding_dashboard.preprocessing.tech_namer`.

    Args:
        duid_data: pd.DataFrame containing at least the columns FUEL SOURCE - DESCRIPTOR and
            TECHNOLOGY TYPE - DESCRIPTOR

    Returns:
        pd.DataFrame with the same columns as input data. Columns FUEL SOURCE - DESCRIPTOR and
        TECHNOLOGY TYPE - DESCRIPTOR have NA values replaced with '-'.

    """
    duid_data.loc[
        duid_data["FUEL SOURCE - DESCRIPTOR"].isna(), "FUEL SOURCE - DESCRIPTOR"
    ] = "-"
    duid_data.loc[
        duid_data["TECHNOLOGY TYPE - DESCRIPTOR"].isna(), "TECHNOLOGY TYPE - DESCRIPTOR"
    ] = "-"
    return duid_data


def calculate_unit_time_series_metrics(as_bid_metrics, after_dispatch_metrics):
    """
    Calculate some additional values associated with unit dispatch. These are used to populate the database table
    unit_dispatch and should be helpful in understanding unit dispatch outcomes, e.g. show when a plant is limited
    by its ramp rate, or if a unit's availability is the same as the availability submitted for the PASA process
    indicating that output is probably limited by a technical constraint.

    Examples:

    >>> as_bid_metrics = pd.DataFrame({
    ... 'INTERVAL_DATETIME': ['2022/01/01 00:00:00', '2022/01/01 00:05:00', '2022/01/01 00:10:00'],
    ... 'DUID': ['AGLHAL', 'AGLHAL', 'AGLHAL'],
    ... 'ROCUP': [10, 15, 60],
    ... 'ROCDOWN': [20, 25, 30],
    ... 'MAXAVAIL': [100, 90,  105],
    ... 'PASAAVAILABILITY': [120, 90, 110],
    ... })

    >>> as_bid_metrics['INTERVAL_DATETIME'] = pd.to_datetime(as_bid_metrics['INTERVAL_DATETIME'])

    >>> as_bid_metrics
        INTERVAL_DATETIME    DUID  ROCUP  ROCDOWN  MAXAVAIL  PASAAVAILABILITY
    0 2022-01-01 00:00:00  AGLHAL     10       20       100               120
    1 2022-01-01 00:05:00  AGLHAL     15       25        90                90
    2 2022-01-01 00:10:00  AGLHAL     60       30       105               110

    >>> after_dispatch_metrics = pd.DataFrame({
    ... 'SETTLEMENTDATE': ['2022/01/01 00:00:00', '2022/01/01 00:05:00', '2022/01/01 00:10:00'],
    ... 'DUID': ['AGLHAL', 'AGLHAL', 'AGLHAL'],
    ... 'RAMPUPRATE': [9*60, 14*60, 50*60],
    ... 'RAMPDOWNRATE': [18*60, 24*60, 28*60],
    ... 'AVAILABILITY': [100, 90,  105],
    ... 'INITIALMW': [80.1, 88.9,  84.5],
    ... 'TOTALCLEARED': [80, 90,  85],
    ...  })

    >>> after_dispatch_metrics['SETTLEMENTDATE'] = pd.to_datetime(after_dispatch_metrics['SETTLEMENTDATE'])

    >>> after_dispatch_metrics
           SETTLEMENTDATE    DUID  RAMPUPRATE  RAMPDOWNRATE  AVAILABILITY  INITIALMW  TOTALCLEARED
    0 2022-01-01 00:00:00  AGLHAL         540          1080           100       80.1            80
    1 2022-01-01 00:05:00  AGLHAL         840          1440            90       88.9            90
    2 2022-01-01 00:10:00  AGLHAL        3000          1680           105       84.5            85

    >>> calculate_unit_time_series_metrics(as_bid_metrics, after_dispatch_metrics)
        INTERVAL_DATETIME    DUID  AVAILABILITY  TOTALCLEARED  FINALMW  ASBIDRAMPUPMAXAVAIL  ASBIDRAMPDOWNMINAVAIL  RAMPUPMAXAVAIL  RAMPDOWNMINAVAIL  PASAAVAILABILITY  MAXAVAIL
    0 2022-01-01 00:00:00  AGLHAL           100            80     88.9                130.1                  -19.9           125.1              -9.9               120       100
    1 2022-01-01 00:05:00  AGLHAL            90            90     84.5                163.9                  -36.1           158.9             -31.1                90        90

    Args:
        as_bid_metrics: pd.DataFrame containing values submitted by unit's as part of the bidding process. Should
            contain columns INTERVAL_DATETIME, DUID, ROCUP (ramp up rate in MW per min), ROCDOWN (ramp down rate in MW per
            min), MAXAVAIL (limit the unit can be dispatched up to), PASAAVAILABILITY (The technical maximum availability of
            unit given 24h notice, not used in dispatch)
        after_dispatch_metrics: pd.DataFrame containing values calculated by AEMO as part of the dispatch process.
            Should contain columns SETTLEMENTDATE, DUID, AVAILABILITY (presumed to be the lesser of the unit bid
            availability (MAXAVAIL column) and unit forecast availability for variable renewables), RAMPUPRATE, RAMPDOWNRATE
            (lesser of bid and telemetry ramp rates, MW per hour), INITIALMW (operating level of unit at start of dispatch
            interval), TOTALCLEARED (dispatch target for unit to ramp to by end of dispatch interval).

    Returns:
        pd.DataFrame containing columns INTERVAL_DATETIME, DUID, ASBIDRAMPUPMAXAVAIL (upper dispatch limit based
        on as bid ramp rate), ASBIDRAMPDOWNMINAVAIL (lower dispatch limit based on as bid ramp rate), RAMPUPMAXAVAIL (
        upper dispatch limit based lesser of as bid and telemetry ramp rates), RAMPDOWNMINAVAIL (lower dispatch limit based
        lesser of as bid and telemetry ramp rates), AVAILABILITY, TOTALCLEARED (as for after_dispatch_metrics),
        PASAAVAILABILITY, MAXAVAIL (as for as_bid_metrics), and FINALMW (the unit operating level at the end of the dispatch
        interval).
    """

    after_dispatch_metrics = after_dispatch_metrics.rename(
        {"SETTLEMENTDATE": "INTERVAL_DATETIME"}, axis=1
    )
    unit_time_series_metrics = pd.merge(
        as_bid_metrics, after_dispatch_metrics, on=["INTERVAL_DATETIME", "DUID"]
    )

    unit_time_series_metrics["ASBIDRAMPUPMAXAVAIL"] = (
        unit_time_series_metrics["INITIALMW"] + unit_time_series_metrics["ROCUP"] * 5
    )
    unit_time_series_metrics["ASBIDRAMPDOWNMINAVAIL"] = (
        unit_time_series_metrics["INITIALMW"] - unit_time_series_metrics["ROCDOWN"] * 5
    )
    unit_time_series_metrics["RAMPUPMAXAVAIL"] = (
        unit_time_series_metrics["INITIALMW"]
        + unit_time_series_metrics["RAMPUPRATE"] / 12
    )
    unit_time_series_metrics["RAMPDOWNMINAVAIL"] = (
        unit_time_series_metrics["INITIALMW"]
        - unit_time_series_metrics["RAMPDOWNRATE"] / 12
    )

    final_mw = unit_time_series_metrics.loc[
        :, ["INTERVAL_DATETIME", "DUID", "INITIALMW"]
    ]
    final_mw["INTERVAL_DATETIME"] -= datetime.timedelta(minutes=5)
    final_mw = final_mw.rename({"INITIALMW": "FINALMW"}, axis=1)

    unit_time_series_metrics = pd.merge(
        unit_time_series_metrics, final_mw, on=["INTERVAL_DATETIME", "DUID"]
    )

    return unit_time_series_metrics.loc[
        :,
        [
            "INTERVAL_DATETIME",
            "DUID",
            "AVAILABILITY",
            "TOTALCLEARED",
            "FINALMW",
            "ASBIDRAMPUPMAXAVAIL",
            "ASBIDRAMPDOWNMINAVAIL",
            "RAMPUPMAXAVAIL",
            "RAMPDOWNMINAVAIL",
            "PASAAVAILABILITY",
            "MAXAVAIL",
        ],
    ]


def add_on_hour_column(data):
    data["ONHOUR"] = np.where(data["INTERVAL_DATETIME"].dt.minute == 0, True, False)
    return data
