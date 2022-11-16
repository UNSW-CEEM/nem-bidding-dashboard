import datetime

import numpy as np
import pandas as pd


def stack_unit_bids(volume_bids, price_bids):
    """Combine volume and price components of energy market offers and reformat them such that each price quantity pair
       is on a separate row of the dataframe.

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
    """Adjust bid volumes where the unit availability would restrict a bid from actually being fully dispatched.

    Arguments:
        stacked_bids: pd dataframe containing matched quantity and price pairs on a 5 minutely basis with bid bands on
            row basis. Should have columns INTERVAL_DATETIME, DUID, BIDBAND, BIDVOLUME and BIDPRICE
        unit_availability: pd dataframe containing unit availability values on 5 minutely basis. Should have columns
            SETTLEMENTDATE, DUID, AVAILABILITY
     Returns:
        pd dataframe containing matched quantity and price pairs on a 5 minutely basis with bid bands on
        row basis. Bid volumes have been adjusted so total bid volume doesn't exceed unit availability. Should have
        columns INTERVAL_DATETIME, DUID, BIDBAND, BIDVOLUME and BIDPRICE


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
    bids["BIDVOLUME"] = bids[["BIDVOLUME", "SPAREBIDVOLUME"]].min(axis=1)
    return bids.loc[
        :, ["INTERVAL_DATETIME", "DUID", "BIDBAND", "BIDVOLUME", "BIDPRICE"]
    ]


def remove_number_from_region_names(region_column, data):
    """
    Removes the trailing 1 from region names in the specified column. Names in input data are expected to be of the
    format NSW1, QLD1 etc. and the names in the output format will be NSW, QLD etc.

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
    Create a name for generation and loads using custom logic applied to the fuel type, technology descriptor, and
    dispatch type supplied by AEMO in the NEM Registration and Exemption List.xls file.

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
    dispatch type supplied by AEMO in the NEM Registration and Exemption List.xls file.

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
    duid_data.loc[
        duid_data["FUEL SOURCE - DESCRIPTOR"].isna(), "FUEL SOURCE - DESCRIPTOR"
    ] = "-"
    duid_data.loc[
        duid_data["TECHNOLOGY TYPE - DESCRIPTOR"].isna(), "TECHNOLOGY TYPE - DESCRIPTOR"
    ] = "-"
    return duid_data


def calculate_unit_time_series_metrics(as_bid_metrics, after_dispatch_metrics):
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
