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
