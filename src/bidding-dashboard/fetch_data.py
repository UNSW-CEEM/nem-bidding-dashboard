from nemosis import dynamic_data_compiler, static_table

"""
Function to fetch electricity demand data using NEMOSIS for use in
plot_region_demand. Simply returns fetched dataframe containing required data at
the moment, may be updated if a different method is used to get or store data.
Also generates feather files for each month of data that is collected.
Arguments:
    start_date: Initial datetime, formatted "DD/MM/YYYY HH:MM:SS" (time always
        set to "00:00:00:)
    end_date: Ending datetime, formatted identical to start_date
    raw_data_cache: Filepath to directory for storing data that is fetched
Returns:
    pd dataframe containing REGIONID, SETTLEMENTDATE and TOTALDEMAND columns
    from the DISPATCHREGIONSUM table from AEMO, covering the specified time
    period.
"""


def get_region_demand_data(start_time: str, end_time: str, raw_data_cache: str):
    region_data = dynamic_data_compiler(
        start_time,
        end_time,
        "DISPATCHREGIONSUM",
        raw_data_cache,
        keep_csv=True,
        select_columns=["REGIONID", "SETTLEMENTDATE", "TOTALDEMAND", "INTERVENTION"],
    )
    region_data = region_data.loc[region_data["INTERVENTION"] == 0]
    region_data = region_data.filter(["REGIONID", "SETTLEMENTDATE", "TOTALDEMAND"])
    return region_data


"""
TODO: update this
Function to fetch electricity demand data using NEMOSIS for use in
plot_region_demand. Simply returns fetched dataframe containing required data at
the moment, may be updated if a different method is used to get or store data.
Also generates feather files for each month of data that is collected.
Arguments:
    start_date: Initial datetime, formatted "DD/MM/YYYY HH:MM:SS" (time always
        set to "00:00:00:)
    end_date: Ending datetime, formatted identical to start_date
    raw_data_cache: Filepath to directory for storing data that is fetched
Returns:
    pd dataframe containing REGIONID, SETTLEMENTDATE and TOTALDEMAND columns
    from the DISPATCHREGIONSUM table from AEMO, covering the specified time
    period.
"""


def get_duid_availability_data(start_time: str, end_time: str, raw_data_cache: str):
    availability_data = dynamic_data_compiler(
        start_time,
        end_time,
        "DISPATCHLOAD",
        raw_data_cache,
        keep_csv=True,
        select_columns=["SETTLEMENTDATE", "INTERVENTION", "DUID", "AVAILABILITY"],
    )
    availability_data = availability_data.loc[availability_data["INTERVENTION"] == 0]
    return availability_data.loc[:, ["SETTLEMENTDATE", "DUID", "AVAILABILITY"]]


def get_duid_data(raw_data_cache: str):
    duid_data = static_table(
        "Generators and Scheduled Loads",
        raw_data_cache,
        select_columns=["Region", "Fuel Source - Primary", "DUID", "Dispatch Type"],
    )
    duid_data = duid_data.loc[duid_data["Dispatch Type"] == "Generator"]
    duid_data = duid_data.loc[duid_data["Fuel Source - Primary"] != ""]
    duid_data = duid_data.filter(["Region", "Fuel Source - Primary", "DUID"])
    return duid_data


def get_volume_bids(start_time: str, end_time: str, raw_data_cache: str):
    """
    Fetch volume bid data using NEMOSIS. Caches feather file in specified directory for each month of data that is
    collected.

    Arguments:
        start_date: Initial datetime, formatted "DD/MM/YYYY HH:MM:SS" (time always
            set to "00:00:00:)
        end_date: Ending datetime, formatted identical to start_date
        raw_data_cache: Filepath to directory for storing data that is fetched
    Returns:
        pd dataframe containing quantity of bids on a 5 minutely basis. Should have columns
            INTERVAL_DATETIME, SETTLEMENTDATE, DUID, BANDAVAIL1 . . . . BANDAVAIL10
    """
    volume_bids = dynamic_data_compiler(
        start_time=start_time,
        end_time=end_time,
        table_name="BIDPEROFFER_D",
        raw_data_location=raw_data_cache,
        fformat="parquet",
        keep_csv=False,
        select_columns=[
            "INTERVAL_DATETIME",
            "SETTLEMENTDATE",
            "DUID",
            "BIDTYPE",
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
    )
    return volume_bids


def get_price_bids(start_time: str, end_time: str, raw_data_cache: str):
    """
    Fetch price bid data using NEMOSIS. Caches feather file in specified directory for each month of data that is
    collected.

    Arguments:
        start_date: Initial datetime, formatted "DD/MM/YYYY HH:MM:SS" (time always
            set to "00:00:00:)
        end_date: Ending datetime, formatted identical to start_date
        raw_data_cache: Filepath to directory for storing data that is fetched
    Returns:
        pd dataframe containing prices of bids on a (market) daily basis. Should have columns
            SETTLEMENTDATE, DUID, PRICEBAND1 . . . . PRICEBAND10
    """
    volume_bids = dynamic_data_compiler(
        start_time=start_time,
        end_time=end_time,
        table_name="BIDDAYOFFER_D",
        raw_data_location=raw_data_cache,
        fformat="parquet",
        keep_csv=False,
        select_columns=[
            "SETTLEMENTDATE",
            "DUID",
            "BIDTYPE",
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
    )
    return volume_bids


if __name__ == "__main__":
    raw_data_cache = (
        "/home/pat/bidding-dashboard/src/bidding-dashboard/nemosis_data_cache/"
    )
    get_duid_data(raw_data_cache)
    # get_region_demand_data("2019/01/23 00:00:00", "2019/01/28 00:00:00", raw_data_cache)
