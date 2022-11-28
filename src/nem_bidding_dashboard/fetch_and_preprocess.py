import fetch_data
import pandas as pd
import preprocessing


def region_data(start_date, end_date, raw_data_cache):
    """ Wrapper for fetching and preprocessing regional demand and price data. Calls
    :py:function:fetch_data.get_region_data to get regional data, re-formats REGIONID by removing the trailing '1'
    using :py:function:preprocessing.remove_number_from_region_names, and finally converting datetime columns to
    string format. Used for preparing data to load into dashboard backend PostgresSQL database, or can be used for
    compiling regional data directly if the user does not have database, but compling data using this function will be
    considerably slower than from a PostgresSQL database.

    Examples:
    >>> region_data(
    ... '2020/01/01 00:00:00',
    ... '2020/01/01 00:01:00',
    ... 'D:/nemosis_data_cache')

    Args:
       start_date: str formatted "DD/MM/YYYY HH:MM:SS", data with date times greater than start_date are returned
       end_date: str formatted identical to start_date, data with date times less than or equal to end_date are returned
       raw_data_cache: Filepath to directory for caching files downloaded from AEMO

    Returns: pandas dataframe with columns SETTLEMENTDATE, REGIONID, TOTALDEMAND (the operational demand AEMO dispatches
             generation to meet), and RRP (the regional reference price for energy).
    """
    regional_data = fetch_data.get_region_data(start_date, end_date, raw_data_cache)
    regional_data = preprocessing.remove_number_from_region_names(
        "REGIONID", regional_data
    )
    regional_data["SETTLEMENTDATE"] = regional_data["SETTLEMENTDATE"].dt.strftime(
        "%Y-%m-%d %X"
    )
    return regional_data


def bid_data(start_date, end_date, raw_data_cache):
    """ Wrapper for fetching and preprocessing bid data.
    - Calls :py:function:fetch_data.get_volume_bids, :py:function:fetch_data.get_price_bids, and
      :py:function:fetch_data.get_duid_availability_data to get raw bidding and availabilty data
    - Volume and price bids are filtered to get only bids for the energy spot market
    - Volume and price bids are combined using :py:function:preprocessing.stack_unit_bids
    - Bids are filtered to remove those with zero volume.
    - The function :py:function:preprocessing.adjust_bids_for_availability to calculate the bid volume adjusted so that
      the bid volume does not exceed the unit availablity
    - Finally datetime columns are converted to string format.

    Used for preparing data to load into dashboard backend PostgresSQL database, or can be used for
    compiling regional data directly if the user does not have database, but compling data using this function will be
    considerably slower than from a PostgresSQL database.

    Examples:
    >>> bid_data(
    ... '2020/01/01 00:00:00',
    ... '2020/01/01 00:01:00',
    ... 'D:/nemosis_data_cache')

    Args:
       start_date: str formatted "DD/MM/YYYY HH:MM:SS", data with date times greater than start_date are returned
       end_date: str formatted identical to start_date, data with date times less than or equal to end_date are returned
       raw_data_cache: Filepath to directory for caching files downloaded from AEMO

    Returns: pandas dataframe with columns INTERVAL_DATETIME, DUID, BIDPRICE ($/MWh), BIDVOLUME (MW),
             BIDVOLUMEADJUSTED (MW)
    """
    volume_bids = fetch_data.get_volume_bids(start_date, end_date, raw_data_cache)
    volume_bids = volume_bids[volume_bids["BIDTYPE"] == "ENERGY"].drop(
        columns=["BIDTYPE"]
    )
    price_bids = fetch_data.get_price_bids(start_date, end_date, raw_data_cache)
    price_bids = price_bids[price_bids["BIDTYPE"] == "ENERGY"].drop(columns=["BIDTYPE"])
    availability = fetch_data.get_duid_availability_data(
        start_date, end_date, raw_data_cache
    )
    combined_bids = preprocessing.stack_unit_bids(volume_bids, price_bids)
    combined_bids = combined_bids[combined_bids["BIDVOLUME"] > 0.0].copy()
    combined_bids = preprocessing.adjust_bids_for_availability(
        combined_bids, availability
    )
    combined_bids["INTERVAL_DATETIME"] = combined_bids["INTERVAL_DATETIME"].dt.strftime(
        "%Y-%m-%d %X"
    )
    return combined_bids


def duid_info(raw_data_cache):
    """ Wrapper for fetching and preprocessing duid summary data.
    - Calls :py:function:fetch_data.get_duid_data to get raw duid info
    - Calls :py:function:preprocessing.hard_code_fix_fuel_source_and_tech_errors to replace NA values with '-'
    - Calls :py:function:preprocessing.remove_number_from_region_names to remove trailing '1' from REGIONIDs
    - Calls :py:function:preprocessing.tech_namer to determine a more concise and consistent technology type for
      units. This value is stored in the column "unit type".\

    Used for preparing data to load into dashboard backend PostgresSQL database, or can be used for
    compiling regional data directly if the user does not have database.

    Examples:
    >>> duid_info('D:/nemosis_data_cache')

    Args:
       raw_data_cache: Filepath to directory for caching files downloaded from AEMO

    Returns: pandas dataframe with columns DUID, REGIONID, "FUEL SOURCE - DESCRIPTOR", "DISPATCH TYPE",
             "TECHNOLOGY TYPE - DESCRIPTOR", "UNIT TYPE", "STATION NAME"
    """
    duid_info = fetch_data.get_duid_data(raw_data_cache)
    duid_info = preprocessing.hard_code_fix_fuel_source_and_tech_errors(duid_info)
    duid_info = preprocessing.remove_number_from_region_names("REGION", duid_info)
    duid_info = preprocessing.tech_namer(duid_info)
    return duid_info


def unit_dispatch(start_date, end_date, raw_data_cache):
    """ Wrapper for fetching and preprocessing unit dispatch data.
    - Calls :py:function:fetch_data.get_volume_bids and :py:function:fetch_data.get_duid_availability_data to get raw
      bidding and availabilty data
    - Volume are filtered to get only bids for the energy spot market
    - Calls :py:function:preprocessing.calculate_unit_time_series_metrics
    - Finally datetime columns are converted to string format.

    Used for preparing data to load into dashboard backend PostgresSQL database, or can be used for
    compiling regional data directly if the user does not have database, but compling data using this function will be
    considerably slower than from a PostgresSQL database.

    Examples:
    >>> unit_dispatch(
    ... '2020/01/01 00:00:00',
    ... '2020/01/01 00:01:00',
    ... 'D:/nemosis_data_cache')

    Args:
       start_date: str formatted "DD/MM/YYYY HH:MM:SS", data with date times greater than start_date are returned
       end_date: str formatted identical to start_date, data with date times less than or equal to end_date are returned
       raw_data_cache: Filepath to directory for caching files downloaded from AEMO

    Returns: pandas dataframe with columns INTERVAL_DATETIME, DUID, AVAILABILITY, TOTALCLEARED, FINALMW,
             ASBIDRAMPUPMAXAVAIL, ASBIDRAMPDOWNMINAVAIL, RAMPUPMAXAVAIL, RAMPDOWNMINAVAIL, PASAAVAILABILITY,
             MAXAVAIL (see unit_dispatch in Database Guide for column definitions)
    """
    as_bid_metrics = fetch_data.get_volume_bids(start_date, end_date, raw_data_cache)
    as_bid_metrics = as_bid_metrics[as_bid_metrics["BIDTYPE"] == "ENERGY"].drop(
        columns=["BIDTYPE"]
    )
    as_bid_metrics = as_bid_metrics.loc[
        :,
        [
            "INTERVAL_DATETIME",
            "DUID",
            "MAXAVAIL",
            "ROCUP",
            "ROCDOWN",
            "PASAAVAILABILITY",
        ],
    ]
    after_dispatch_metrics = fetch_data.get_duid_availability_data(
        start_date, end_date, raw_data_cache
    )
    unit_time_series_metrics = preprocessing.calculate_unit_time_series_metrics(
        as_bid_metrics, after_dispatch_metrics
    )
    unit_time_series_metrics["INTERVAL_DATETIME"] = unit_time_series_metrics[
        "INTERVAL_DATETIME"
    ].dt.strftime("%Y-%m-%d %X")
    return unit_time_series_metrics


def define_and_return_price_bins():
    """
    Defines the bins for aggregating bidding data.

    Examples:
    >>> define_and_return_price_bins()

    Returns: pandas dataframe with column bin_name, lower_edge and upper_edge
    """
    price_bins = pd.DataFrame(
        {
            "bin_name": [
                "[-1000, -100)",
                "[-100, 0)",
                "[0, 50)",
                "[50, 100)",
                "[100, 200)",
                "[200, 300)",
                "[300, 500)",
                "[500, 1000)",
                "[1000, 5000)",
                "[5000, 10000)",
                "[10000, 15500)",
            ],
            "lower_edge": [-2000, -100, 0, 50, 100, 200, 300, 500, 1000, 5000, 10000],
            "upper_edge": [-100, 0, 50, 100, 200, 300, 500, 1000, 5000, 10000, 16000],
        }
    )
    return price_bins
