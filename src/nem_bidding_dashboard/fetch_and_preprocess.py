import fetch_data
import pandas as pd
import preprocessing


def region_data(start_date, end_date, raw_data_cache):
    regional_data = fetch_data.get_region_data(start_date, end_date, raw_data_cache)
    regional_data = preprocessing.remove_number_from_region_names(
        "REGIONID", regional_data
    )
    regional_data["SETTLEMENTDATE"] = regional_data["SETTLEMENTDATE"].dt.strftime(
        "%Y-%m-%d %X"
    )
    return regional_data


def bid_table(start_date, end_date, raw_data_cache):
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


def duid_info_table(raw_data_cache):
    duid_info = fetch_data.get_duid_data(raw_data_cache)
    duid_info = preprocessing.hard_code_fix_fuel_source_and_tech_errors(duid_info)
    duid_info = preprocessing.remove_number_from_region_names("REGION", duid_info)
    duid_info = preprocessing.tech_namer(duid_info)
    return duid_info


def unit_dispatch(start_date, end_date, raw_data_cache):
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
