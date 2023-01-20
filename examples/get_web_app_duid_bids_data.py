import pandas as pd

from nem_bidding_dashboard import query_cached_data

raw_data_cache = (
    "D:/nemosis_data_cache"  # Edit this to lead to the raw data cache on your computer.
)
start_time = "2022/01/01 00:00:00"
end_time = "2022/01/02 00:00:00"
regions = ["QLD", "NSW", "VIC", "SA", "TAS"]
duids = ["BERYLSF1"]  # This is the set of DUIDs bids will be downloaded for.
resolution = "hourly"  # This variable can be 'hourly' or '5-min'
adjusted = "adjusted"  # This variable can be 'adjusted' or 'raw'
dispatch_data_column = "AVAILABILITY"

agg_bids = query_cached_data.duid_bids(
    raw_data_cache, start_time, end_time, duids, resolution, adjusted
)

agg_bids.to_csv("duids_bids.csv")

dispatch_data = query_cached_data.get_aggregated_dispatch_data_by_duids(
    raw_data_cache, dispatch_data_column, start_time, end_time, duids, resolution
)

dispatch_data = dispatch_data.rename(
    columns={
        "INTERVAL_DATETIME": "SETTLEMENTDATE",
        "COLUMNVALUES": dispatch_data_column,
    }
)

region_demand = query_cached_data.region_demand(
    raw_data_cache, start_time, end_time, regions
)

aggregated_vwap = query_cached_data.get_aggregated_vwap(
    raw_data_cache, start_time, end_time, regions
)

dispatch_data = pd.merge(dispatch_data, region_demand, on="SETTLEMENTDATE")
dispatch_data = pd.merge(dispatch_data, aggregated_vwap, on="SETTLEMENTDATE")
dispatch_data.to_csv("dispatch_data.csv")
