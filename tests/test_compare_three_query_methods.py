import pandas as pd
import pytest

from nem_bidding_dashboard import query_cached_data, query_supabase_db

test_cache = "D:/nemosis_data_cache"


@pytest.mark.parametrize(
    "regions", [["SA"], ["TAS", "QLD"], ["QLD", "NSW", "VIC", "SA", "TAS"]]
)
@pytest.mark.parametrize("time_pairs", [("2022/01/01 00:05:00", "2022/01/01 01:00:00")])
def test_region_demand(regions, time_pairs):
    region_demand_cache = query_cached_data.region_demand(
        regions, time_pairs[0], time_pairs[1], test_cache
    )
    region_demand_supabase = query_supabase_db.region_demand(
        regions,
        time_pairs[0],
        time_pairs[1],
    )
    region_demand_cache = region_demand_cache.sort_values(
        ["SETTLEMENTDATE"]
    ).reset_index(drop=True)
    region_demand_supabase = region_demand_supabase.sort_values(
        ["SETTLEMENTDATE"]
    ).reset_index(drop=True)
    pd.testing.assert_frame_equal(region_demand_cache, region_demand_supabase)
