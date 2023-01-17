import pandas as pd
import pytest
from defaults import (
    bid_types,
    column_names,
    dispatch_types,
    duid_combos,
    query_time_windows,
    region_combos,
    resolutions,
    tech_types_combos,
)

from nem_bidding_dashboard import query_cached_data, query_supabase_db

test_cache = "D:/nem_bidding_dashboard_cache"


@pytest.mark.parametrize("regions", region_combos)
@pytest.mark.parametrize("time_pair", query_time_windows)
def test_region_demand(regions, time_pair):
    region_demand_cache = query_cached_data.region_demand(
        regions, time_pair[0], time_pair[1], test_cache
    )
    region_demand_supabase = query_supabase_db.region_demand(
        regions,
        time_pair[0],
        time_pair[1],
    )
    pd.testing.assert_frame_equal(region_demand_cache, region_demand_supabase)


@pytest.mark.parametrize("regions", region_combos)
@pytest.mark.parametrize("time_pair", query_time_windows)
@pytest.mark.parametrize("resolution", resolutions)
@pytest.mark.parametrize("dispatch_type", dispatch_types)
@pytest.mark.parametrize("adjusted", bid_types)
@pytest.mark.parametrize("tech_types", tech_types_combos)
def test_aggregate_bids(
    regions, time_pair, resolution, dispatch_type, adjusted, tech_types
):
    aggregate_bids_cache = query_cached_data.aggregate_bids(
        regions,
        time_pair[0],
        time_pair[1],
        resolution,
        adjusted,
        tech_types,
        dispatch_type,
        test_cache,
    )
    aggregate_bids_supabase = query_supabase_db.aggregate_bids(
        regions,
        time_pair[0],
        time_pair[1],
        resolution,
        adjusted,
        tech_types,
        dispatch_type,
    )
    pd.testing.assert_frame_equal(aggregate_bids_cache, aggregate_bids_supabase)


@pytest.mark.parametrize("duids", duid_combos)
@pytest.mark.parametrize("time_pair", query_time_windows)
@pytest.mark.parametrize("resolution", resolutions)
@pytest.mark.parametrize("adjusted", bid_types)
def test_duid_bids(
    duids,
    time_pair,
    resolution,
    adjusted,
):
    duid_bids_cache = query_cached_data.duid_bids(
        duids, time_pair[0], time_pair[1], resolution, adjusted, test_cache
    )
    duid_bids_supabase = query_supabase_db.duid_bids(
        duids,
        time_pair[0],
        time_pair[1],
        resolution,
        adjusted,
    )
    duid_bids_supabase["BIDVOLUME"] = duid_bids_supabase["BIDVOLUME"].astype(float)
    pd.testing.assert_frame_equal(duid_bids_cache, duid_bids_supabase)


@pytest.mark.parametrize("regions", region_combos)
@pytest.mark.parametrize("time_pair", query_time_windows)
@pytest.mark.parametrize("dispatch_type", dispatch_types)
@pytest.mark.parametrize("tech_types", tech_types_combos)
def test_stations_and_duids_in_regions_and_time_window(
    regions, time_pair, dispatch_type, tech_types
):
    stations_and_duids_in_regions_and_time_window_cache = (
        query_cached_data.stations_and_duids_in_regions_and_time_window(
            regions, time_pair[0], time_pair[1], dispatch_type, tech_types, test_cache
        )
    )
    stations_and_duids_in_regions_and_time_window_supabase = (
        query_supabase_db.stations_and_duids_in_regions_and_time_window(
            regions, time_pair[0], time_pair[1], dispatch_type, tech_types
        )
    )
    pd.testing.assert_frame_equal(
        stations_and_duids_in_regions_and_time_window_cache,
        stations_and_duids_in_regions_and_time_window_supabase,
    )


@pytest.mark.parametrize("column_name", column_names)
@pytest.mark.parametrize("regions", region_combos)
@pytest.mark.parametrize("time_pair", query_time_windows)
@pytest.mark.parametrize("resolution", resolutions)
@pytest.mark.parametrize("dispatch_type", dispatch_types)
@pytest.mark.parametrize("tech_types", tech_types_combos)
def test_get_aggregated_dispatch_data(
    column_name, regions, time_pair, resolution, dispatch_type, tech_types
):
    aggregated_dispatch_cache = query_cached_data.get_aggregated_dispatch_data(
        column_name,
        regions,
        time_pair[0],
        time_pair[1],
        resolution,
        dispatch_type,
        tech_types,
        test_cache,
    )
    aggregated_dispatch_supabase = query_supabase_db.get_aggregated_dispatch_data(
        column_name,
        regions,
        time_pair[0],
        time_pair[1],
        resolution,
        dispatch_type,
        tech_types,
    )
    pd.testing.assert_frame_equal(
        aggregated_dispatch_cache, aggregated_dispatch_supabase
    )


@pytest.mark.parametrize("column_name", column_names)
@pytest.mark.parametrize("duids", duid_combos)
@pytest.mark.parametrize("time_pair", query_time_windows)
@pytest.mark.parametrize("resolution", resolutions)
def test_get_aggregated_dispatch_data_by_duids(
    column_name, duids, time_pair, resolution
):
    aggregated_dispatch_cache = query_cached_data.get_aggregated_dispatch_data_by_duids(
        column_name, duids, time_pair[0], time_pair[1], resolution, test_cache
    )
    aggregated_dispatch_supabase = (
        query_supabase_db.get_aggregated_dispatch_data_by_duids(
            column_name, duids, time_pair[0], time_pair[1], resolution
        )
    )
    aggregated_dispatch_supabase["COLUMNVALUES"] = aggregated_dispatch_supabase[
        "COLUMNVALUES"
    ].astype(float)
    pd.testing.assert_frame_equal(
        aggregated_dispatch_cache, aggregated_dispatch_supabase
    )


@pytest.mark.parametrize("regions", region_combos)
@pytest.mark.parametrize("time_pair", query_time_windows)
def test_get_aggregated_vwap(regions, time_pair):
    aggregated_vwap_cache = query_cached_data.get_aggregated_vwap(
        regions, time_pair[0], time_pair[1], test_cache
    )
    aggregated_vwap_supabase = query_supabase_db.get_aggregated_vwap(
        regions,
        time_pair[0],
        time_pair[1],
    )
    pd.testing.assert_frame_equal(aggregated_vwap_cache, aggregated_vwap_supabase)


@pytest.mark.parametrize("regions", region_combos)
@pytest.mark.parametrize("dispatch_type", dispatch_types)
def test_unit_types(regions, dispatch_type):
    unit_types_cache = query_cached_data.unit_types(test_cache, dispatch_type, regions)
    unit_types_supabase = query_supabase_db.unit_types(
        dispatch_type,
        regions,
    )
    pd.testing.assert_frame_equal(unit_types_cache, unit_types_supabase)
