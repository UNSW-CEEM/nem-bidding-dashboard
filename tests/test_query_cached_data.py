import pandas as pd
from mock_nemosis import dynamic_data_compiler, static_table

from nem_bidding_dashboard import query_cached_data


def test_region_demand_filter_regions(monkeypatch):
    monkeypatch.setattr(
        "nem_bidding_dashboard.fetch_data.dynamic_data_compiler", dynamic_data_compiler
    )
    region_demand = query_cached_data.region_demand(
        "tests/nemosis_dummy_cache",
        "2020/01/01 00:00:00",
        "2020/01/01 06:00:00",
        ["SA"],
    )
    expected_region_demand = pd.DataFrame(
        columns=["SETTLEMENTDATE", "TOTALDEMAND"],
        data=[("2020/01/01 01:00:00", 1000.0), ("2020/01/01 05:00:00", 1010.0)],
    )
    expected_region_demand["SETTLEMENTDATE"] = pd.to_datetime(
        expected_region_demand["SETTLEMENTDATE"]
    )
    expected_region_demand["SETTLEMENTDATE"] = expected_region_demand[
        "SETTLEMENTDATE"
    ].dt.strftime("%Y-%m-%d %X")
    region_demand = region_demand.sort_values(["SETTLEMENTDATE"]).reset_index(drop=True)
    pd.testing.assert_frame_equal(region_demand, expected_region_demand)


def test_region_demand(monkeypatch):
    monkeypatch.setattr(
        "nem_bidding_dashboard.fetch_data.dynamic_data_compiler", dynamic_data_compiler
    )
    region_demand = query_cached_data.region_demand(
        "tests/nemosis_dummy_cache",
        "2020/01/01 00:00:00",
        "2020/01/01 06:00:00",
        ["SA", "TAS"],
    )
    expected_region_demand = pd.DataFrame(
        columns=["SETTLEMENTDATE", "TOTALDEMAND"],
        data=[("2020/01/01 01:00:00", 3000.0), ("2020/01/01 05:00:00", 3020.0)],
    )
    expected_region_demand["SETTLEMENTDATE"] = pd.to_datetime(
        expected_region_demand["SETTLEMENTDATE"]
    )
    expected_region_demand["SETTLEMENTDATE"] = expected_region_demand[
        "SETTLEMENTDATE"
    ].dt.strftime("%Y-%m-%d %X")
    region_demand = region_demand.sort_values(["SETTLEMENTDATE"]).reset_index(drop=True)
    pd.testing.assert_frame_equal(region_demand, expected_region_demand)


def test_aggregate_bids_generator(monkeypatch):
    monkeypatch.setattr(
        "nem_bidding_dashboard.fetch_data.dynamic_data_compiler", dynamic_data_compiler
    )
    monkeypatch.setattr("nem_bidding_dashboard.fetch_data.static_table", static_table)
    bids = query_cached_data.aggregate_bids(
        raw_data_cache="tests/nemosis_dummy_cache",
        start_time="2020/01/01 00:00:00",
        end_time="2020/01/01 05:00:00",
        regions=["SA", "TAS"],
        dispatch_type="Generator",
        tech_types=[],
        resolution="hourly",
        adjusted="adjusted",
    )
    bids_expected = pd.DataFrame(
        columns=["INTERVAL_DATETIME", "BIN_NAME", "BIDVOLUME"],
        data=[
            ("2020/01/01 01:00:00", "[0, 50)", 110.0),
            ("2020/01/01 05:00:00", "[0, 50)", 203.0),
        ],
    )
    bids_expected["INTERVAL_DATETIME"] = pd.to_datetime(
        bids_expected["INTERVAL_DATETIME"]
    )
    bids_expected["INTERVAL_DATETIME"] = bids_expected["INTERVAL_DATETIME"].dt.strftime(
        "%Y-%m-%d %X"
    )
    bids = bids.sort_values(["INTERVAL_DATETIME"]).reset_index(drop=True)
    pd.testing.assert_frame_equal(bids, bids_expected)


def test_aggregate_bids_generator_not_adjusted(monkeypatch):
    monkeypatch.setattr(
        "nem_bidding_dashboard.fetch_data.dynamic_data_compiler", dynamic_data_compiler
    )
    monkeypatch.setattr("nem_bidding_dashboard.fetch_data.static_table", static_table)
    bids = query_cached_data.aggregate_bids(
        raw_data_cache="tests/nemosis_dummy_cache",
        start_time="2020/01/01 00:00:00",
        end_time="2020/01/01 05:00:00",
        regions=["SA", "TAS"],
        dispatch_type="Generator",
        tech_types=[],
        resolution="hourly",
        adjusted="raw",
    )
    bids_expected = pd.DataFrame(
        columns=["INTERVAL_DATETIME", "BIN_NAME", "BIDVOLUME"],
        data=[
            ("2020/01/01 01:00:00", "[0, 50)", 110.0),
            ("2020/01/01 05:00:00", "[0, 50)", 210.0),
        ],
    )
    bids_expected["INTERVAL_DATETIME"] = pd.to_datetime(
        bids_expected["INTERVAL_DATETIME"]
    )
    bids_expected["INTERVAL_DATETIME"] = bids_expected["INTERVAL_DATETIME"].dt.strftime(
        "%Y-%m-%d %X"
    )
    bids = bids.sort_values(["INTERVAL_DATETIME"]).reset_index(drop=True)
    pd.testing.assert_frame_equal(bids, bids_expected)


def test_aggregate_bids_load(monkeypatch):
    monkeypatch.setattr(
        "nem_bidding_dashboard.fetch_data.dynamic_data_compiler", dynamic_data_compiler
    )
    monkeypatch.setattr("nem_bidding_dashboard.fetch_data.static_table", static_table)
    bids = query_cached_data.aggregate_bids(
        raw_data_cache="tests/nemosis_dummy_cache",
        start_time="2020/01/01 00:00:00",
        end_time="2020/01/01 05:00:00",
        regions=["SA", "TAS"],
        dispatch_type="Load",
        tech_types=[],
        resolution="hourly",
        adjusted="adjusted",
    )
    bids_expected = pd.DataFrame(
        columns=["INTERVAL_DATETIME", "BIN_NAME", "BIDVOLUME"],
        data=[
            ("2020/01/01 01:00:00", "[0, 50)", 101.0),
            ("2020/01/01 05:00:00", "[0, 50)", 82.0),
        ],
    )
    bids_expected["INTERVAL_DATETIME"] = pd.to_datetime(
        bids_expected["INTERVAL_DATETIME"]
    )
    bids_expected["INTERVAL_DATETIME"] = bids_expected["INTERVAL_DATETIME"].dt.strftime(
        "%Y-%m-%d %X"
    )
    bids = bids.sort_values(["INTERVAL_DATETIME"]).reset_index(drop=True)
    pd.testing.assert_frame_equal(bids, bids_expected)


def test_duid_bids(monkeypatch):
    monkeypatch.setattr(
        "nem_bidding_dashboard.fetch_data.dynamic_data_compiler", dynamic_data_compiler
    )
    bids = query_cached_data.duid_bids(
        raw_data_cache="tests/nemosis_dummy_cache",
        start_time="2020/01/01 00:00:00",
        end_time="2020/01/01 05:00:00",
        duids=["A", "B"],
        resolution="hourly",
        adjusted="adjusted",
    )
    bids_expected = pd.DataFrame(
        columns=["INTERVAL_DATETIME", "DUID", "BIDBAND", "BIDVOLUME", "BIDPRICE"],
        data=[
            ("2020/01/01 01:00:00", "A", 4, 70.0, 4.1),
            ("2020/01/01 01:00:00", "A", 10, 31.0, 10.1),
            ("2020/01/01 01:00:00", "B", 5, 100.0, 5.3),
            ("2020/01/01 01:00:00", "B", 9, 10.0, 9.3),
            ("2020/01/01 05:00:00", "A", 3, 70.0, 3.2),
            ("2020/01/01 05:00:00", "A", 9, 12.0, 9.2),
            ("2020/01/01 05:00:00", "B", 4, 100.0, 4.4),
            ("2020/01/01 05:00:00", "B", 8, 100.0, 8.4),
            ("2020/01/01 05:00:00", "B", 9, 3.0, 9.4),
        ],
    )
    bids_expected["INTERVAL_DATETIME"] = pd.to_datetime(
        bids_expected["INTERVAL_DATETIME"]
    )
    bids_expected["INTERVAL_DATETIME"] = bids_expected["INTERVAL_DATETIME"].dt.strftime(
        "%Y-%m-%d %X"
    )
    bids = bids.sort_values(["INTERVAL_DATETIME", "DUID", "BIDBAND"]).reset_index(
        drop=True
    )
    pd.testing.assert_frame_equal(bids, bids_expected)


def test_duid_bids_raw(monkeypatch):
    monkeypatch.setattr(
        "nem_bidding_dashboard.fetch_data.dynamic_data_compiler", dynamic_data_compiler
    )
    bids = query_cached_data.duid_bids(
        raw_data_cache="tests/nemosis_dummy_cache",
        start_time="2020/01/01 00:00:00",
        end_time="2020/01/01 05:00:00",
        duids=["A", "B"],
        resolution="hourly",
        adjusted="raw",
    )
    bids_expected = pd.DataFrame(
        columns=["INTERVAL_DATETIME", "DUID", "BIDBAND", "BIDVOLUME", "BIDPRICE"],
        data=[
            ("2020/01/01 01:00:00", "A", 4, 70.0, 4.1),
            ("2020/01/01 01:00:00", "A", 10, 40.0, 10.1),
            ("2020/01/01 01:00:00", "B", 5, 100.0, 5.3),
            ("2020/01/01 01:00:00", "B", 9, 10.0, 9.3),
            ("2020/01/01 05:00:00", "A", 3, 70.0, 3.2),
            ("2020/01/01 05:00:00", "A", 9, 12.0, 9.2),
            ("2020/01/01 05:00:00", "B", 4, 100.0, 4.4),
            ("2020/01/01 05:00:00", "B", 8, 100.0, 8.4),
            ("2020/01/01 05:00:00", "B", 9, 10.0, 9.4),
        ],
    )
    bids_expected["INTERVAL_DATETIME"] = pd.to_datetime(
        bids_expected["INTERVAL_DATETIME"]
    )
    bids_expected["INTERVAL_DATETIME"] = bids_expected["INTERVAL_DATETIME"].dt.strftime(
        "%Y-%m-%d %X"
    )
    bids = bids.sort_values(["INTERVAL_DATETIME", "DUID", "BIDBAND"]).reset_index(
        drop=True
    )
    pd.testing.assert_frame_equal(bids, bids_expected)


def stations_and_duids_in_regions_and_time_window_load(monkeypatch):
    monkeypatch.setattr(
        "nem_bidding_dashboard.fetch_data.dynamic_data_compiler", dynamic_data_compiler
    )
    stations_and_duids = (
        query_cached_data.stations_and_duids_in_regions_and_time_window(
            raw_data_cache="tests/nemosis_dummy_cache",
            start_time="2020/01/01 00:00:00",
            end_time="2020/01/01 05:00:00",
            regions=["SA", "TAS"],
            dispatch_type="Load",
            tech_types=[],
        )
    )
    stations_and_duids_expected = pd.DataFrame(
        columns=["DUID", "STATION NAME"], data=[("A", "Adelaide Desalination Plant")]
    )
    stations_and_duids = stations_and_duids.sort_values(["DUID"]).reset_index(drop=True)
    pd.testing.assert_frame_equal(stations_and_duids, stations_and_duids_expected)


def stations_and_duids_in_regions_and_time_window_generator(monkeypatch):
    monkeypatch.setattr(
        "nem_bidding_dashboard.fetch_data.dynamic_data_compiler", dynamic_data_compiler
    )
    stations_and_duids = (
        query_cached_data.stations_and_duids_in_regions_and_time_window(
            raw_data_cache="tests/nemosis_dummy_cache",
            start_time="2020/01/01 00:00:00",
            end_time="2020/01/01 05:00:00",
            regions=["SA", "TAS"],
            dispatch_type="Generator",
            tech_types=[],
        )
    )
    stations_and_duids_expected = pd.DataFrame(
        columns=["DUID", "STATION NAME"], data=[("A", "Ararat Wind Farm")]
    )
    stations_and_duids = stations_and_duids.sort_values(["DUID"]).reset_index(drop=True)
    pd.testing.assert_frame_equal(stations_and_duids, stations_and_duids_expected)


def test_get_aggregated_dispatch_data_as_bid_ramp_up_max_avail(monkeypatch):
    monkeypatch.setattr(
        "nem_bidding_dashboard.fetch_data.dynamic_data_compiler", dynamic_data_compiler
    )
    monkeypatch.setattr("nem_bidding_dashboard.fetch_data.static_table", static_table)
    column_values = query_cached_data.get_aggregated_dispatch_data(
        raw_data_cache="tests/nemosis_dummy_cache",
        column_name="ASBIDRAMPUPMAXAVAIL",
        start_time="2020/01/01 00:00:00",
        end_time="2020/01/01 05:00:00",
        regions=["SA", "TAS"],
        dispatch_type="Load",
        tech_types=[],
        resolution="hourly",
    )
    expected_column_values = pd.DataFrame(
        columns=["INTERVAL_DATETIME", "COLUMNVALUES"],
        data=[("2020/01/01 01:00:00", 101.0), ("2020/01/01 05:00:00", 72.0 + 1 * 5)],
    )
    expected_column_values["INTERVAL_DATETIME"] = pd.to_datetime(
        expected_column_values["INTERVAL_DATETIME"]
    )
    expected_column_values["INTERVAL_DATETIME"] = expected_column_values[
        "INTERVAL_DATETIME"
    ].dt.strftime("%Y-%m-%d %X")
    column_values = column_values.sort_values(["INTERVAL_DATETIME"]).reset_index(
        drop=True
    )
    pd.testing.assert_frame_equal(column_values, expected_column_values)


def test_get_aggregated_dispatch_data_as_bid_ramp_down_min_avail(monkeypatch):
    monkeypatch.setattr(
        "nem_bidding_dashboard.fetch_data.dynamic_data_compiler", dynamic_data_compiler
    )
    monkeypatch.setattr("nem_bidding_dashboard.fetch_data.static_table", static_table)
    column_values = query_cached_data.get_aggregated_dispatch_data(
        raw_data_cache="tests/nemosis_dummy_cache",
        column_name="ASBIDRAMPDOWNMINAVAIL",
        start_time="2020/01/01 00:00:00",
        end_time="2020/01/01 05:00:00",
        regions=["SA", "TAS"],
        dispatch_type="Load",
        tech_types=[],
        resolution="hourly",
    )
    expected_column_values = pd.DataFrame(
        columns=["INTERVAL_DATETIME", "COLUMNVALUES"],
        data=[("2020/01/01 01:00:00", 71.0 - 1 * 5), ("2020/01/01 05:00:00", 0.0)],
    )
    expected_column_values["INTERVAL_DATETIME"] = pd.to_datetime(
        expected_column_values["INTERVAL_DATETIME"]
    )
    expected_column_values["INTERVAL_DATETIME"] = expected_column_values[
        "INTERVAL_DATETIME"
    ].dt.strftime("%Y-%m-%d %X")
    column_values = column_values.sort_values(["INTERVAL_DATETIME"]).reset_index(
        drop=True
    )
    pd.testing.assert_frame_equal(column_values, expected_column_values)


def test_get_aggregated_dispatch_data_ramp_up_max_avail(monkeypatch):
    monkeypatch.setattr(
        "nem_bidding_dashboard.fetch_data.dynamic_data_compiler", dynamic_data_compiler
    )
    monkeypatch.setattr("nem_bidding_dashboard.fetch_data.static_table", static_table)
    column_values = query_cached_data.get_aggregated_dispatch_data(
        raw_data_cache="tests/nemosis_dummy_cache",
        column_name="RAMPUPMAXAVAIL",
        start_time="2020/01/01 00:00:00",
        end_time="2020/01/01 05:00:00",
        regions=["SA", "TAS"],
        dispatch_type="Load",
        tech_types=[],
        resolution="hourly",
    )
    expected_column_values = pd.DataFrame(
        columns=["INTERVAL_DATETIME", "COLUMNVALUES"],
        data=[
            ("2020/01/01 01:00:00", 101.0),
            ("2020/01/01 05:00:00", 72.0 + 246.0 / 12),
        ],
    )
    expected_column_values["INTERVAL_DATETIME"] = pd.to_datetime(
        expected_column_values["INTERVAL_DATETIME"]
    )
    expected_column_values["INTERVAL_DATETIME"] = expected_column_values[
        "INTERVAL_DATETIME"
    ].dt.strftime("%Y-%m-%d %X")
    column_values = column_values.sort_values(["INTERVAL_DATETIME"]).reset_index(
        drop=True
    )
    pd.testing.assert_frame_equal(column_values, expected_column_values)


def test_get_aggregated_dispatch_data_ramp_down_min_avail(monkeypatch):
    monkeypatch.setattr(
        "nem_bidding_dashboard.fetch_data.dynamic_data_compiler", dynamic_data_compiler
    )
    monkeypatch.setattr("nem_bidding_dashboard.fetch_data.static_table", static_table)
    column_values = query_cached_data.get_aggregated_dispatch_data(
        raw_data_cache="tests/nemosis_dummy_cache",
        column_name="RAMPDOWNMINAVAIL",
        start_time="2020/01/01 00:00:00",
        end_time="2020/01/01 05:00:00",
        regions=["SA", "TAS"],
        dispatch_type="Load",
        tech_types=[],
        resolution="hourly",
    )
    expected_column_values = pd.DataFrame(
        columns=["INTERVAL_DATETIME", "COLUMNVALUES"],
        data=[("2020/01/01 01:00:00", 71.0 - 122.0 / 12), ("2020/01/01 05:00:00", 0.0)],
    )
    expected_column_values["INTERVAL_DATETIME"] = pd.to_datetime(
        expected_column_values["INTERVAL_DATETIME"]
    )
    expected_column_values["INTERVAL_DATETIME"] = expected_column_values[
        "INTERVAL_DATETIME"
    ].dt.strftime("%Y-%m-%d %X")
    column_values = column_values.sort_values(["INTERVAL_DATETIME"]).reset_index(
        drop=True
    )
    pd.testing.assert_frame_equal(column_values, expected_column_values)


def test_get_aggregated_dispatch_data_as_bid_ramp_up_max_avail_generator(monkeypatch):
    monkeypatch.setattr(
        "nem_bidding_dashboard.fetch_data.dynamic_data_compiler", dynamic_data_compiler
    )
    monkeypatch.setattr("nem_bidding_dashboard.fetch_data.static_table", static_table)
    column_values = query_cached_data.get_aggregated_dispatch_data(
        raw_data_cache="tests/nemosis_dummy_cache",
        column_name="ASBIDRAMPUPMAXAVAIL",
        start_time="2020/01/01 00:00:00",
        end_time="2020/01/01 05:00:00",
        regions=["SA", "TAS"],
        dispatch_type="Generator",
        tech_types=[],
        resolution="hourly",
    )
    expected_column_values = pd.DataFrame(
        columns=["INTERVAL_DATETIME", "COLUMNVALUES"],
        data=[("2020/01/01 01:00:00", 61.0 + 2 * 5), ("2020/01/01 05:00:00", 104.0)],
    )
    expected_column_values["INTERVAL_DATETIME"] = pd.to_datetime(
        expected_column_values["INTERVAL_DATETIME"]
    )
    expected_column_values["INTERVAL_DATETIME"] = expected_column_values[
        "INTERVAL_DATETIME"
    ].dt.strftime("%Y-%m-%d %X")
    column_values = column_values.sort_values(["INTERVAL_DATETIME"]).reset_index(
        drop=True
    )
    pd.testing.assert_frame_equal(column_values, expected_column_values)


def test_get_aggregated_dispatch_data_as_bid_ramp_down_min_avail_generator(monkeypatch):
    monkeypatch.setattr(
        "nem_bidding_dashboard.fetch_data.dynamic_data_compiler", dynamic_data_compiler
    )
    monkeypatch.setattr("nem_bidding_dashboard.fetch_data.static_table", static_table)
    column_values = query_cached_data.get_aggregated_dispatch_data(
        raw_data_cache="tests/nemosis_dummy_cache",
        column_name="ASBIDRAMPDOWNMINAVAIL",
        start_time="2020/01/01 00:00:00",
        end_time="2020/01/01 05:00:00",
        regions=["SA", "TAS"],
        dispatch_type="Generator",
        tech_types=[],
        resolution="hourly",
    )
    expected_column_values = pd.DataFrame(
        columns=["INTERVAL_DATETIME", "COLUMNVALUES"],
        data=[("2020/01/01 01:00:00", 0.0), ("2020/01/01 05:00:00", 0.0)],
    )
    expected_column_values["INTERVAL_DATETIME"] = pd.to_datetime(
        expected_column_values["INTERVAL_DATETIME"]
    )
    expected_column_values["INTERVAL_DATETIME"] = expected_column_values[
        "INTERVAL_DATETIME"
    ].dt.strftime("%Y-%m-%d %X")
    column_values = column_values.sort_values(["INTERVAL_DATETIME"]).reset_index(
        drop=True
    )
    pd.testing.assert_frame_equal(column_values, expected_column_values)


def test_get_aggregated_dispatch_data_ramp_up_max_avail_generator(monkeypatch):
    monkeypatch.setattr(
        "nem_bidding_dashboard.fetch_data.dynamic_data_compiler", dynamic_data_compiler
    )
    monkeypatch.setattr("nem_bidding_dashboard.fetch_data.static_table", static_table)
    column_values = query_cached_data.get_aggregated_dispatch_data(
        raw_data_cache="tests/nemosis_dummy_cache",
        column_name="RAMPUPMAXAVAIL",
        start_time="2020/01/01 00:00:00",
        end_time="2020/01/01 05:00:00",
        regions=["SA", "TAS"],
        dispatch_type="Generator",
        tech_types=[],
        resolution="hourly",
    )
    expected_column_values = pd.DataFrame(
        columns=["INTERVAL_DATETIME", "COLUMNVALUES"],
        data=[
            ("2020/01/01 01:00:00", 61.0 + 128.0 / 12),
            ("2020/01/01 05:00:00", 62.0 + 248.0 / 12),
        ],
    )
    expected_column_values["INTERVAL_DATETIME"] = pd.to_datetime(
        expected_column_values["INTERVAL_DATETIME"]
    )
    expected_column_values["INTERVAL_DATETIME"] = expected_column_values[
        "INTERVAL_DATETIME"
    ].dt.strftime("%Y-%m-%d %X")
    column_values = column_values.sort_values(["INTERVAL_DATETIME"]).reset_index(
        drop=True
    )
    pd.testing.assert_frame_equal(column_values, expected_column_values)


def test_get_aggregated_dispatch_data_ramp_down_min_avail_generator(monkeypatch):
    monkeypatch.setattr(
        "nem_bidding_dashboard.fetch_data.dynamic_data_compiler", dynamic_data_compiler
    )
    monkeypatch.setattr("nem_bidding_dashboard.fetch_data.static_table", static_table)
    column_values = query_cached_data.get_aggregated_dispatch_data(
        raw_data_cache="tests/nemosis_dummy_cache",
        column_name="RAMPDOWNMINAVAIL",
        start_time="2020/01/01 00:00:00",
        end_time="2020/01/01 05:00:00",
        regions=["SA", "TAS"],
        dispatch_type="Generator",
        tech_types=[],
        resolution="hourly",
    )
    expected_column_values = pd.DataFrame(
        columns=["INTERVAL_DATETIME", "COLUMNVALUES"],
        data=[
            ("2020/01/01 01:00:00", 61.0 - 124.0 / 12),
            ("2020/01/01 05:00:00", 62.0 - 244.0 / 12),
        ],
    )
    expected_column_values["INTERVAL_DATETIME"] = pd.to_datetime(
        expected_column_values["INTERVAL_DATETIME"]
    )
    expected_column_values["INTERVAL_DATETIME"] = expected_column_values[
        "INTERVAL_DATETIME"
    ].dt.strftime("%Y-%m-%d %X")
    column_values = column_values.sort_values(["INTERVAL_DATETIME"]).reset_index(
        drop=True
    )
    pd.testing.assert_frame_equal(column_values, expected_column_values)


def test_get_aggregated_dispatch_data_by_duids_as_bid_ramp_up_max_avail(monkeypatch):
    monkeypatch.setattr(
        "nem_bidding_dashboard.fetch_data.dynamic_data_compiler", dynamic_data_compiler
    )
    column_values = query_cached_data.get_aggregated_dispatch_data_by_duids(
        raw_data_cache="tests/nemosis_dummy_cache",
        column_name="ASBIDRAMPUPMAXAVAIL",
        start_time="2020/01/01 00:00:00",
        end_time="2020/01/01 05:00:00",
        duids=["B"],
        resolution="hourly",
    )
    expected_column_values = pd.DataFrame(
        columns=["INTERVAL_DATETIME", "COLUMNVALUES"],
        data=[("2020/01/01 01:00:00", 61.0 + 2 * 5), ("2020/01/01 05:00:00", 104.0)],
    )
    expected_column_values["INTERVAL_DATETIME"] = pd.to_datetime(
        expected_column_values["INTERVAL_DATETIME"]
    )
    expected_column_values["INTERVAL_DATETIME"] = expected_column_values[
        "INTERVAL_DATETIME"
    ].dt.strftime("%Y-%m-%d %X")
    column_values = column_values.sort_values(["INTERVAL_DATETIME"]).reset_index(
        drop=True
    )
    pd.testing.assert_frame_equal(column_values, expected_column_values)


def test_get_aggregated_dispatch_data_by_duids_as_bid_ramp_down_min_avail(monkeypatch):
    monkeypatch.setattr(
        "nem_bidding_dashboard.fetch_data.dynamic_data_compiler", dynamic_data_compiler
    )
    column_values = query_cached_data.get_aggregated_dispatch_data_by_duids(
        raw_data_cache="tests/nemosis_dummy_cache",
        column_name="ASBIDRAMPDOWNMINAVAIL",
        start_time="2020/01/01 00:00:00",
        end_time="2020/01/01 05:00:00",
        duids=["B"],
        resolution="hourly",
    )
    expected_column_values = pd.DataFrame(
        columns=["INTERVAL_DATETIME", "COLUMNVALUES"],
        data=[("2020/01/01 01:00:00", 0.0), ("2020/01/01 05:00:00", 0.0)],
    )
    expected_column_values["INTERVAL_DATETIME"] = pd.to_datetime(
        expected_column_values["INTERVAL_DATETIME"]
    )
    expected_column_values["INTERVAL_DATETIME"] = expected_column_values[
        "INTERVAL_DATETIME"
    ].dt.strftime("%Y-%m-%d %X")
    column_values = column_values.sort_values(["INTERVAL_DATETIME"]).reset_index(
        drop=True
    )
    pd.testing.assert_frame_equal(column_values, expected_column_values)


def test_get_aggregated_dispatch_data_by_duids_ramp_up_max_avail(monkeypatch):
    monkeypatch.setattr(
        "nem_bidding_dashboard.fetch_data.dynamic_data_compiler", dynamic_data_compiler
    )
    column_values = query_cached_data.get_aggregated_dispatch_data_by_duids(
        raw_data_cache="tests/nemosis_dummy_cache",
        column_name="RAMPUPMAXAVAIL",
        start_time="2020/01/01 00:00:00",
        end_time="2020/01/01 05:00:00",
        duids=["B"],
        resolution="hourly",
    )
    expected_column_values = pd.DataFrame(
        columns=["INTERVAL_DATETIME", "COLUMNVALUES"],
        data=[
            ("2020/01/01 01:00:00", 61.0 + 128.0 / 12),
            ("2020/01/01 05:00:00", 62.0 + 248.0 / 12),
        ],
    )
    expected_column_values["INTERVAL_DATETIME"] = pd.to_datetime(
        expected_column_values["INTERVAL_DATETIME"]
    )
    expected_column_values["INTERVAL_DATETIME"] = expected_column_values[
        "INTERVAL_DATETIME"
    ].dt.strftime("%Y-%m-%d %X")
    column_values = column_values.sort_values(["INTERVAL_DATETIME"]).reset_index(
        drop=True
    )
    pd.testing.assert_frame_equal(column_values, expected_column_values)


def test_get_aggregated_dispatch_data_by_duids_ramp_down_min_avail(monkeypatch):
    monkeypatch.setattr(
        "nem_bidding_dashboard.fetch_data.dynamic_data_compiler", dynamic_data_compiler
    )
    column_values = query_cached_data.get_aggregated_dispatch_data_by_duids(
        raw_data_cache="tests/nemosis_dummy_cache",
        column_name="RAMPDOWNMINAVAIL",
        start_time="2020/01/01 00:00:00",
        end_time="2020/01/01 05:00:00",
        duids=["B"],
        resolution="hourly",
    )
    expected_column_values = pd.DataFrame(
        columns=["INTERVAL_DATETIME", "COLUMNVALUES"],
        data=[
            ("2020/01/01 01:00:00", 61.0 - 124.0 / 12),
            ("2020/01/01 05:00:00", 62.0 - 244.0 / 12),
        ],
    )
    expected_column_values["INTERVAL_DATETIME"] = pd.to_datetime(
        expected_column_values["INTERVAL_DATETIME"]
    )
    expected_column_values["INTERVAL_DATETIME"] = expected_column_values[
        "INTERVAL_DATETIME"
    ].dt.strftime("%Y-%m-%d %X")
    column_values = column_values.sort_values(["INTERVAL_DATETIME"]).reset_index(
        drop=True
    )
    pd.testing.assert_frame_equal(column_values, expected_column_values)


def test_get_aggregated_vwap(monkeypatch):
    monkeypatch.setattr(
        "nem_bidding_dashboard.fetch_data.dynamic_data_compiler", dynamic_data_compiler
    )
    region_demand = query_cached_data.get_aggregated_vwap(
        "tests/nemosis_dummy_cache",
        "2020/01/01 00:00:00",
        "2020/01/01 06:00:00",
        ["SA", "TAS"],
    )
    expected_region_demand = pd.DataFrame(
        columns=["SETTLEMENTDATE", "PRICE"],
        data=[
            ("2020/01/01 01:00:00", (1000.0 * 55.4 + 2000.0 * 75.4) / 3000.0),
            ("2020/01/01 05:00:00", (1010.0 * 80.1 + 2010.0 * 90.0) / 3020.0),
        ],
    )
    expected_region_demand["SETTLEMENTDATE"] = pd.to_datetime(
        expected_region_demand["SETTLEMENTDATE"]
    )
    expected_region_demand["SETTLEMENTDATE"] = expected_region_demand[
        "SETTLEMENTDATE"
    ].dt.strftime("%Y-%m-%d %X")
    region_demand = region_demand.sort_values(["SETTLEMENTDATE"]).reset_index(drop=True)
    pd.testing.assert_frame_equal(region_demand, expected_region_demand)


def test_unit_types_vwap(monkeypatch):
    monkeypatch.setattr(
        "nem_bidding_dashboard.fetch_data.dynamic_data_compiler", dynamic_data_compiler
    )
    monkeypatch.setattr("nem_bidding_dashboard.fetch_data.static_table", static_table)
    unit_types = query_cached_data.unit_types(
        "tests/nemosis_dummy_cache", ["SA"], "Generator"
    )
    unit_types_expected = pd.DataFrame(
        columns=["UNIT TYPE"],
        data=[
            ("Battery Discharge",),
            ("Engine",),
            ("Gas Thermal",),
            ("OCGT",),
            ("Pump Storage Discharge",),
            ("Run of River Hydro",),
            ("Solar",),
            ("Wind",),
        ],
    )
    unit_types = unit_types.sort_values(["UNIT TYPE"]).reset_index(drop=True)
    pd.testing.assert_frame_equal(unit_types, unit_types_expected)
