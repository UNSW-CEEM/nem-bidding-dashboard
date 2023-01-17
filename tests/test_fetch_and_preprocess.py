import pandas as pd
from mock_nemosis import dynamic_data_compiler, static_table

from nem_bidding_dashboard import fetch_and_preprocess


def test_duid_info(monkeypatch):
    monkeypatch.setattr("nem_bidding_dashboard.fetch_data.static_table", static_table)
    duid_info = fetch_and_preprocess.duid_info("dummy_directory")
    expected_duid_info = pd.read_csv("tests/test_duid_data_preprocessing_output.csv")
    pd.testing.assert_frame_equal(duid_info, expected_duid_info)


def test_bid_data(monkeypatch):
    monkeypatch.setattr(
        "nem_bidding_dashboard.fetch_data.dynamic_data_compiler", dynamic_data_compiler
    )
    bid_data = fetch_and_preprocess.bid_data(
        "dummy_start", "dummy_end", "dummy_directory"
    )
    expected_bid_data = pd.DataFrame(
        columns=[
            "INTERVAL_DATETIME",
            "DUID",
            "BIDBAND",
            "BIDVOLUME",
            "BIDVOLUMEADJUSTED",
            "BIDPRICE",
            "ONHOUR",
        ],
        data=[
            ("2020/01/01 01:00:00", "A", 4, 70, 70, 4.1, True),
            ("2020/01/01 01:00:00", "A", 10, 40, 31.0, 10.1, True),
            ("2020/01/01 05:00:00", "A", 3, 70, 70, 3.2, True),
            ("2020/01/01 05:00:00", "A", 9, 12, 12, 9.2, True),
            ("2020/01/01 01:00:00", "B", 5, 100, 100, 5.3, True),
            ("2020/01/01 01:00:00", "B", 9, 10, 10, 9.3, True),
            ("2020/01/01 05:00:00", "B", 4, 100, 100, 4.4, True),
            ("2020/01/01 05:00:00", "B", 8, 100, 100, 8.4, True),
            ("2020/01/01 05:00:00", "B", 9, 10, 3.0, 9.4, True),
        ],
    )
    expected_bid_data["INTERVAL_DATETIME"] = pd.to_datetime(
        expected_bid_data["INTERVAL_DATETIME"]
    )
    expected_bid_data["INTERVAL_DATETIME"] = expected_bid_data[
        "INTERVAL_DATETIME"
    ].dt.strftime("%Y-%m-%d %X")
    bid_data = bid_data[bid_data["ONHOUR"]]
    bid_data = bid_data.sort_values(
        ["DUID", "INTERVAL_DATETIME", "BIDBAND"]
    ).reset_index(drop=True)
    pd.testing.assert_frame_equal(bid_data, expected_bid_data)


def test_unit_dispatch(monkeypatch):
    monkeypatch.setattr(
        "nem_bidding_dashboard.fetch_data.dynamic_data_compiler", dynamic_data_compiler
    )
    unit_dispatch = fetch_and_preprocess.unit_dispatch(
        "dummy_start", "dummy_end", "dummy_directory"
    )
    expected_unit_dispatch = pd.DataFrame(
        columns=[
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
            "ONHOUR",
        ],
        data=[
            (
                "2020/01/01 01:00:00",
                "A",
                101.0,
                81.0,
                71.0,
                71.0 + 50 * 5,
                71.0 - 1 * 5,
                71.0 + 426.0 / 12,
                71.0 - 122.0 / 12,
                110,
                101,
                True,
            ),
            (
                "2020/01/01 05:00:00",
                "A",
                103.0,
                82.0,
                72.0,
                72.0 + 1 * 5,
                72.0 - 50 * 5,
                72.0 + 246.0 / 12,
                72.0 - 1000.0 / 12,
                120,
                102,
                True,
            ),
            (
                "2020/01/01 01:00:00",
                "B",
                201.0,
                91.0,
                61.0,
                61.0 + 2 * 5,
                61.0 - 60 * 5,
                61.0 + 128.0 / 12,
                61.0 - 124.0 / 12,
                130,
                103,
                True,
            ),
            (
                "2020/01/01 05:00:00",
                "B",
                203.0,
                92.0,
                62.0,
                62.0 + 60 * 5,
                62.0 - 20 * 5,
                62.0 + 248.0 / 12,
                62.0 - 244.0 / 12,
                140,
                104,
                True,
            ),
        ],
    )
    expected_unit_dispatch["INTERVAL_DATETIME"] = pd.to_datetime(
        expected_unit_dispatch["INTERVAL_DATETIME"]
    )
    expected_unit_dispatch["INTERVAL_DATETIME"] = expected_unit_dispatch[
        "INTERVAL_DATETIME"
    ].dt.strftime("%Y-%m-%d %X")
    unit_dispatch = unit_dispatch.sort_values(
        ["DUID", "INTERVAL_DATETIME"]
    ).reset_index(drop=True)
    pd.testing.assert_frame_equal(unit_dispatch, expected_unit_dispatch)
