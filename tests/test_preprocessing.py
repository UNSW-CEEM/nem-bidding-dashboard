import pandas as pd

from nem_bidding_dashboard import preprocessing


def test_adjust_bids_for_availability_duid_with_two_intervals(monkeypatch):
    bid_data = pd.DataFrame(
        columns=[
            "INTERVAL_DATETIME",
            "DUID",
            "BIDBAND",
            "BIDVOLUME",
            "BIDPRICE",
        ],
        data=[
            ("2020/01/01 01:00:00", "A", 4, 10, 55),
            ("2020/01/01 01:00:00", "A", 5, 20, 56),
            ("2020/01/01 01:00:00", "A", 10, 30, 57),
            ("2020/01/01 05:00:00", "A", 3, 40, 58),
            ("2020/01/01 05:00:00", "A", 9, 50, 59),
        ],
    )
    availability_data = pd.DataFrame(
        columns=["INTERVAL_DATETIME", "DUID", "AVAILABILITY"],
        data=[
            ("2020/01/01 01:00:00", "A", 25),
            ("2020/01/01 05:00:00", "A", 45),
        ],
    )
    expected_adjusted_bids = pd.DataFrame(
        columns=[
            "INTERVAL_DATETIME",
            "DUID",
            "BIDBAND",
            "BIDVOLUME",
            "BIDVOLUMEADJUSTED",
            "BIDPRICE",
        ],
        data=[
            ("2020/01/01 01:00:00", "A", 4, 10, 10, 55),
            ("2020/01/01 01:00:00", "A", 5, 20, 15, 56),
            ("2020/01/01 01:00:00", "A", 10, 30, 0, 57),
            ("2020/01/01 05:00:00", "A", 3, 40, 40, 58),
            ("2020/01/01 05:00:00", "A", 9, 50, 5, 59),
        ],
    )
    adjusted_bids = preprocessing.adjust_bids_for_availability(
        bid_data, availability_data
    )
    pd.testing.assert_frame_equal(adjusted_bids, expected_adjusted_bids)


def test_adjust_bids_for_availability_duid_with_two_units(monkeypatch):
    bid_data = pd.DataFrame(
        columns=[
            "INTERVAL_DATETIME",
            "DUID",
            "BIDBAND",
            "BIDVOLUME",
            "BIDPRICE",
        ],
        data=[
            ("2020/01/01 01:00:00", "A", 4, 10, 55),
            ("2020/01/01 01:00:00", "A", 5, 20, 56),
            ("2020/01/01 01:00:00", "A", 10, 30, 57),
            ("2020/01/01 01:00:00", "B", 3, 40, 58),
            ("2020/01/01 01:00:00", "B", 9, 50, 59),
        ],
    )
    availability_data = pd.DataFrame(
        columns=["INTERVAL_DATETIME", "DUID", "AVAILABILITY"],
        data=[
            ("2020/01/01 01:00:00", "A", 0),
            ("2020/01/01 01:00:00", "B", 35),
        ],
    )
    expected_adjusted_bids = pd.DataFrame(
        columns=[
            "INTERVAL_DATETIME",
            "DUID",
            "BIDBAND",
            "BIDVOLUME",
            "BIDVOLUMEADJUSTED",
            "BIDPRICE",
        ],
        data=[
            ("2020/01/01 01:00:00", "A", 4, 10, 0, 55),
            ("2020/01/01 01:00:00", "A", 5, 20, 0, 56),
            ("2020/01/01 01:00:00", "A", 10, 30, 0, 57),
            ("2020/01/01 01:00:00", "B", 3, 40, 35, 58),
            ("2020/01/01 01:00:00", "B", 9, 50, 0, 59),
        ],
    )
    adjusted_bids = preprocessing.adjust_bids_for_availability(
        bid_data, availability_data
    )
    pd.testing.assert_frame_equal(adjusted_bids, expected_adjusted_bids)
