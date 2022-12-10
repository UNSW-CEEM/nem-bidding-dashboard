import sys

import pandas as pd

sys.modules["nemosis"] = __import__("mock_nemosis")

from nem_bidding_dashboard import fetch_data


def test_fetch_data_region_data_current_and_archive():
    region_data = fetch_data.get_region_data(
        "2020/01/01 00:00:00", "2020/01/02 00:00:00", "dummy_directory"
    )
    expected_data = pd.DataFrame(
        columns=["REGIONID", "SETTLEMENTDATE", "TOTALDEMAND", "RRP"],
        data=[
            ("SA1", "2020/01/01 01:00:00", 1000.0, 55.4),
            ("TAS1", "2020/01/01 01:00:00", 2000.0, 75.4),
            ("SA1", "2020/01/01 05:00:00", 1010.0, 80.1),
            ("TAS1", "2020/01/01 05:00:00", 2010.0, 90.0),
        ],
    )
    expected_data["SETTLEMENTDATE"] = pd.to_datetime(expected_data["SETTLEMENTDATE"])
    region_data = region_data.sort_values(["REGIONID", "SETTLEMENTDATE"]).reset_index(
        drop=True
    )
    expected_data = expected_data.sort_values(
        ["REGIONID", "SETTLEMENTDATE"]
    ).reset_index(drop=True)
    pd.testing.assert_frame_equal(region_data, expected_data)


def test_fetch_data_region_data_archive():
    region_data = fetch_data.get_region_data(
        "2020/01/01 00:00:00", "2020/01/01 01:00:00", "dummy_directory"
    )
    expected_data = pd.DataFrame(
        columns=["REGIONID", "SETTLEMENTDATE", "TOTALDEMAND", "RRP"],
        data=[
            ("SA1", "2020/01/01 01:00:00", 1000.0, 55.4),
            ("TAS1", "2020/01/01 01:00:00", 2000.0, 75.4),
        ],
    )
    expected_data["SETTLEMENTDATE"] = pd.to_datetime(expected_data["SETTLEMENTDATE"])
    region_data = region_data.sort_values(["REGIONID", "SETTLEMENTDATE"]).reset_index(
        drop=True
    )
    expected_data = expected_data.sort_values(
        ["REGIONID", "SETTLEMENTDATE"]
    ).reset_index(drop=True)
    pd.testing.assert_frame_equal(region_data, expected_data)


def test_fetch_data_availability_data_current_and_archive():
    region_data = fetch_data.get_duid_availability_data(
        "2020/01/01 00:00:00", "2020/01/02 00:00:00", "dummy_directory"
    )
    expected_data = pd.DataFrame(
        columns=[
            "SETTLEMENTDATE",
            "DUID",
            "AVAILABILITY",
            "TOTALCLEARED",
            "INITIALMW",
            "RAMPDOWNRATE",
            "RAMPUPRATE",
        ],
        data=[
            ("2020/01/01 01:00:00", "A", 101.0, 81.0, 71.0, 122.0, 126.0),
            ("2020/01/01 05:00:00", "A", 103.0, 82.0, 72.0, 242.0, 246.0),
            ("2020/01/01 01:00:00", "B", 201.0, 91.0, 61.0, 124.0, 128.0),
            ("2020/01/01 05:00:00", "B", 203.0, 92.0, 62.0, 244.0, 248.0),
        ],
    )
    expected_data["SETTLEMENTDATE"] = pd.to_datetime(expected_data["SETTLEMENTDATE"])
    region_data = region_data.sort_values(["SETTLEMENTDATE", "DUID"]).reset_index(
        drop=True
    )
    expected_data = expected_data.sort_values(["SETTLEMENTDATE", "DUID"]).reset_index(
        drop=True
    )
    pd.testing.assert_frame_equal(region_data, expected_data)


def test_fetch_data_availability_data_archive():
    region_data = fetch_data.get_duid_availability_data(
        "2020/01/01 00:00:00", "2020/01/01 01:00:00", "dummy_directory"
    )
    expected_data = pd.DataFrame(
        columns=[
            "SETTLEMENTDATE",
            "DUID",
            "AVAILABILITY",
            "TOTALCLEARED",
            "INITIALMW",
            "RAMPDOWNRATE",
            "RAMPUPRATE",
        ],
        data=[
            ("2020/01/01 01:00:00", "A", 101.0, 81.0, 71.0, 122.0, 126.0),
            ("2020/01/01 01:00:00", "B", 201.0, 91.0, 61.0, 124.0, 128.0),
        ],
    )
    expected_data["SETTLEMENTDATE"] = pd.to_datetime(expected_data["SETTLEMENTDATE"])
    region_data = region_data.sort_values(["SETTLEMENTDATE", "DUID"]).reset_index(
        drop=True
    )
    expected_data = expected_data.sort_values(["SETTLEMENTDATE", "DUID"]).reset_index(
        drop=True
    )
    pd.testing.assert_frame_equal(region_data, expected_data)


def test_fetch_data_duid_info():
    region_data = fetch_data.get_duid_data("dummy_directory")
    expected_data = pd.DataFrame(
        columns=[
            "DUID",
            "REGION",
            "FUEL SOURCE - DESCRIPTOR",
            "DISPATCH TYPE",
            "TECHNOLOGY TYPE - DESCRIPTOR",
            "STATION NAME",
        ],
        data=[
            (
                "A",
                "SA1",
                "Solar",
                "Generator",
                "Photovoltaic Tracking Flat panel",
                "Solar A",
            ),
            ("B", "TAS1", "-", "Load", "-", "Hydro B"),
        ],
    )
    region_data = region_data.sort_values(["DUID"]).reset_index(drop=True)
    expected_data = expected_data.sort_values(["DUID"]).reset_index(drop=True)
    pd.testing.assert_frame_equal(region_data, expected_data)
