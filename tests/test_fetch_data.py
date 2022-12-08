import pytest
import pandas as pd

import sys

sys.modules['nemosis'] = __import__('mock_nemosis')

from nem_bidding_dashboard import fetch_data


def test_stack_bids():
    region_data = fetch_data.get_region_data(
        '2020/01/01 00:00:00',
        '2020/01/01 00:00:00',
        'dummy_directory')
    expected_data = pd.DataFrame(
        columns=["REGIONID", "SETTLEMENTDATE", "TOTALDEMAND", "RRP"],
        data=[('SA', '2020/01/01 01:00:00', 1000.0, 55.4),
              ('TAS', '2020/01/01 01:00:00', 2000.0, 75.4),
              ('SA', '2020/01/01 05:00:00', 1010.0, 80.1),
              ('TAS', '2020/01/01 05:00:00', 2010.0, 90.0)]
    )
    expected_data['SETTLEMENTDATE'] = pd.to_datetime(expected_data['SETTLEMENTDATE'])
    region_data = region_data.sort_values(['REGIONID', 'SETTLEMENTDATE']).reset_index(drop=True)
    expected_data = expected_data.sort_values(['REGIONID', 'SETTLEMENTDATE']).reset_index(drop=True)
    pd.testing.assert_frame_equal(region_data, expected_data)