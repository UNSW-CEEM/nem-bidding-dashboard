from time import sleep

import pandas as pd

from nem_bidding_dashboard import fetch_and_preprocess

unit_info_1 = fetch_and_preprocess.duid_info("nemosis_cache")
sleep(10)
unit_info_2 = fetch_and_preprocess.duid_info("nemosis_cache")
pd.testing.assert_frame_equal(unit_info_1, unit_info_2)
