# Introduction

nem-bidding-dashboard is a web app and python package for collating, processing and visualising data relevant to
understanding participant behaviour in the Australian National Electricity Market wholesale spot market.

The web app is intended to make reviewing the bidding behaviour of market participants as easy as possible. Aggregate
behaviour can be visualised as at a whole of market, regional, or technology level. Alternatively, the non-aggregated
bids of dispatch units, and stations can be visualised.

We have additionally published the code required to run the web app as a python package, so that it can used to help
analysis and visualise bidding behaviour in alternative or more sophisticated ways than allowed by the web app.

The development of nem-bidding-dashboard up to December 2022 was funded by the
[Digital Grid Futures Institute](https://www.dgfi.unsw.edu.au/)

## Status

Underdevelopment and not recommended for use yet, check back here in early 2023 to see if a production version has been
launched.

## Web app

nem-bidding-dashboard is hosted as a web app here: [http://nembiddingdashboard.org](http://nembiddingdashboard.org)

## Python package / API

The python api can be used to:
- run the web app interface locally
- download publicly available bidding and operational data from the Australian Energy Market Operator
- process and aggregate bidding and operational data into the format used in the web app
- build and populate a PostgresSQL database for efficiently querying and aggregating bidding and operational data

### Installation

`pip install nem_bidding_dashboard`

### Quick examples

Below are some quick examples that provide at taste of the api capabilities, see the full set of examples and api
documentation for a complete guide.

#### Get raw data
To get the raw data used by nem-bidding-dashboard before preprocessing use functions in the `fetch_data` module, e.g.
`get_volume_bids`.

```python
from nem_bidding_dashboard import fetch_data

volume_bids = fetch_data.get_volume_bids(
    start_time='2022/01/01 00:00:00',
    end_time='2022/01/01 00:05:00',
    raw_data_cache='D:/nemosis_data_cache')

print(volume_bids.head(5))
#        SETTLEMENTDATE     DUID  ... PASAAVAILABILITY   INTERVAL_DATETIME
# 309360     2021-12-31  ADPBA1G  ...              6.0 2022-01-01 00:05:00
# 309361     2021-12-31  ADPBA1G  ...              NaN 2022-01-01 00:05:00
# 309362     2021-12-31  ADPBA1G  ...              NaN 2022-01-01 00:05:00
# 309363     2021-12-31  ADPBA1G  ...              NaN 2022-01-01 00:05:00
# 309364     2021-12-31  ADPBA1G  ...              NaN 2022-01-01 00:05:00
#
# [5 rows x 18 columns]
```

#### Get processed data
To get data in the format stored by nem-bidding-dashboard in the PostgresSQL database use functions in the module
`fetch_and_preprocess`, e.g. `bid_data`.

```python
from nem_bidding_dashboard import fetch_and_preprocess

bids = fetch_and_preprocess.bid_data(
    start_time='2022/01/01 00:00:00',
    end_time='2022/01/01 00:05:00',
    raw_data_cache='D:/nemosis_data_cache')

print(bids.head(5))
#        INTERVAL_DATETIME     DUID  BIDBAND  BIDVOLUME  BIDVOLUMEADJUSTED  BIDPRICE  ONHOUR
# 0    2022-01-01 00:05:00  ADPBA1G        8          6                0.0    998.00   False
# 462  2022-01-01 00:05:00   REECE1        2         45               45.0    -55.03   False
# 463  2022-01-01 00:05:00   REECE1        4         74               74.0     -0.85   False
# 464  2022-01-01 00:05:00   REECE2        2         35               35.0    -54.77   False
# 465  2022-01-01 00:05:00   REECE2        4         84               84.0     -0.86   False
```

#### Setup a PostgresSQL database

Create tables for storing processed data and functions, then populate the database with historical data.

```python
from nem_bidding_dashboard import postgres_helpers, populate_postgres_db

con_string = postgres_helpers.build_connection_string(
    hostname='localhost',
    dbname='bidding_dashboard_db',
    username='bidding_dashboard_maintainer',
    password='1234abcd',
    port=5433)

raw_data_cache = "D:/nemosis_cache"
start = "2022/01/01 00:00:00"
end = "2022/02/01 00:00:00"

populate_postgres_db.duid_info(con_string, raw_data_cache)
populate_postgres_db.bid_data(con_string, start, end, raw_data_cache)
populate_postgres_db.region_data(con_string, start, end, raw_data_cache)
populate_postgres_db.unit_dispatch(con_string, start, end, raw_data_cache)
```

#### Query and aggregate bidding data from PostgresSQL database

Filter bids by time and region, and then aggregate into price bands. Other functions in the module `query_postgres_db`
provide querying an aggregation and functionality for each table in the db.

```python
from nem_bidding_dashboard import postgres_helpers, query_postgres_db

con_string = postgres_helpers.build_connection_string(
    hostname='localhost',
    dbname='bidding_dashboard_db',
    username='bidding_dashboard_maintainer',
    password='1234abcd',
    port=5433)

agg_bids = query_postgres_db.aggregate_bids(
    connection_string=con_string,
    regions=['QLD', 'NSW', 'SA'],
    start_time="2022/01/01 00:00:00",
    end_time="2022/01/01 01:00:00",
    resolution='hourly',
    raw_adjusted='adjusted',
    tech_types=[],
    dispatch_type='Generator')

print(agg_bids)
#       INTERVAL_DATETIME        BIN_NAME   BIDVOLUME
# 0   2022-01-01 01:00:00   [-1000, -100)  9673.93400
# 1   2022-01-01 01:00:00       [-100, 0)   366.70236
# 2   2022-01-01 01:00:00         [0, 50)  1527.00000
# 3   2022-01-01 01:00:00       [50, 100)  1290.00000
# 4   2022-01-01 01:00:00      [100, 200)   908.00000
# 5   2022-01-01 01:00:00      [200, 300)  1217.00000
# 6   2022-01-01 01:00:00      [300, 500)   943.00000
# 7   2022-01-01 01:00:00     [500, 1000)   240.00000
# 8   2022-01-01 01:00:00    [1000, 5000)   210.00000
# 9   2022-01-01 01:00:00   [5000, 10000)   125.00000
# 10  2022-01-01 01:00:00  [10000, 15500)  6766.00000
```

## Contributing

Interested in contributing? Check out the [contributing guidelines](https://github.com/UNSW-CEEM/nem-bidding-dashboard/blob/master/CONTRIBUTING.md).

Please note that this project is released with a [Code of Conduct](https://github.com/UNSW-CEEM/nem-bidding-dashboard/blob/master/CONDUCT.md). By contributing to this project, you
agree to abide by its terms.

## License

`nem-bidding-dashboard` was created by `Nicholas Gorman` and `Patrick Chambers`. It is licensed under the terms of the
`BSD-3-Clause license`.
