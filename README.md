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
    start_time='2020/01/01 00:00:00',
    end_time='2020/01/01 00:05:00',
    raw_data_cache='D:/nemosis_data_cache')

print(volume_bids.head(5))
#         SETTLEMENTDATE      DUID  ... PASAAVAILABILITY   INTERVAL_DATETIME
# 9547382     2019-12-31    AGLHAL  ...            181.0 2020-01-01 00:05:00
# 9547383     2019-12-31    AGLSOM  ...            140.0 2020-01-01 00:05:00
# 9547384     2019-12-31   ANGAST1  ...             44.0 2020-01-01 00:05:00
# 9547385     2019-12-31     APD01  ...              0.0 2020-01-01 00:05:00
# 9547386     2019-12-31     APD01  ...              NaN 2020-01-01 00:05:00
```

#### Get processed data
To get data in the format stored by nem-bidding-dashboard in the PostgresSQL database use functions in the module
`fetch_and_preprocess`, e.g. `bid_data`.

```python
from nem_bidding_dashboard import fetch_and_preprocess

bids = fetch_and_preprocess.bid_data(
    start_time='2020/01/01 00:00:00',
    end_time='2020/01/01 00:05:00',
    raw_data_cache='D:/nemosis_data_cache')

print(bids.head(5))
#        INTERVAL_DATETIME      DUID  ...  BIDVOLUMEADJUSTED  BIDPRICE
# 0    2020-01-01 00:05:00    BALBL1  ...                0.0    -48.06
# 1    2020-01-01 00:05:00    RT_SA4  ...                0.0  -1000.00
# 2    2020-01-01 00:05:00    RT_SA5  ...                0.0  -1000.00
# 3    2020-01-01 00:05:00    RT_SA6  ...                0.0  -1000.00
# 4    2020-01-01 00:05:00   RT_TAS1  ...                0.0  -1000.00
```

#### Setup a PostgresSQL database

Create tables for storing processed data and functions, then populate the database with historical data.

```python
from nem_bidding_dashboard import postgres_helpers, build_postgres_db, populate_postgres_db

con_string = postgres_helpers.build_connection_string(
    hostname='localhost',
    dbname='bidding_dashboard_db',
    username='bidding_dashboard_maintainer',
    password='1234abcd',
    port=5433)

build_postgres_db.create_db_tables_and_functions(con_string)

raw_data_cache = "D:/nemosis_cache"
start = "2020/01/01 00:00:00"
end = "2020/02/01 00:00:00"

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
    con_string,
    ['QLD', 'NSW', 'SA'],
    "2020/01/01 00:00:00",
    "2020/01/01 01:00:00",
    'hourly')

print(agg_bids)
#      INTERVAL_DATETIME        BIN_NAME  BIDVOLUME
# 0  2020-01-01 01:00:00    [1000, 5000)   1004.000
# 1  2020-01-01 01:00:00      [100, 200)    300.000
# 2  2020-01-01 01:00:00       [50, 100)   1788.000
# 3  2020-01-01 01:00:00   [-1000, -100)   9672.090
# 4  2020-01-01 01:00:00      [200, 300)   1960.000
# 5  2020-01-01 01:00:00         [0, 50)   4810.708
# 6  2020-01-01 01:00:00       [-100, 0)      7.442
# 7  2020-01-01 01:00:00      [300, 500)    157.000
# 8  2020-01-01 01:00:00     [500, 1000)    728.000
# 9  2020-01-01 01:00:00  [10000, 15500)   4359.000
# 10 2020-01-01 01:00:00   [5000, 10000)     20.000
```

## Contributing

Interested in contributing? Check out the [contributing guidelines](CONTRIBUTING.md).

Please note that this project is released with a [Code of Conduct](CONDUCT.md). By contributing to this project, you 
agree to abide by its terms.

## License

`nem-bidding-dashboard` was created by `Nicholas Gorman` and `Patrick Chambers`. It is licensed under the terms of the 
`BSD-3-Clause license`.

