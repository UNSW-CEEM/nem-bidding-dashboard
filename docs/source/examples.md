
# Examples

## Setup a PostgresSQL database

This section provides a guide on using the nem-bidding-dashboard package with a PostgresSQL backend. The main reason to
set up and use a PostgresSQl database is that it makes retrieving and aggregating data much faster than re-loading data
from files cached by NEMOSIS.

### Install PostgresSQL and create database

To use a PostgresSQL database, PostgresSQL needs to be installed, you'll also want to install pgAdmin4 which you can
use to create a specific database to use with nem-bidding-dashboard. 

Follow this guide to install PostgresSQL and pgAdmin4: 
[https://www.postgresqltutorial.com/postgresql-getting-started/install-postgresql/](
https://www.postgresqltutorial.com/postgresql-getting-started/install-postgresql/)

Don't forget to remember your password and port number that you configure as part of the install.

After the installation is completed launch pgAdmin4 and use it to create a new PostgresSQL database.

You can use the default username 'postgres' and the password you created to connect to the database. However, depending
on your use case it may be better (and more secure) to set up a new user which only has permissions for interacting
with the database you are using with nem-bidding-dashboard.

When using nem-bidding-dashboard with PostgresSQL you'll need to create a connection string to provide 
nem-bidding-dashboard functions, so they can connect to your database. nem-bidding-dashboard provides a helper
function for creating a connection string for local postgres databases as shown below.

```python
from nem_bidding_dashboard import postgres_helpers

con_string = postgres_helpers.build_connection_string(
hostname='localhost',
dbname='bidding_dashboard_db',
username='bidding_dashboard_maintainer',
password='1234abcd', 
port=5433)
```

### Build database tables and functions

After installing PostgresSQL and creating a database for use with nem-bidding-dashboards the next setup is to build the
database tables and functions utilised by nem-bidding-dashboard. This can be done as shown below:

```python
from nem_bidding_dashboard import postgres_helpers, build_postgres_db

con_string = postgres_helpers.build_connection_string(
    hostname='localhost',
    dbname='bidding_dashboard_db',
    username='bidding_dashboard_maintainer',
    password='1234abcd',
    port=5433)

build_postgres_db.create_db_tables_and_functions(con_string)
```

If you need to delete these tables and functions this can be done as shown below.

```python
from nem_bidding_dashboard import postgres_helpers

con_string = postgres_helpers.build_connection_string(
hostname='localhost',
dbname='bidding_dashboard_db',
username='bidding_dashboard_maintainer',
password='1234abcd',
port=5433)

postgres_helpers.drop_tables_and_functions(con_string)
```

### Populate database with data

Before you can make use of you're postgres tables and function you'll need to add data, this can be done as shown below.
Note it's best to add only one month of data at time to avoid running into memory limitations on your computer.

```python
from nem_bidding_dashboard import postgres_helpers, populate_postgres_db

con_string = postgres_helpers.build_connection_string(
    hostname='localhost',
    dbname='bidding_dashboard_db',
    username='bidding_dashboard_maintainer',
    password='1234abcd',
    port=5433)

raw_data_cache = "D:/nemosis_cache"
start = "2020/01/01 00:00:00"
end = "2020/02/01 00:00:00"

populate_postgres_db.duid_info(con_string, raw_data_cache)
populate_postgres_db.bid_data(con_string, start, end, raw_data_cache)
populate_postgres_db.region_data(con_string, start, end, raw_data_cache)
populate_postgres_db.unit_dispatch(con_string, start, end, raw_data_cache)
```

### Retrieve or aggregate data from database

Now that you've installed PostgresSQL, create a database, built the required tables and functions, and populated the 
database with data, you can begin using to retrieve and aggregate data.

To retrieve data or perform your own custom queries of the database you can use the run_query_return_dataframe function.

```python
from nem_bidding_dashboard import postgres_helpers

con_string = postgres_helpers.build_connection_string(
hostname='localhost',
dbname='bidding_dashboard_db',
username='bidding_dashboard_maintainer',
password='1234abcd',
port=5433)

data = postgres_helpers.run_query_return_dataframe(
    con_string, "select * from duid_info limit 10;")

print(data)
#        DUID REGION  ...           UNIT TYPE                 STATION NAME
# 0   ADPBA1G     SA  ...   Battery Discharge  Adelaide Desalination Plant
# 1   ADPBA1L     SA  ...      Battery Charge  Adelaide Desalination Plant
# 2    ADPMH1     SA  ...  Run of River Hydro  Adelaide Desalination Plant
# 3    ADPPV3     SA  ...               Solar  Adelaide Desalination Plant
# 4    ADPPV2     SA  ...               Solar  Adelaide Desalination Plant
# 5    ADPPV1     SA  ...               Solar  Adelaide Desalination Plant
# 6  AGLSITA1    NSW  ...              Engine              Agl Kemps Creek
# 7   ANGAST1     SA  ...              Engine       Angaston Power Station
# 8     APPIN    NSW  ...              Engine            Appin Power Plant
# 9     ARWF1    VIC  ...                Wind             Ararat Wind Farm
# <BLANKLINE>
# [10 rows x 7 columns]
```

Alternatively you can use the functions defined in the module query_postgres_db to perform predefined filtering and 
aggregation procedures. For example, aggregating bidding data from a number of regions, as shown below:

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
    'hourly',
    'Generator',
    'adjusted',
    [])

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

### Run web app locally

When the functionality to run the web app locally with a local postgres database is complete an example will be added
here.

## Using the file cache backend

Data can also be retrieved, and aggregated directly from the raw file cache kept by 
[NEMOSIS](https://github.com/UNSW-CEEM/NEMOSIS) the package is used to download and compile raw data from AEMO. While,
this means no PostgresSQL database is needed, if processed or aggregated data is going to be queried on an ongoing 
using the file cache will be significantly slower than using the postgres database.

### Retrieving raw AEMO data

While, nem-bidding-dashboard uses [NEMOSIS](https://github.com/UNSW-CEEM/NEMOSIS) to download raw AEMO data, it wraps 
the calls to NEMOSIS in its own functions that check both AEMO current and archive tables, perform filtering of 
market intervention periods, and in some cases combine data from multiple tables. The functions for compiling raw AEMO 
data are in the module fetch_data.

This example demonstrates retrieving regional price and demand data:

```python

from nem_bidding_dashboard import fetch_data

regional_data =  fetch_data.get_region_data(
    start_time='2020/01/01 00:00:00',
    end_time='2020/01/01 00:05:00',
    raw_data_cache='D:/nemosis_data_cache')

print(regional_data)
#   REGIONID      SETTLEMENTDATE  TOTALDEMAND       RRP
# 0     NSW1 2020-01-01 00:05:00      7245.31  49.00916
# 1     QLD1 2020-01-01 00:05:00      6095.75  50.81148
# 2      SA1 2020-01-01 00:05:00      1466.53  68.00000
# 3     TAS1 2020-01-01 00:05:00      1010.06  81.79115
# 4     VIC1 2020-01-01 00:05:00      4267.32  65.67826
```

### Retrieving processed data

Before data is stored in the postgres database for use by the dashboard a number of preprocessing steps are carried out
to reduce the data volume and speed up the final aggregation and processing when the dashboard sends a request to the 
database. The user can retrieve the data in the form stored in the database, but without creating a database, using the 
functions in the module fetch_and_preprocess, these are the same functions used to prepare data before loading into the 
database.

For example, the function fetch_and_preprocess.bid_data can be used to retrieve processed bidding data in the format 
stored in the database:

```python
from nem_bidding_dashboard import fetch_and_preprocess

bids = fetch_and_preprocess.bid_data(
    start_time='2020/01/01 00:00:00',
    end_time='2020/01/01 00:05:00',
    raw_data_cache='D:/nemosis_data_cache')

print(bids.head(5))
#        INTERVAL_DATETIME      DUID  BIDBAND  BIDVOLUME  BIDVOLUMEADJUSTED  BIDPRICE
# 0    2020-01-01 00:05:00    BALBL1        1         20                0.0    -48.06
# 1    2020-01-01 00:05:00    RT_SA4        1       3000                0.0  -1000.00
# 2    2020-01-01 00:05:00    RT_SA5        1       3000                0.0  -1000.00
# 3    2020-01-01 00:05:00    RT_SA6        1       3000                0.0  -1000.00
# 4    2020-01-01 00:05:00   RT_TAS1        1       3000                0.0  -1000.00
```

### Aggregating data

The data aggregation and querying options available for the database are also replicated for the raw data cache. This
allows the user to get the aggregate data shown in the bidding dashboard without setting up a local postgres database.

For example, bidding data can be aggregated as shown below:

```python

from nem_bidding_dashboard import query_cached_data


agg_bids = query_cached_data.aggregate_bids(
    ['QLD', 'NSW', 'SA'],
    "2020/01/01 00:00:00",
    "2020/01/01 01:00:00",
    'hourly',
    'Generator',
    'adjusted',
    [],
    'D:/nemosis_data_cache')

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