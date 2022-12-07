
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

### Retrieve aggregate data from database

Now that you've installed PostgresSQL, create a database, built the required tables and functions, and populated the 
database with data, you can begin using to retrieve and aggregate data.

To retrieve data or perform your own custom queries of the data you can use the run_query_return_dataframe function.

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
aggregation procedures. For example:

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

### Run web app locally

When the functionality to run the web app locally with a local postgres database is complete an example will be added
here.

## Retrieving and processing raw data