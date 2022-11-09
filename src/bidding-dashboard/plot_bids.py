"""
TODO:
    Fetch data from supabase database
    Fix issue with Tasmania
    Option for adjusting start time as well as date
    Aggregate by DUID
    Documentation
"""


from datetime import datetime, date, timedelta 
import time
import fetch_data
import layout_template

import pandas as pd
import os
from pandasql import sqldf
import plotly.express as px
import plotly.graph_objects as go
import numpy as np
from dash import Dash, dcc, html, Input, Output
import dash
from nemosis import dynamic_data_compiler, static_table


cwd = os.path.dirname(__file__)
raw_data_cache = os.path.join(cwd, "nemosis_data_cache/")
ffformat = "parquet"

app = Dash(__name__)
app.title = "NEM Dashboard"


title = "NEM Bidding Data"
settings_content = [
    html.Div(
        id="date-selector",
        children=[
            dcc.DatePickerSingle(
                className="",
                id="start-date-picker",
                date=date(2020, 1, 1), 
                display_format="DD/MM/YY",
            ), 
            dcc.RadioItems(
                id="duration-selector",
                options=["Daily", "Weekly"],
                value="Daily",
                inline=True,
            )
        ]
    ),

    dcc.Checklist(
        id="region-checklist",
        options=["NSW", "VIC", "TAS", "SA", "QLD"],
        value=[],
        inline=True,
    ),
    html.Button(
        "Update Graph", 
        id="update-graph-button",
        n_clicks=0,
    ), 
]
graph_content = dcc.Graph(id="graph")
app.layout = layout_template.build(title, settings_content, graph_content)

"""
TODO
Update plot when the user interacts with any of the inputs, i.e. the start date, 
end date or region selection. 
Arguments:
    start_date: Initial date for graph in form "DD-MM-YYYY", taken from the 
        starting date picker. 
    end_date: Ending date for graph in form "DD-MM-YYYY", taken from the 
        ending date picker. 
Returns:
    px line graph figure displaying electricity demand data for the selected 
    time period and regions. 
"""
@app.callback(
    Output("graph", "figure"),
    Input("start-date-picker", "date"),
    Input("duration-selector", "value"),
    Input("region-checklist", "value"),
    Input("update-graph-button", "n_clicks"))
def update(start_date: str, duration: str, regions:list, num_clicks: int):
    trigger_id = dash.ctx.triggered_id
    if trigger_id and trigger_id != "update-graph-button":
        return dash.no_update
    # TODO: only update dataframe when required
    start_date = f"{start_date.replace('-', '/')} 00:00:00"
    start_date_obj = datetime.strptime(start_date, "%Y/%m/%d %H:%M:%S")
    if (duration == "Daily"):
        end_date = (start_date_obj + timedelta(days=1)).strftime("%Y/%m/%d %H:%M:%S")
    elif (duration == "Weekly"):
        end_date = (start_date_obj + timedelta(days=7)).strftime("%Y/%m/%d %H:%M:%S")

    #runtime_start = time.time()
    fig = plot_bids(start_date, end_date, duration, regions)
    #write_runtime_data(start_date, end_date, runtime_start, "plot_bid_runtime", ffformat)

    return fig


def stack_unit_bids(volume_bids, price_bids):
    """Combine volume and price components of offers and reformat them such that each price quantity pair is on a
    separate row of the dataframe."""
    volume_bids = pd.melt(volume_bids, id_vars=['INTERVAL_DATETIME', 'DUID'],
                          value_vars=['BANDAVAIL1', 'BANDAVAIL2', 'BANDAVAIL3', 'BANDAVAIL4',
                                      'BANDAVAIL5', 'BANDAVAIL6', 'BANDAVAIL7', 'BANDAVAIL8',
                                      'BANDAVAIL9', 'BANDAVAIL10'],
                          var_name='BIDBAND', value_name='BIDVOLUME')
    price_bids = pd.melt(price_bids, id_vars=['SETTLEMENTDATE', 'DUID'],
                         value_vars=['PRICEBAND1', 'PRICEBAND2', 'PRICEBAND3', 'PRICEBAND4',
                                     'PRICEBAND5', 'PRICEBAND6', 'PRICEBAND7', 'PRICEBAND8',
                                     'PRICEBAND9', 'PRICEBAND10'],
                         var_name='BIDBAND', value_name='BIDPRICE')
    price_bids['APPLICABLEFROM'] = price_bids['SETTLEMENTDATE'] + timedelta(hours=4, minutes=5)
    price_bids['BIDBAND'] = pd.to_numeric(price_bids['BIDBAND'].str[9:])
    volume_bids['BIDBAND'] = pd.to_numeric(volume_bids['BIDBAND'].str[9:])
    bids = pd.merge_asof(volume_bids.sort_values('INTERVAL_DATETIME'),
                         price_bids.sort_values('APPLICABLEFROM'),
                         left_on='INTERVAL_DATETIME', right_on='APPLICABLEFROM',
                         by=['BIDBAND', 'DUID'])
    return bids


def adjust_bids_for_availability(stacked_bids, unit_availability):
    """Adjust bid volume where the total avaibility bid in would restrict an a bid from actually being dispatched."""
    bids = stacked_bids.sort_values('BIDBAND')
    bids['BIDVOLUMECUMULATIVE'] = bids.groupby(['DUID', 'INTERVAL_DATETIME'], as_index=False)['BIDVOLUME'].cumsum()
    availability = unit_availability.rename({'SETTLEMENTDATE': 'INTERVAL_DATETIME'}, axis=1)
    bids = pd.merge(bids, availability, 'left', on=['INTERVAL_DATETIME', 'DUID'])
    bids['SPAREBIDVOLUME'] = (bids['AVAILABILITY'] - bids['BIDVOLUMECUMULATIVE']) + bids['BIDVOLUME']
    bids['SPAREBIDVOLUME'] = np.where(bids['SPAREBIDVOLUME'] < 0, 0, bids['SPAREBIDVOLUME'])
    bids['ADJUSTEDBIDVOLUME'] = bids[['BIDVOLUME', 'SPAREBIDVOLUME']].min(axis=1)
    return bids


def create_bid_stack_time_series_bar_plot(bids, demand_data, duration):
    """Plot the volume bid accorinding to a set of price range bins. Down sample to bids for dispatch intervals on the
    hour"""
    if duration == "Weekly":
        bids = bids[bids['INTERVAL_DATETIME'].dt.minute.isin([0])]
    bids = bids.sort_values('BIDPRICE')
    bins = [-1000.0, 0.0, 100.0, 300.0, 500.0, 1000.0, 5000.0, 14400.0, 14500.0, 16000.0]
    bids['price_bin'] = pd.cut(bids['BIDPRICE'], bins=bins)
    bids = bids.groupby(['INTERVAL_DATETIME', 'price_bin'], as_index=False).agg({'ADJUSTEDBIDVOLUME': 'sum'})
    fig = px.bar(
        bids, 
        x='INTERVAL_DATETIME', 
        y='ADJUSTEDBIDVOLUME', 
        color='price_bin',
    )
    demand_data = demand_data.groupby(['SETTLEMENTDATE'], as_index=False).agg({'TOTALDEMAND': 'sum'})
    fig.add_trace(go.Scatter(x=demand_data['SETTLEMENTDATE'], y=demand_data['TOTALDEMAND'],
                             marker=dict(color='blue', size=4), name='demand'))
    # Update graph axes and hover text
    fig.update_yaxes(title="Volume (MW)")
    if duration == "Weekly":
        fig.update_xaxes(title=f"Time (Bid stack sampled on the hour)")
    else:
        fig.update_xaxes(title=f"Time (Bid stack sampled at 5 min intervals)")
    fig.update_traces(
        hovertemplate="%{x}<br>Bid Volume: %{y}<extra></extra>",
    )
    fig.update_traces(
        hovertemplate="%{x}<br>Demand: %{y}<extra></extra>",
        selector={"name": "demand"}
    )
    return fig


def plot_bids(start_time:str, end_time:str, duration:str, regions:list):

    volume_bids = dynamic_data_compiler(start_time=start_time, end_time=end_time, table_name='BIDPEROFFER_D',
                                        raw_data_location=raw_data_cache,
                                        fformat=ffformat, keep_csv=True)
    if regions:
        duid_info = static_table("Generators and Scheduled Loads", raw_data_cache)

        query = f"""
            SELECT V.DUID, V.SETTLEMENTDATE, V.INTERVAL_DATETIME, D.REGION, V.BIDTYPE,
            V.BANDAVAIL1, V.BANDAVAIL1, V.BANDAVAIL2, V.BANDAVAIL3, V.BANDAVAIL4, 
            V.BANDAVAIL5, V.BANDAVAIL6, V.BANDAVAIL7, V.BANDAVAIL8, V.BANDAVAIL9, V.BANDAVAIL10 
            FROM volume_bids V LEFT JOIN duid_info D ON V.DUID = D.DUID WHERE D.REGION = '{regions[0] + "1"}'
        """
        for region in regions[1:]:
            query += f" OR D.REGION = '{region + '1'}' "
        volume_bids = sqldf(query, locals())
    volume_bids["INTERVAL_DATETIME"] = pd.to_datetime(volume_bids["INTERVAL_DATETIME"])
    volume_bids = volume_bids[volume_bids['BIDTYPE'] == 'ENERGY']
    price_bids = dynamic_data_compiler(start_time=start_time, end_time=end_time, table_name='BIDDAYOFFER_D',
                                       raw_data_location=raw_data_cache,
                                       fformat=ffformat)
    price_bids = price_bids[price_bids['BIDTYPE'] == 'ENERGY']
    availability = dynamic_data_compiler(start_time=start_time, end_time=end_time, table_name='DISPATCHLOAD',
                                         raw_data_location=raw_data_cache,
                                         fformat=ffformat, select_columns=['INTERVENTION', 'SETTLEMENTDATE', 'DUID',
                                                                            'AVAILABILITY'])
    availability = availability[availability['INTERVENTION'] == 0]
    stacked_bids = stack_unit_bids(volume_bids, price_bids)
    stacked_bids = adjust_bids_for_availability(stacked_bids, availability)

    demand_data = dynamic_data_compiler(start_time=start_time, end_time=end_time,
                                        raw_data_location=raw_data_cache,
                                        table_name='DISPATCHREGIONSUM', fformat=ffformat,
                                        select_columns=['REGIONID', 'TOTALDEMAND', 'SETTLEMENTDATE', 'INTERVENTION'])
    if regions:
        query = f"SELECT * FROM demand_data WHERE REGIONID = '{regions[0] + '1'}'"
        for region in regions[1:]:
            query += f" OR REGIONID = '{region + '1'}' "
        demand_data = sqldf(query, locals())
    demand_data = demand_data[demand_data['INTERVENTION'] == 0]
    demand_data = demand_data.loc[:, ['SETTLEMENTDATE', 'REGIONID', 'TOTALDEMAND']]

    complete_bid_stack_plot_bar = create_bid_stack_time_series_bar_plot(stacked_bids, demand_data, duration)
    return complete_bid_stack_plot_bar


def write_runtime_data(start_date, end_date, runtime_start, filename, ffformat):

    runtime_end = time.time()
    total_runtime = round((runtime_end - runtime_start) * 1000)
    with open(filename, 'a') as log:
        text = f"Bidding data from {start_date} to {end_date}\n"\
               f"Runtime ({ffformat} format): {total_runtime}\n\n"
        log.write(text)


if __name__ == '__main__':
    app.run()