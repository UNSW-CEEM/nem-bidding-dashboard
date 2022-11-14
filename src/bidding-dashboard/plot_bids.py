"""
TODO:
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
from query_supabase import aggregate_bids, demand_data, duid_data, duid_bids


cwd = os.path.dirname(__file__)
raw_data_cache = os.path.join(cwd, "nemosis_data_cache/")
ffformat = "parquet"

app = Dash(__name__)
app.title = "NEM Dashboard"



duid_options = sorted(duid_data()["duid"])
region_options = ["NSW", "VIC", "TAS", "SA", "QLD"]
title = "NEM Bidding Data"
settings_content = [
    html.Div(
        id="date-selector",
        children=[
            dcc.DatePickerSingle(
                id="start-date-picker",
                date=date(2019, 1, 21), 
                display_format="DD/MM/YY",
            ), 
            dcc.Dropdown(
                id="start-hour-picker",
                options=[f"{x:02}" for x in range(0, 25)],
                value="00"
            ),
            dcc.Dropdown(
                id="start-minute-picker",
                options=[f"{x:02}" for x in range(0, 61, 5)],
                value="00"
            ),
            dcc.RadioItems(
                id="duration-selector",
                options=["Daily", "Weekly"],
                value="Daily",
            )
        ]
    ),

    dcc.Checklist(
        id="region-checklist",
        options=region_options,
        value=region_options,
        inline=True,
    ),
    dcc.Dropdown(
        id="duid-dropdown", 
        value=None,
        options=duid_options,
        multi=True,
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
    Input("start-hour-picker", "value"),
    Input("start-minute-picker", "value"),
    Input("duration-selector", "value"),
    Input("region-checklist", "value"),
    Input("duid-dropdown", "value"),
    Input("update-graph-button", "n_clicks"))
def update(start_date: str, hour: str, minute: str, duration: str, regions: list, duids: list, num_clicks: int):
    trigger_id = dash.ctx.triggered_id
    if trigger_id and trigger_id != "update-graph-button":
        return dash.no_update
    # TODO: only update dataframe when required
    start_date = f"{start_date.replace('-', '/')} {hour}:{minute}:00"
    start_date_obj = datetime.strptime(start_date, "%Y/%m/%d %H:%M:%S")
    if (duration == "Daily"):
        end_date = (start_date_obj + timedelta(days=1)).strftime("%Y/%m/%d %H:%M:%S")
        resolution = "5-min"
    elif (duration == "Weekly"):
        end_date = (start_date_obj + timedelta(days=7)).strftime("%Y/%m/%d %H:%M:%S")
        resolution = "hourly"

    print(duids)
    fig = plot_bids(start_date, end_date, resolution, regions, duids)

    return fig




def plot_bids(start_time:str, end_time:str, resolution:str, regions:list, duids: list):

    if (duids):
        #stacked_bids = duid_bids(duids, start_time, end_time, resolution)
        stacked_bids = aggregate_bids(region_options, start_time, end_time, resolution)
        stacked_bids = stacked_bids[stacked_bids["duid"].isin(duids)]
        print(stacked_bids)
    else: 
        stacked_bids = aggregate_bids(regions, start_time, end_time, resolution)

    

    stacked_bids = stacked_bids.groupby(["interval_datetime", "bin_name"], as_index=False).agg({"bidvolume": "sum"})
    bid_order = [ 
        "[-1000, -100)", 
        "[-100, 0)", 
        "[0, 50)", 
        "[50, 100)", 
        "[100, 200)", 
        "[200, 300)", 
        "[300, 500)", 
        "[500, 1000)", 
        "[1000, 5000)", 
        "[5000, 10000)", 
        "[10000, 15500)", 
    ]

    color_map = {
        "[-1000, -100)": "lightsalmon", 
        "[-100, 0)": "yellow",
        "[0, 50)": "red", 
        "[50, 100)": "orange", 
        "[100, 200)": "#00CC96", 
        "[200, 300)": "#636efa", 
        "[300, 500)": "purple", 
        "[500, 1000)": "cyan", 
        "[1000, 5000)": "fuchsia", 
        "[5000, 10000)": "palegreen", 
        "[10000, 15500)": "lightblue", 
    }

    demand = demand_data(start_time, end_time)
    demand.loc[:,"regionid"] = demand["regionid"].str[:-1]
    demand = demand[demand["regionid"].isin(regions)]
    demand = demand.groupby(["settlementdate"], as_index=False).agg({"totaldemand": "sum"})

    fig = px.bar(
        stacked_bids, 
        x='interval_datetime', 
        y='bidvolume', 
        category_orders={"bin_name": bid_order},
        color='bin_name',
        color_discrete_map=color_map,
    )
    fig.add_trace(go.Scatter(x=demand['settlementdate'], y=demand['totaldemand'],
                             marker=dict(color='blue', size=4), name='demand'))

    # Update graph axes and hover text
    fig.update_yaxes(title="Volume (MW)")
    if resolution == "hourly":
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


if __name__ == '__main__':
    app.run()