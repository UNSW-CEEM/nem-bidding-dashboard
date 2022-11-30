"""
Functions to create the plots used in plot_bids.py
"""

from datetime import datetime, date, timedelta 
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from query_supabase import aggregate_bids, stations_and_duids_in_regions_and_time_window, region_data, duid_bids, get_aggregated_vwap


def get_duid_station_options(start_date: str, regions: list, duration: str) -> list:
    start_date_obj = datetime.strptime(start_date, "%Y/%m/%d %H:%M:%S")
    if duration == 'Daily':
        end_date = (start_date_obj + timedelta(days=1)).strftime("%Y/%m/%d %H:%M:%S")
        return stations_and_duids_in_regions_and_time_window(regions, start_date, end_date)
    if duration == 'Weekly':
        end_date = (start_date_obj + timedelta(days=7)).strftime("%Y/%m/%d %H:%M:%S")
        return stations_and_duids_in_regions_and_time_window(regions, start_date, end_date)


def plot_duid_bids(start_time: str, end_time: str, resolution: str, duids: list):
    """
    TODO:
        Adjust for multiple units
        Get all bid bands in legend
        Hover text showing price for bid band 
        Make sure it doesn't break if selected duid has no bids/adjust duid 
            options for each date range
        Make colour map consistent for each price band
    """

    stacked_bids = duid_bids(duids, start_time, end_time, resolution)
    # This creates a bug where selectin a duid with no bids means no individual 
    # duid bids will be able to be plotted subsequently
    # if stacked_bids.empty:
    #     print("DUID bids dataframe is empty")
    #     return None

    stacked_bids = stacked_bids.groupby(["INTERVAL_DATETIME", "BIDPRICE"], as_index=False).agg({"BIDVOLUME": "sum"})

    stacked_bids.sort_values(by=["BIDPRICE"], inplace=True)
    fig = px.bar(
        stacked_bids, 
        barmode="stack",
        x='INTERVAL_DATETIME', 
        y='BIDVOLUME', 
        color='BIDPRICE',
        labels={
            "BIDPRICE": "Bid Price", 
            'BIDVOLUME': "Bid Volume",
        },
        hover_data={
            'INTERVAL_DATETIME': True, 
            'BIDPRICE': ':.0f', 
            'BIDVOLUME': ':.0f',
        }, 
        custom_data=['BIDPRICE'],
    )


    fig.update_layout()
    fig.update_yaxes(title="Volume (MW)")
    if resolution == "hourly":
        fig.update_xaxes(title=f"Time (Bid stack sampled on the hour)")
    else:
        fig.update_xaxes(title=f"Time (Bid stack sampled at 5 min intervals)")

    fig.update_traces(
        hovertemplate="%{x}<br>Bid Price: $%{customdata[0]:.0f}<br>Bid Volume: %{y:.0f} MW"
    )
    
    return fig


def plot_aggregate_bids(start_time:str, end_time:str, resolution:str, regions:list, show_demand: bool):
    stacked_bids = aggregate_bids(regions, start_time, end_time, resolution)
    
    # if stacked_bids.empty:
    #     print("Aggregate bids dataframe is empty")
    #     return None

    stacked_bids = stacked_bids.groupby(["INTERVAL_DATETIME", "BIN_NAME"], as_index=False).agg({"BIDVOLUME": "sum"})
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


    fig = px.bar(
        stacked_bids, 
        x='INTERVAL_DATETIME', 
        y='BIDVOLUME', 
        category_orders={"BIN_NAME": bid_order},
        color='BIN_NAME',
        color_discrete_map=color_map,
        labels={
            "BIN_NAME": "Price Bin",
        },
    )

    # Update graph axes and hover text
    fig.update_yaxes(title="Volume (MW)")
    if resolution == "hourly":
        fig.update_xaxes(title=f"Time (Bid stack sampled on the hour)")
    else:
        fig.update_xaxes(title=f"Time (Bid stack sampled at 5 min intervals)")
    fig.update_traces(
        hovertemplate="%{x}<br>Bid Volume: %{y:.0f} MW<extra></extra>",
    )
    if show_demand:
        demand = region_data(start_time, end_time)
        demand.loc[:,"REGIONID"] = demand["REGIONID"].str[:-1]
        demand = demand[demand["REGIONID"].isin(regions)]
        demand = demand.groupby(["SETTLEMENTDATE"], as_index=False).agg({"TOTALDEMAND": "sum"})
        fig.add_trace(go.Scatter(x=demand['SETTLEMENTDATE'], y=demand['TOTALDEMAND'],
                                 marker=dict(color='blue', size=4), name='demand'))
        fig.update_traces(
            hovertemplate="%{x}<br>Demand: %{y:.0f} MW<extra></extra>",
            selector={"name": "demand"}
        )

    return fig


def plot_bids(start_time:str, end_time:str, resolution:str, regions:list, duids: list, show_demand: bool, show_price: bool):
    if (duids):
        fig = plot_duid_bids(start_time, end_time, resolution, duids)
    else: 
        fig = plot_aggregate_bids(start_time, end_time, resolution, regions, show_demand)
    fig.update_layout(height=400)
    
    if show_price:
        return add_price_subplot(fig, start_time, end_time, regions, resolution)
    return fig



def add_price_subplot(fig, start_date: str, end_date: str, regions: list, resolution: str):

    prices = get_aggregated_vwap(regions, start_date, end_date)
    prices = prices.sort_values(by="SETTLEMENTDATE")
    if resolution == "hourly":
        prices = prices[prices["SETTLEMENTDATE"].str.contains(":00:00")]

    price_graph = px.line(
        prices, 
        x = "SETTLEMENTDATE", 
        y = "PRICE", 
        labels={
            "SETTLEMENTDATE": "Time", 
            "PRICE": "Average electricity price ($/MWh)",
        }, 
        color_discrete_sequence=["red"]
    )
    bid_traces = []
    for i in range(len(fig["data"])):
        bid_traces.append(fig["data"][i])
    price_traces = []
    for i in range(len(price_graph["data"])):
        price_traces.append(price_graph["data"][i])

    plot = make_subplots(
        rows=2,
        cols=1, 
        shared_xaxes=True,
        row_heights=[0.66, 0.34],
        vertical_spacing=0.03,
    )
    for trace in bid_traces:
        plot.append_trace(trace, row=1, col=1)
    for trace in price_traces:
        plot.append_trace(trace, row=2, col=1)
    plot.update_layout(
        {"barmode": "stack", "height": 600},
        showlegend=True, 
        coloraxis_colorscale="Plasma",
    )
    plot.update_yaxes(
        title_text="Volume (MW)", 
        row=1, 
        col=1
    )
    plot.update_yaxes(
        title_text="Average electricity <br>price ($/MWh)",
        range=[0, 199],
        row=2, 
        col=1
    )
    if resolution == "hourly":
        plot.update_xaxes(title_text="Time (Bid stack sampled on the hour)", row=2, col=1)
    else:
        plot.update_xaxes(title_text="Time (Bid stack sampled at 5 min intervals)", row=2, col=1)
    
    return plot

    