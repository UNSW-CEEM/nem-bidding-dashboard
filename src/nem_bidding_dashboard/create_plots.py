"""
Functions here are used to create plots for bids, demand and price. They are
used in the app callbacks in plot_bids.py.
"""

from datetime import datetime, timedelta
from typing import List

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.graph_objects import Figure
from plotly.subplots import make_subplots

from nem_bidding_dashboard import defaults, query_functions_for_dashboard

DISPATCH_COLUMNS = {
    "Availability": {"name": "AVAILABILITY", "color": "red"},
    "Dispatch Volume": {"name": "TOTALCLEARED", "color": "green"},
    "Final MW": {"name": "FINALMW", "color": "cyan"},
    "As Bid Ramp Up Max Availability": {
        "name": "ASBIDRAMPUPMAXAVAIL",
        "color": "magenta",
    },
    "As Bid Ramp Down Min Availability": {
        "name": "ASBIDRAMPDOWNMINAVAIL",
        "color": "violet",
    },
    "Ramp Up Max Avail": {"name": "RAMPUPMAXAVAIL", "color": "lightgrey"},
    "Ramp Down Min Avail": {"name": "RAMPDOWNMINAVAIL", "color": "crimson"},
    "PASA Availability": {"name": "PASAAVAILABILITY", "color": "hotpink"},
    "Max Availability": {"name": "MAXAVAIL", "color": "brown"},
}


def get_duid_station_options(
    start_time: str,
    regions: List[str],
    duration: str,
    tech_types: List[str] = [],
    dispatch_type: str = "Generator",
) -> pd.DataFrame:
    """
    TODO
    Gets the duids and corresponding station names of all units that are
    based within the given regions and have made bids during the specified
    timeframe, as well as station names of all stations that include such units.

    Arguments:
        start_time: Initial datetime in the format 'YYYY/MM/DD HH:MM:SS'
        regions: List of specified regions
        duration: Duration of time period. Either 'Daily' or 'Weekly'
    Returns:
        Dataframe - column 'DUID' contains unit duids, column 'STATION NAME'
            contains name of station that each unit belongs to
    """
    start_time_obj = datetime.strptime(start_time, "%Y/%m/%d %H:%M:%S")
    if duration == "Daily":
        end_time = (start_time_obj + timedelta(days=1)).strftime("%Y/%m/%d %H:%M:%S")

        return (
            query_functions_for_dashboard.stations_and_duids_in_regions_and_time_window(
                start_time, end_time, regions, dispatch_type, tech_types
            )
        )
    if duration == "Weekly":
        end_time = (start_time_obj + timedelta(days=7)).strftime("%Y/%m/%d %H:%M:%S")

        return (
            query_functions_for_dashboard.stations_and_duids_in_regions_and_time_window(
                start_time, end_time, regions, dispatch_type, tech_types
            )
        )


def adjust_fig_layout(fig: Figure) -> Figure:
    """
    Adjusts the layout for a figure. Reduces the top margin of the given figure.

    Arguments:
        fig: Plotly express/plotly go figure to format
    Returns:
        Formatted figure
    """
    fig.update_layout(margin={"t": 20})
    return fig


def get_graph_name(duids: List[str]):
    """
    Returns graph name based on the data being presented.
    """
    if duids:
        return "Selected Units' Bidstack"
    else:
        return "Aggregated Bids"


def plot_bids(
    start_time: str,
    end_time: str,
    resolution: str,
    regions: List[str],
    duids: List[str],
    show_demand: bool,
    show_demand_lower: bool,
    show_price: bool,
    raw_adjusted: str,
    tech_types: List[str],
    dispatch_type: bool,
    dispatch_metrics: List[str],
    color_scheme: str,
) -> Figure:
    """
    Plots volume bids over time based on the given parameters. See
    plot_duid_bids, plot_aggregate_bids and add_price_subplot for more info.

    Arguments:
        start_time: Initial datetime in the format 'YYYY/MM/DD HH:MM:SS'
        end_time: Ending datetime, formatted identical to start_time
        resolution: Either 'hourly' or '5-min'
        regions: list of specified regions
        duids: list of specified unit duids. If this empty, only the specified
            duid bids will be plotted, otherwise aggregate bids for the given
            timeframe and regions will be plotted
        show_demand: True if electricity demand is to be plotted over figure,
            otherwise False
        show_demand_lower: True if electricity demand is to be plotted over lower figure,
            otherwise False
        show_price: Bool. If True, the figure returned will consist of 2
            subplots, 1 showing bids and 1 showing average electricity prices
            over the same timeframe. If False, only bids will be plotted.
        raw_adjusted: Determines whether to show raw or availability adjusted
            bids. Either 'raw' or 'adjusted'
        tech_types: List of unit types to show bidding data for
        dispatch_type: Either 'Generator' or 'Load'
        dispatch_metrics: List of dispatch metrics to plot on main graph
        color_scheme: Name of the color scheme to use
    Returns:
            Plotly express figure if show_price is False. Plotly go subplot if
            show_price is True.
    """
    if duids:
        fig = plot_duid_bids(
            start_time, end_time, resolution, duids, raw_adjusted, dispatch_metrics
        )
    else:
        fig = plot_aggregate_bids(
            start_time,
            end_time,
            resolution,
            regions,
            show_demand,
            raw_adjusted,
            tech_types,
            dispatch_type,
            dispatch_metrics,
            color_scheme,
        )

    if not fig:
        return None

    if show_price:
        fig = add_price_subplot(
            fig, start_time, end_time, regions, resolution, show_demand_lower
        )

    if not duids:
        fig.update_layout(hovermode="x unified")
        fig.update_traces(xaxis="x1", row=1)
    else:
        fig.update_layout(
            coloraxis_colorscale=defaults.continuous_color_scales[color_scheme],
            coloraxis_cmin=defaults.market_price_floor,
            coloraxis_cmax=defaults.market_price_cap,
        )
        update_colorbar_length(fig)

    return fig


def plot_duid_bids(
    start_time: str,
    end_time: str,
    resolution: str,
    duids: List[str],
    raw_adjusted: str,
    dispatch_metrics: List[str],
) -> Figure:
    """
    Plots a stacked bar chart showing the bid volumes for each unit specified in
    the 'duids' list.

    x-axis: Bid volume. Stacked bars sorted by bid price in ascending order.
    y-axis: Datetime. Bids will be displayed at 5 min intervals if time period
        is 1 day or hourly intervals if time period is 1 week.
    Colour: Bid price for each volume bid.

    Arguments:
        start_time: Initial datetime in the format 'YYYY/MM/DD HH:MM:SS'
        end_time: Ending datetime, formatted identical to start_time
        resolution: Either 'hourly' or '5-min
        duids: list of unit duids
        raw_adjusted: Determines whether to plot raw or availability adjusted
            bids. Either 'raw' or 'adjusted'. Not implemented yet.
        dispatch_metrics: List of dispatch metrics to plot over graph
    Returns:
        Plotly express figure (stacked bar chart)
    BUG: overlap in legend entry when plotting with duid bids
    TODO: raw_adjusted not implemented in query_supabase.duid_bids yet, bids
        can be plotted as raw or adjusted using 'raw_adjusted' argument
    """
    stacked_bids = query_functions_for_dashboard.duid_bids(
        start_time, end_time, duids, resolution, raw_adjusted
    )
    if stacked_bids.empty:
        return None
    stacked_bids = stacked_bids.groupby(
        ["INTERVAL_DATETIME", "BIDPRICE"], as_index=False
    ).agg({"BIDVOLUME": "sum"})

    stacked_bids.sort_values(by=["BIDPRICE"], inplace=True)
    fig = px.bar(
        stacked_bids,
        barmode="stack",
        x="INTERVAL_DATETIME",
        y="BIDVOLUME",
        color="BIDPRICE",
        labels={
            "BIDPRICE": "Bid Price",
            "BIDVOLUME": "Bid Volume",
        },
        hover_data={
            "INTERVAL_DATETIME": True,
            "BIDPRICE": ":.0f",
            "BIDVOLUME": ":.0f",
        },
        custom_data=["BIDPRICE"],
    )

    fig.update_yaxes(title="Volume (MW)")
    fig.update_traces(
        hovertemplate="%{x}<br>Bid Price: $%{customdata[0]:.0f}<br>Bid Volume: %{y:.0f} MW"
    )

    if resolution == "hourly":
        fig.update_xaxes(
            title="Time (Bid stack for dispatch intervals ending on the hour)"
        )
    else:
        fig.update_xaxes(title="Time (Bid stack sampled at 5 min intervals)")

    if dispatch_metrics:
        fig = add_duid_dispatch_data(
            fig, duids, start_time, end_time, resolution, dispatch_metrics
        )

    return fig


def add_duid_dispatch_data(
    fig: Figure,
    duids: List[str],
    start_time: str,
    end_time: str,
    resolution: str,
    dispatch_metrics: List[str],
) -> Figure:
    """
    Plots the selected dispatch metrics on the figure given. Dispatch metrics
    are plotted for the specific duids selected. Relies on
    query_supabase.get_aggregated_dispatch_data_by_duids.

    Arguments:
        fig: plotly figure to add traces to. Currently plots duid bids.
        duids: List of duids to get dispatch data for
        start_time: Initial datetime in the format "YYYY/MM/DD HH:MM:SS"
        end_time: Ending datetime, formatted identical to start_time
        resolution: Either 'hourly' or '5-min'
        dispatch_metrics: List of dispatch metrics to plot over the main graph
    Returns:
        Plotly figure consisting of 'fig' with the given dispatch metrics
            plotted over it
    """
    for metric in dispatch_metrics:
        dispatch_data = query_functions_for_dashboard.aggregated_dispatch_data_by_duids(
            DISPATCH_COLUMNS[metric]["name"],
            start_time,
            end_time,
            duids,
            resolution,
        )
        dispatch_data = dispatch_data.sort_values(by=["INTERVAL_DATETIME"])
        fig.add_trace(
            go.Scatter(
                x=dispatch_data["INTERVAL_DATETIME"],
                y=dispatch_data["COLUMNVALUES"],
                marker=dict(color=DISPATCH_COLUMNS[metric]["color"], size=4),
                name=metric,
                legendgroup="dispatch_traces",
                legendgrouptitle_text="Dispatch Data",
            )
        )
        fig.update_traces(
            hovertemplate=metric + ": %{y:.0f} MW<extra></extra>",
            selector={"name": metric},
        )
    return fig


def plot_aggregate_bids(
    start_time: str,
    end_time: str,
    resolution: str,
    regions: List[str],
    show_demand: bool,
    raw_adjusted: str,
    tech_types: List[str],
    dispatch_type: str,
    dispatch_metrics: List[str],
    color_scheme: str,
) -> Figure:
    """
    Plots a stacked bar chart showing the aggregate bids for the specified
    regions grouped into a set of predefined bins. If show_demand is True, total
    electricity demand for all specified regions is plotted on top of the
    aggregated bids. Relies on query_supabase.aggregate_bids.

    x-axis: Bid volume. Stacked bars sorted from lowest price bin to highest.
    y-axis: Datetime. Bids will be displayed at 5 min intervals if time period
        is 1 day or hourly intervals if time period is 1 week.
    Colour: Price bin that each bid's price falls into.

    Arguments:
        start_time: Initial datetime in the format 'YYYY/MM/DD HH:MM:SS'
        end_time: Ending datetime, formatted identical to start_time
        resolution: Either 'hourly' or '5-min'
        regions: list of specified regions
        show_demand: True if electricity demand is to be plotted over figure,
            otherwise False
        raw_adjusted: Determines whether to plot raw or availability adjusted
            bids. Either 'raw' or 'adjusted'. Not implemented yet.
        tech_types: List of unit types to show bidding data for
        dispatch_type: Either 'Generator' or 'Load'
        dispatch_metrics: List of dispatch metrics to plot over graph
        color_scheme: name of the color scheme to use.
    Returns:
        Plotly express figure (stacked bar chart)
    """
    if tech_types is None:
        tech_types = []

    stacked_bids = query_functions_for_dashboard.aggregate_bids(
        start_time,
        end_time,
        regions,
        dispatch_type,
        tech_types,
        resolution,
        raw_adjusted,
    )

    if stacked_bids.empty:
        return None

    color_map = {}
    for i in range(len(defaults.bid_order)):
        color_map[defaults.bid_order[i]] = defaults.discrete_color_scale[color_scheme][
            i
        ]

    fig = px.bar(
        stacked_bids,
        x="INTERVAL_DATETIME",
        y="BIDVOLUME",
        category_orders={"BIN_NAME": defaults.bid_order},
        color="BIN_NAME",
        color_discrete_map=color_map,
        labels={"BIN_NAME": "Bid Price ($/MW/h)", "PRICE": "Average Electricity Price"},
        custom_data=["BIN_NAME"],
    )

    # Update graph axes and hover text
    fig.update_yaxes(title="Volume (MW)")
    fig.update_traces(
        hovertemplate="Price range %{customdata[0]}: %{y:.0f} MW<extra></extra>"
    )

    if resolution == "hourly":
        fig.update_xaxes(
            title="Time (Bid stack for dispatch intervals ending on the hour)"
        )
    else:
        fig.update_xaxes(title="Time (Bid stack for each 5 min dispatch interval)")

    if show_demand:
        fig = add_demand_trace(fig, start_time, end_time, regions)

    if dispatch_metrics:
        fig = add_region_dispatch_data(
            fig,
            regions,
            start_time,
            end_time,
            resolution,
            dispatch_type,
            tech_types,
            dispatch_metrics,
        )

    return fig


def add_demand_trace(
    fig: Figure, start_time: str, end_time: str, regions: List[str]
) -> Figure:
    """
    Adds the line plot showing electricity demand to an existing figure. Plots
    total demand over the given time period for the specified regions. Relies on
    query_supabase.region_data.

    x-axis: Volume of electricity demand (MW).
    y-axis: Datetime. Bids will be displayed at 5 min intervals if time period
        is 1 day or hourly intervals if time period is 1 week.

    Arguments:
        fig: plotly express figure
        start_time: Initial datetime in the format "YYYY/MM/DD HH:MM:SS"
        end_time: Ending datetime, formatted identical to start_time
        regions: list of specified regions
    Returns:
        Updated plotly express figure consisting of the electricity demand curve
        plotted on top of the original figure
    """
    demand = query_functions_for_dashboard.region_demand(start_time, end_time, regions)
    demand = demand.sort_values("SETTLEMENTDATE")
    fig.add_trace(
        go.Scatter(
            x=demand["SETTLEMENTDATE"],
            y=demand["TOTALDEMAND"],
            marker=dict(color="blue", size=4),
            name="Demand",
            legendgroup="dispatch_traces",
            legendgrouptitle_text="Dispatch Data",
        ),
    )
    fig.update_traces(
        hovertemplate="Demand: %{y:.0f} MW<extra></extra>", selector={"name": "Demand"}
    )
    return fig


def add_region_dispatch_data(
    fig: Figure,
    regions: List[str],
    start_time: str,
    end_time: str,
    resolution: str,
    dispatch_type: str,
    tech_types: List[str],
    dispatch_metrics: List[str],
) -> Figure:
    """
    Plots the selected dispatch metrics on the figure given. Dispatch metrics
    are plotted by region. Relies on query_supabase.get_aggregated_dispatch_data.


    Arguments:
        fig: plotly figure to add traces to. Currently plots aggregate bids by
            region
        regions: list of regions to get dispatch data for
        start_time: Initial datetime in the format "YYYY/MM/DD HH:MM:SS"
        end_time: Ending datetime, formatted identical to start_time
        resolution: Either 'hourly' or '5-min'
        dispatch_metrics: List of dispatch metrics to plot over the main graph
    Returns:
        Plotly figure consisting of 'fig' with the given dispatch metrics
            plotted over it
    """
    if tech_types is None:
        tech_types = []
    for metric in dispatch_metrics:
        dispatch_data = query_functions_for_dashboard.aggregated_dispatch_data(
            DISPATCH_COLUMNS[metric]["name"],
            start_time,
            end_time,
            regions,
            dispatch_type,
            tech_types,
            resolution,
        )
        dispatch_data = dispatch_data.sort_values(by=["INTERVAL_DATETIME"])
        fig.add_trace(
            go.Scatter(
                x=dispatch_data["INTERVAL_DATETIME"],
                y=dispatch_data["COLUMNVALUES"],
                marker=dict(color=DISPATCH_COLUMNS[metric]["color"], size=4),
                name=metric,
                legendgroup="dispatch_traces",
                legendgrouptitle_text="Dispatch Data",
            )
        )
        fig.update_traces(
            hovertemplate=metric + ": %{y:.0f} MW<extra></extra>",
            selector={"name": metric},
        )
    return fig


def add_price_subplot(
    fig: Figure,
    start_time: str,
    end_time: str,
    regions: List[str],
    resolution: str,
    show_demand_lower: bool,
) -> Figure:
    """
    Takes a plotly express figure plotting bids and puts it into a plotly go
    subplot consisting of the original figure on top with the average
    electricity price plotted below. The figures share their x-axis (datetime).
    The data from the original figure is removed and appended to a plotly go
    figure so it will be compatible with the subplot (plotly express does not
    support subplots). Electricity price is first plotted as a plotly express
    figure, the data is removed and appended to the subplot.
    See plot_price for more info.

    Arguments:
        fig: plotly express figure
        start_time: Initial datetime in the format 'YYYY/MM/DD HH:MM:SS'
        end_time: Ending datetime, formatted identical to start_time
        regions: list of specified regions
        resolution: Either 'hourly' or '5-min'
        show_demand_lower: True if demand is to be plotted as well otherwise False
    Returns:
        plotly go subplot containing original bid plot on top and electricity
        price below
    BUG: x-axis time ticks seem to disappear, I think in an earlier version they
        worked fine
    """
    price_graph = plot_price(start_time, end_time, regions, resolution)

    plot = make_subplots(
        rows=2,
        cols=1,
        shared_xaxes=True,
        row_heights=[0.66, 0.34],
        vertical_spacing=0.0,
        specs=[[{}], [{"secondary_y": True}]],
    )

    bid_traces = []
    for i in range(len(fig["data"])):
        bid_traces.append(fig["data"][i])
    for trace in bid_traces:
        if trace.legendgroup != "dispatch_traces":
            trace.legendgroup = "bid_traces"
            trace.legendgrouptitle = {"text": "Bid Price ($/MW/h)"}
        plot.add_trace(trace, row=1, col=1)

    price_trace = price_graph["data"][0]
    price_trace["showlegend"] = True
    price_trace["name"] = "Price"
    plot.add_trace(price_trace, row=2, col=1)

    if show_demand_lower:
        add_demand_trace_to_lower_plot(plot, start_time, end_time, regions)

    plot.update_layout(
        {"barmode": "stack"},
    )
    plot.update_yaxes(title_text="Volume (MW)", row=1, col=1)
    plot.update_yaxes(title_text="Average electricity<br>price ($/MW/h)", row=2, col=1)
    plot.update_yaxes(title_text="Volume (MW)", row=2, col=1, secondary_y=True)
    plot.update_layout(coloraxis_colorbar_title="Bid Price ($/MW/h)")
    if resolution == "hourly":
        plot.update_xaxes(
            title_text="Time (Bid stack for dispatch intervals ending on the hour)",
            visible=True,
            row=2,
            col=1,
        )
    else:
        plot.update_xaxes(
            title_text="Time (Bid stack for each 5 min dispatch interval)",
            visible=True,
            row=2,
            col=1,
        )
    return plot


def add_demand_trace_to_lower_plot(plot, start_time, end_time, regions):
    demand = query_functions_for_dashboard.region_demand(start_time, end_time, regions)
    demand = demand.sort_values("SETTLEMENTDATE")
    plot.add_trace(
        go.Scatter(
            x=demand["SETTLEMENTDATE"],
            y=demand["TOTALDEMAND"],
            marker=dict(color="blue", size=4),
            name="Demand on secondary plot",
            legendgroup="dispatch_traces",
            legendgrouptitle_text="Dispatch Data",
        ),
        row=2,
        col=1,
        secondary_y=True,
    )


def plot_price(
    start_time: str, end_time: str, regions: List[str], resolution: str
) -> Figure:
    """
    Plots aggregated Volume Weighted Average Price against time. Price is
    averaged across the specified regions over the given timeframe. Relies on
    query_supabase.get_aggregated_vwap.

    x-axis: Volume weighted average price ($/MWh)
    y-axis: Datetime, shared with bid plot. Displayed at 5 min or hourly
        intervals based on the given resolution.

    Arguments:
        start_time: Initial datetime in the format 'YYYY/MM/DD HH:MM:SS'
        end_time: Ending datetime, formatted identical to start_time
        regions: list of specified regions
        resolution: Either 'hourly' or '5-min'

    """
    prices = query_functions_for_dashboard.aggregated_vwap(
        start_time, end_time, regions
    )
    prices = prices.sort_values(by="SETTLEMENTDATE")

    price_graph = px.line(
        prices,
        x="SETTLEMENTDATE",
        y="PRICE",
        labels={
            "SETTLEMENTDATE": "Time",
            "PRICE": "Average electricity price ($/MWh)",
        },
        color_discrete_sequence=["red"],
    )
    price_graph.update_traces(
        hovertemplate="%{x}<br>Average electricity price: $%{y:.2f}/MWh<extra></extra>"
    )
    return price_graph


def update_colorbar_length(fig):
    c = 0
    for trace in fig.data:
        if trace.visible is True or trace.visible is None:
            c += 1
    c = min(c, 10)
    fig.update_layout(coloraxis_colorbar_len=0.8 - c * 0.07)
