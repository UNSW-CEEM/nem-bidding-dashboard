"""
TODO:
    Aggregate by DUID
    Documentation
"""


from datetime import datetime, date, timedelta 
import layout_template

import plotly.express as px
import plotly.graph_objects as go
from dash import Dash, dcc, html, Input, Output
import dash
from query_supabase import aggregate_bids, stations_and_duids_in_regions_and_time_window, region_data, duid_bids




def get_duid_station_options(start_date: str, regions: list, duration: str) -> list:
    start_date_obj = datetime.strptime(start_date, "%Y/%m/%d %H:%M:%S")
    if duration == 'Daily':
        end_date = (start_date_obj + timedelta(days=1)).strftime("%Y/%m/%d %H:%M:%S")
        return stations_and_duids_in_regions_and_time_window(regions, start_date, end_date)
    if duration == 'Weekly':
        end_date = (start_date_obj + timedelta(days=7)).strftime("%Y/%m/%d %H:%M:%S")
        return stations_and_duids_in_regions_and_time_window(regions, start_date, end_date)
    


app = Dash(__name__)
app.title = "NEM Dashboard"


title = "NEM Bidding Data"

region_options = ["NSW", "VIC", "TAS", "SA", "QLD"]
initial_regions = region_options
initial_start_date_obj = date(2020, 1, 21)
initial_start_date_str = initial_start_date_obj.strftime("%Y/%m/%d %H:%M:%S")
initial_duration = "Daily"

duid_station_options = get_duid_station_options(initial_start_date_str, initial_regions, initial_duration)
duid_options = sorted(duid_station_options["DUID"])
station_options = sorted(list(set(duid_station_options["STATION NAME"])))
settings_content = [
    html.Div(
        id="date-selector",
        children=[
            html.H6("Select Time Period", className="selector-title"),
            html.Div(
                id="datetime-picker", 
                children=[
                    dcc.DatePickerSingle(
                        id="start-date-picker",
                        date=initial_start_date_obj, 
                        display_format="DD/MM/YY",
                    ), 
                    dcc.Dropdown(
                        className="start-time-picker",
                        id="start-hour-picker",
                        options=[f"{x:02}" for x in range(0, 25)],
                        value="00", 
                        clearable=False,
                    ),
                    dcc.Dropdown(
                        className="start-time-picker",
                        id="start-minute-picker",
                        options=[f"{x:02}" for x in range(0, 61, 5)],
                        value="00",
                        clearable=False,
                    ),
                ]
            ),
            dcc.RadioItems(
                id="duration-selector",
                options=["Daily", "Weekly"],
                value=initial_duration,
                inline=True,
            )
        ]
    ),
    html.Div(
        id="region-div",
        children=[
            html.H6("Select Region", className="selector-title"),
            dcc.Checklist(
                id="region-checklist",
                options=region_options,
                value=initial_regions,
                inline=True,
            ),
            
        ]
    ),
    html.Div(
        id="duid-div", 
        children=[
            html.H6("Select Units by DUID", className="selector-title"),
            dcc.Dropdown(
                id="duid-dropdown", 
                value=None,
                options=duid_options,
                multi=True,
            ),
            dcc.Dropdown(
                id="station-dropdown", 
                value=None,
                options=station_options,
                multi=False,
            ),
        ]
    ),
    html.Div(
        id="show-demand-div",
        children=[
            html.H6("Demand Curve", className="selector-title"),
            dcc.RadioItems(
                id="show-demand-checkbox", 
                options=['Show', 'Hide'], 
                value="Show",
            ),
        ]
    )

]
graph_content = dcc.Graph(id="graph")
app.layout = layout_template.build(title, settings_content, graph_content)

#@app.callback(
#    Output("duid-dropdown", "options"),
#    Input("start-date-picker", "date"),
#    Input("start-hour-picker", "value"),
#    Input("start-minute-picker", "value"),
#    Input("duration-selector", "value"),
#    Input("station-dropdown", "value"),
#    Input("region-checklist", "value"))
#def update_duids_from_date_region(start_date: str, hour: str, minute: str, duration: str, station: str, regions: list):
#    print("DUID options callback executed")
#    start_date = f"{start_date.replace('-', '/')} {hour}:{minute}:00"
#    duid_options = get_duid_station_options(start_date, regions, duration)
#    if station:
#        print(station)
#        print(duid_options["STATION NAME"])
#        duid_options = duid_options.loc[duid_options["STATION NAME"] == station]
#        duid_options = sorted(duid_options["DUID"])
#    else:
#        duid_options = sorted(duid_options["DUID"])
#    print("\n\n\n")
#    print(f"Amount of duid options: {len(duid_options)}")
#    print("\n\n\n")
#    return duid_options
#

@app.callback(
    Output("duid-dropdown", "value"),
    Output("station-dropdown", "value"),
    Input("start-date-picker", "date"),
    Input("start-hour-picker", "value"),
    Input("start-minute-picker", "value"),
    Input("duration-selector", "value"),
    Input("station-dropdown", "value"),
    Input("duid-dropdown", "value"),
    Input("region-checklist", "value"))
def update_duids_from_date_region(start_date: str, hour: str, minute: str, duration: str, station: str, duids: list, regions: list):
    print("DUID station options callback executed")
    print(dash.ctx.triggered)

    trigger_id = dash.ctx.triggered_id
    # if trigger_id and trigger_id == "duid-dropdown":
    #     return dash.no_update, None

    if trigger_id and trigger_id not in ["station-dropdown", "duid-dropdown"]:
        print("Not updating duid options")
        return dash.no_update, dash.no_update

    start_date = f"{start_date.replace('-', '/')} {hour}:{minute}:00"
    duid_options = get_duid_station_options(start_date, regions, duration)
    if station:
        print(station)
        print(duid_options["STATION NAME"])
        duid_options = duid_options.loc[duid_options["STATION NAME"] == station]
        duid_options = sorted(duid_options["DUID"])
    else:
        return dash.no_update, None
    
    if duids and sorted(duids) != duid_options:
        return duid_options, None


    print(f"Amount of duid options: {len(duid_options)}")
    return duid_options, dash.no_update

# @app.callback(
#     Output("station-dropdown", "value"),
#     Input("duid-dropdown", "value"))
# def remove_station_from_duid_update(duids:list):
#     print("remove station")
#     return None

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
    Input("show-demand-checkbox", "value"),
    Input("update-graph-button", "n_clicks"))
def update(start_date: str, hour: str, minute: str, duration: str, regions: list, duids: list, demand_checkbox: str, num_clicks: int):
    print("Regular callback executed")
    start_date = f"{start_date.replace('-', '/')} {hour}:{minute}:00"
    start_date_obj = datetime.strptime(start_date, "%Y/%m/%d %H:%M:%S")
    if (duration == "Daily"):
        end_date = (start_date_obj + timedelta(days=1)).strftime("%Y/%m/%d %H:%M:%S")
        resolution = "5-min"
    elif (duration == "Weekly"):
        end_date = (start_date_obj + timedelta(days=7)).strftime("%Y/%m/%d %H:%M:%S")
        resolution = "hourly"

    #trigger_id = dash.ctx.triggered_id
    #if trigger_id and trigger_id != "update-graph-button":
    #    return dash.no_update

    show_demand = True if demand_checkbox == "Show" else False
    fig = plot_bids(start_date, end_date, resolution, regions, duids, show_demand)

    return fig


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
    if stacked_bids.empty:
        print("DUID bids dataframe is empty")
        return None

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
    
    if stacked_bids.empty:
        print("Aggregate bids dataframe is empty")
        return None

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

    demand = region_data(start_time, end_time)
    demand.loc[:,"REGIONID"] = demand["REGIONID"].str[:-1]
    demand = demand[demand["REGIONID"].isin(regions)]
    demand = demand.groupby(["SETTLEMENTDATE"], as_index=False).agg({"TOTALDEMAND": "sum"})

    fig = px.bar(
        stacked_bids, 
        x='INTERVAL_DATETIME', 
        y='BIDVOLUME', 
        category_orders={"BIN_NAME": bid_order},
        color='BIN_NAME',
        color_discrete_map=color_map,
        labels={
            "BIN_NAME": "Price Bin",
        }
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
        fig.add_trace(go.Scatter(x=demand['SETTLEMENTDATE'], y=demand['TOTALDEMAND'],
                                 marker=dict(color='blue', size=4), name='demand'))
        fig.update_traces(
            hovertemplate="%{x}<br>Demand: %{y:.0f} MW<extra></extra>",
            selector={"name": "demand"}
        )

    return fig


def plot_bids(start_time:str, end_time:str, resolution:str, regions:list, duids: list, show_demand: bool):
    if (duids):
        return plot_duid_bids(start_time, end_time, resolution, duids)
    else: 
        return plot_aggregate_bids(start_time, end_time, resolution, regions, show_demand)




if __name__ == '__main__':
    app.run()