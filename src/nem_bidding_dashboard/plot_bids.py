from datetime import datetime, date, timedelta 
from dash import Dash, dcc, html, Input, Output

import layout_template
from create_plots import *


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
            html.H6("Other Metrics", className="selector-title"),
            dcc.Checklist(
                id="price-demand-checkbox", 
                options=['Demand', 'Price'], 
                value=["Demand", "Price"],
            ),
        ]
    ) 
]
graph_content = dcc.Graph(id="graph")
app.layout = layout_template.build(title, settings_content, graph_content)


# @app.callback(
#     Output("duid-dropdown", "value"),
#     Output("station-dropdown", "value"),
#     Input("start-date-picker", "date"),
#     Input("start-hour-picker", "value"),
#     Input("start-minute-picker", "value"),
#     Input("duration-selector", "value"),
#     Input("station-dropdown", "value"),
#     Input("duid-dropdown", "value"),
#     Input("region-checklist", "value"))
# def update_duids_from_date_region(start_date: str, hour: str, minute: str, duration: str, station: str, duids: list, regions: list):
#     print("DUID station options callback executed")
#     print(dash.ctx.triggered)

#     trigger_id = dash.ctx.triggered_id
#     # if trigger_id and trigger_id == "duid-dropdown":
#     #     return dash.no_update, None

#     if trigger_id and trigger_id not in ["station-dropdown", "duid-dropdown"]:
#         print("Not updating duid options")
#         return dash.no_update, dash.no_update

#     start_date = f"{start_date.replace('-', '/')} {hour}:{minute}:00"
#     duid_options = get_duid_station_options(start_date, regions, duration)
#     if station:
#         print(station)
#         print(duid_options["STATION NAME"])
#         duid_options = duid_options.loc[duid_options["STATION NAME"] == station]
#         duid_options = sorted(duid_options["DUID"])
#     else:
#         return dash.no_update, None
    
#     if duids and sorted(duids) != duid_options:
#         return duid_options, None


    # print(f"Amount of duid options: {len(duid_options)}")
    # return duid_options, dash.no_update
     
     
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
    Input("price-demand-checkbox", "value"))
def update(start_date: str, hour: str, minute: str, duration: str, regions: list, duids: list, price_demand_checkbox: str):
    start_date = f"{start_date.replace('-', '/')} {hour}:{minute}:00"
    start_date_obj = datetime.strptime(start_date, "%Y/%m/%d %H:%M:%S")
    if (duration == "Daily"):
        end_date = (start_date_obj + timedelta(days=1)).strftime("%Y/%m/%d %H:%M:%S")
        resolution = "5-min"
    elif (duration == "Weekly"):
        end_date = (start_date_obj + timedelta(days=7)).strftime("%Y/%m/%d %H:%M:%S")
        resolution = "hourly"

    show_demand = "Demand" in price_demand_checkbox
    show_price = "Price" in price_demand_checkbox
    fig = plot_bids(start_date, end_date, resolution, regions, duids, show_demand, show_price)
    return fig


if __name__ == '__main__':
    app.run(debug=True)