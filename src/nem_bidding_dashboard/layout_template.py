"""
TODO:
    Pages and links
    Any other GUI elements, aesthetics
    Documentation
"""

import dash_loading_spinners as dls
import plotly.graph_objects as go
from create_plots import DISPATCH_COLUMNS
from dash import dcc, html


def build_banner():
    return html.Div(
        id="banner",
        className="banner",
        children=[
            html.Div(
                id="banner-text",
                children=[
                    html.H5("NEM Bidding Dashboard (BETA TESTING VERSION)"),
                    html.A(
                        "Project page",
                        href="https://github.com/UNSW-CEEM/nem-bidding-dashboard",
                        target="_blank",
                        style={"color": "black", "margin-left": "10px"},
                    ),
                    html.A(
                        "License and disclaimer",
                        href="https://github.com/UNSW-CEEM/nem-bidding-dashboard/blob/master/LICENSE",
                        target="_blank",
                        style={"color": "black", "margin-left": "10px"},
                    ),
                    html.A(
                        "Funded by DGFI",
                        href="https://www.dgfi.unsw.edu.au/",
                        target="_blank",
                        style={"color": "black", "margin-left": "10px"},
                    ),
                ],
            ),
        ],
    )


def how_to_use():
    return """
This dashboard plots the aggregate bids made in the National Electricity Market (NEM).
This data can be filtered by datetime, and will either be shown in daily periods
(at 5-min resolution) or weekly periods (at hourly resolution).
...
"""


def get_settings_content(
    duid_options, station_options, tech_type_options, region_options, initial_regions
):
    settings_content = [
        html.Div(
            id="tech-type-div",
            children=[
                html.H6("Regions", className="selector-title"),
                dcc.Checklist(
                    id="region-checklist",
                    options=region_options,
                    value=initial_regions,
                    inline=True,
                ),
                html.H6("Unit Type", className="selector-title"),
                dcc.Dropdown(
                    id="tech-type-dropdown",
                    options=tech_type_options,
                    value=None,
                    multi=True,
                ),
            ],
        ),
        html.Div(
            id="duid-div",
            children=[
                html.H6("Select Units by Station", className="selector-title"),
                dcc.Dropdown(
                    id="station-dropdown",
                    value=None,
                    options=station_options,
                    multi=True,
                ),
                html.H6("Select Units by DUID", className="selector-title"),
                dcc.Dropdown(
                    id="duid-dropdown",
                    value=None,
                    options=duid_options,
                    multi=True,
                ),
            ],
        ),
        html.Div(
            id="dispatch-type-div",
            children=[
                html.H6("Dispatch Type", className="selector-title"),
                dcc.RadioItems(
                    id="dispatch-type-selector",
                    options=["Generator", "Load"],
                    value="Generator",
                ),
                html.H6("Bidding Data Options", className="selector-title"),
                dcc.RadioItems(
                    id="raw-adjusted-selector",
                    options=["Raw Bids", "Adjusted Bids"],
                    value="Adjusted Bids",
                ),
            ],
        ),
        html.Div(
            id="show-demand-div",
            children=[
                html.H6("Show other Metrics", className="selector-title"),
                dcc.Checklist(
                    id="price-demand-checkbox",
                    options=["Demand", "Price"],
                    value=["Demand", "Price"],
                ),
            ],
        ),
    ]
    return settings_content


title = "Aggregated Bids by Region"
graph_content = dls.Dot(
    dcc.Graph(
        id="graph",
        figure={
            "layout": go.Layout(margin={"t": 20}),
        },
    )
)


def get_content(
    region_options,
    initial_regions,
    max_date,
    initial_start_date_obj,
    initial_duration,
    duid_options,
    station_options,
    tech_type_options,
):
    content = [
        html.Div(
            html.H6(id="graph-name", children=title),
        ),
        html.Div(
            html.H6(
                id="datetime-duration-title",
                className="selector-title",
                children="Pick starting datetime and duration:",
            )
        ),
        html.Div(
            id="datetime-duration-selector",
            children=[
                html.Div(
                    id="datetime-picker",
                    children=[
                        dcc.DatePickerSingle(
                            id="start-date-picker",
                            date=initial_start_date_obj,
                            max_date_allowed=max_date,
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
                    ],
                ),
                dcc.RadioItems(
                    id="duration-selector",
                    options=["Daily", "Weekly"],
                    value=initial_duration,
                    inline=True,
                ),
            ],
        ),
        html.Div(
            id="error-message-div", children=[html.P(id="error-message", children=[])]
        ),
        html.Div(
            id="graph-box",
            children=graph_content,
        ),
        html.Div(
            id="graph-selectors",
            children=get_settings_content(
                duid_options,
                station_options,
                tech_type_options,
                region_options,
                initial_regions,
            ),
        ),
        html.Div(
            id="dispatch-metric-selectors",
            children=[
                html.H6(
                    className="selector-title", children="Additional Dispatch Data"
                ),
                dcc.Checklist(
                    id="dispatch-checklist",
                    options=list(DISPATCH_COLUMNS.keys()),
                    value=["Availability"],
                    inline=True,
                ),
            ],
        ),
    ]

    return content


def build(
    region_options,
    initial_regions,
    max_date,
    initial_start_date_obj,
    initial_duration,
    duid_options,
    station_options,
    tech_type_options,
):
    return html.Div(
        [
            build_banner(),
            html.Div(
                id="app-container",
                children=get_content(
                    region_options,
                    initial_regions,
                    max_date,
                    initial_start_date_obj,
                    initial_duration,
                    duid_options,
                    station_options,
                    tech_type_options,
                ),
            ),
        ],
    )
