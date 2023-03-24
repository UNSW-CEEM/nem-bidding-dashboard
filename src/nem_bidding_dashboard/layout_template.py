"""
TODO:
    Pages and links
    Any other GUI elements, aesthetics
    Documentation
"""

import dash_bootstrap_components as dbc
import dash_loading_spinners as dls
import plotly.graph_objects as go
from create_plots import DISPATCH_COLUMNS
from dash import dcc, html


def build_info_popup():
    return dbc.Modal(
        [
            dbc.ModalBody(
                [
                    html.H4(
                        "An Open-source tool for exploring National Electricity Market participant behaviour"
                    ),
                    dcc.Markdown(
                        """A key driver of market pricing and dispatch outcomes is the operational decision
                                  making of participants. The Australian Energy Market Operator (AEMO) publishes the
                                  bids submitted by participants to the market via its
                                  [data portal](https://aemo.com.au/en/energy-systems/electricity/national-electricity-market-nem/data-nem/market-data-nemweb).
                                  This data provides an excellent opportunity to study participant behaviour. However,
                                  downloading, processing, and visualising the data requires significant effort or
                                  access to a commercial data platform. **NEM Bidding Dashboard** allows anyone to
                                  easily visualise energy market bidding and dispatch data. Additionally, the backend
                                  code for running the web app has been published as an
                                  [open-source python package](https://github.com/UNSW-CEEM/nem-bidding-dashboard)."""
                    ),
                    html.H5("Functionality"),
                    dcc.Markdown(
                        """The web app functionality allows the user to visualise the bidding and dispatch
                        data over the time period of a week or day. Currently only energy market data is available, and
                        FCAS markets are not included. To limit cloud computing resource use, the weekly
                        visualisation only displays bidding data for the last dispatch interval of every hour. Filtering
                        can be done by region, dispatch type, unit type, station name and dispatch unit ID (DUID). Hover
                        your cursor over filters and options for more details. The underlying data can be access by
                        using the [nem-bidding-dashboard python package](https://github.com/UNSW-CEEM/nem-bidding-dashboard).
                        """
                    ),
                    html.H5("Project status"),
                    dcc.Markdown(
                        """The project is currently in a beta testing phase. The primary ongoing activities
                        are testing and documentation."""
                    ),
                    html.H5("Funding and project team"),
                    dcc.Markdown(
                        """**NEM Bidding Dashboard** is a project of the
                        [Collaboration on Energy and Environmental Markets](https://www.ceem.unsw.edu.au/) and the
                         [Digital Grid Futures Institute](https://www.dgfi.unsw.edu.au/)"""
                    ),
                    dcc.Markdown(
                        """Development by [Nicholas Gorman](https://www.linkedin.com/in/nicholas-gorman-32433a20b/)
                        and Patrick Chambers"""
                    ),
                ]
            ),
            dbc.ModalFooter(
                dbc.Button("Close", id="close", className="ms-auto", n_clicks=0)
            ),
        ],
        id="info",
        is_open=False,
        backdrop=False,
    )


def build_banner():
    return html.Div(
        id="banner",
        className="banner",
        children=[
            html.Div(
                id="banner-text",
                children=[
                    html.H5("NEM Bidding Dashboard"),
                    html.A(
                        "About",
                        target="_blank",
                        style={"color": "black", "margin-left": "10px"},
                        id="open",
                        n_clicks=0,
                    ),
                    build_info_popup(),
                    html.A(
                        "GitHub page",
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
                    html.A(
                        "Access data",
                        href="https://nem-bidding-dashboard.readthedocs.io/en/latest/examples.html#getting-the-data-behind-the-web-app-visualisations",
                        target="_blank",
                        style={"color": "black", "margin-left": "10px"},
                    ),
                ],
            ),
        ],
    )


def get_settings_content(
    initial_start_date_obj,
    initial_duration,
    duid_options,
    station_options,
    tech_type_options,
    region_options,
    initial_regions,
):
    settings_content = [
        html.Div(
            id="time-window-div",
            children=[
                html.Div(
                    id="datetime-duration-selector",
                    children=[
                        html.H6(
                            children="Starting datetime", className="selector-title"
                        ),
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
                            ],
                        ),
                    ],
                ),
                html.Div(
                    [
                        html.Button(
                            "\U00002190", id="decrease-date-button", n_clicks=0
                        ),
                        html.Button(
                            "\U00002192", id="increase-date-button", n_clicks=0
                        ),
                    ]
                ),
                html.Div(
                    className="tooltip",
                    children=[
                        html.Pre(
                            (
                                "The Weekly option displays bidding and dispatch data at an hourly resolution, down \n"
                                + "down sampling by selecting data for 5-minute dispatch interval ending on the hour. \n"
                                + "The Daily option displays bidding and dispatch data for each 5-minute dispatch interval."
                            ),
                            className="tooltiptext",
                        ),
                        html.H6(children="Duration", className="selector-title"),
                        dcc.RadioItems(
                            id="duration-selector",
                            options=["Weekly", "Daily"],
                            value=initial_duration,
                            inline=True,
                        ),
                    ],
                ),
                html.Div(
                    id="error-message-div",
                    children=[html.P(id="error-message", children=[])],
                ),
            ],
        ),
        html.Div(
            id="tech-type-div",
            children=[
                html.Div(
                    className="tooltip",
                    children=[
                        html.Pre(
                            (
                                "Which regions of the NEM to include in the charts, also affects the options available \n"
                                "in the unit type, station and DUID filters."
                            ),
                            className="tooltiptext",
                        ),
                        html.H6("Regions", className="selector-title"),
                        dcc.Checklist(
                            id="region-checklist",
                            options=region_options,
                            value=initial_regions,
                            inline=True,
                        ),
                    ],
                ),
                html.Div(
                    className="tooltip",
                    children=[
                        html.Pre(
                            (
                                "Which unit types to include in the charts, also affects the options available in the \n"
                                "station and DUID filters."
                            ),
                            className="tooltiptext",
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
            ],
        ),
        html.Div(
            id="duid-div",
            className="tooltip",
            children=[
                html.Pre(
                    (
                        "Which units to include in the charts, select by station name or individual DUIDs. \n"
                        "If a selection is made here, then the bidding data is not aggregated and the volume for each \n"
                        "bid band is plotted, however, additional dispatch data is still plotted on an aggregate basis."
                    ),
                    className="tooltiptext",
                ),
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
                html.Div(
                    className="tooltip",
                    children=[
                        html.Pre(
                            (
                                "Choose whether to plot bid for generators or loads, also affects the options available \n"
                                + "in the unit type, station and DUID filters."
                            ),
                            className="tooltiptext",
                        ),
                        html.H6("Dispatch Type", className="selector-title"),
                        dcc.RadioItems(
                            id="dispatch-type-selector",
                            options=["Generator", "Load"],
                            value="Generator",
                        ),
                    ],
                ),
                html.Div(
                    className="tooltip",
                    children=[
                        html.Pre(
                            (
                                "Choose whether to plot raw bidding data or bidding data adjusted for unit \n"
                                "availability. The availability adjusted bidding data is adjusted on a unit level,  \n"
                                "where the total bid by a unit exceeds its availability the bid volume is adjusted down \n"
                                "starting with the highest priced bids until the total bid equals the total available. \n"
                                "The availability value is taken from the AEMO MMS table Dispatch Load, and is the as \n"
                                "bid availability of the unit, or for variable renewable generators, the lesser of the  \n"
                                "as bid availability and the forecast availability."
                            ),
                            className="tooltiptext",
                        ),
                        html.H6("Bidding Data Options", className="selector-title"),
                        dcc.RadioItems(
                            id="raw-adjusted-selector",
                            options=["Raw Bids", "Adjusted Bids"],
                            value="Adjusted Bids",
                        ),
                    ],
                ),
            ],
        ),
        html.Div(
            [
                html.Div(
                    id="show-demand-div",
                    className="tooltip",
                    children=[
                        html.Pre(
                            "Demand: summed across selected regions. \n"
                            "Demand on secondary plot: same as demand but plotted on the lower graph. \n"
                            "Price: volume weighted average across selected regions.",
                            className="tooltiptext",
                        ),
                        html.H6("Show other Metrics", className="selector-title"),
                        dcc.Checklist(
                            id="price-demand-checkbox",
                            options=["Demand", "Demand on secondary plot", "Price"],
                            value=["Demand", "Price"],
                        ),
                    ],
                ),
                html.Div(
                    id="choose-color-scheme-div",
                    className="tooltip",
                    children=[
                        html.Pre(
                            "Choose the colour scheme.",
                            className="tooltiptext",
                        ),
                        html.H6("Choose colour scheme", className="selector-title"),
                        dcc.Dropdown(
                            id="colour-dropdown",
                            options=["original", "divergent"],
                            value="original",
                        ),
                    ],
                ),
            ]
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
        style={"height": "60vh", "min-height": "480px"},
    )
)


def get_content(
    region_options,
    initial_regions,
    initial_start_date_obj,
    initial_duration,
    duid_options,
    station_options,
    tech_type_options,
):
    content = [
        # html.Div(
        #     html.H6(id="graph-name", children=title),
        # ),
        html.Div(
            id="graph-box",
            children=graph_content,
        ),
        html.Div(
            id="graph-selectors",
            children=get_settings_content(
                initial_start_date_obj,
                initial_duration,
                duid_options,
                station_options,
                tech_type_options,
                region_options,
                initial_regions,
            ),
        ),
        html.Div(
            id="dispatch-metric-selectors",
            className="tooltip",
            children=[
                html.Pre(
                    (
                        "Additional dispatch metrics to plot (all in MW): \n"
                        "  - Availability: unit availability, lesser of the unit bid availability (Max Availabilty) \n"
                        "    and forecast availability for variable renewables. Aggregated by summing. \n"
                        "  - Dispatch Volume: the unit dispatch target for the end of the dispatch interval.  \n"
                        "    Aggregated by summing. \n"
                        "  - Final MW: the unit actual output at the end of the dispatch interval. Aggregated by \n"
                        "    summing. \n"
                        "  - As Bid Ramp Up Max Availability: The availability of a unit based on its as bid ramp rate. \n"
                        "    Aggregated by summing, but unit level contributions to the aggregate are capped at the  \n"
                        "    unit bid in availability. \n"
                        "  - As Bid Ramp Down Min Availability: The minimum operating level of a unit based on its as \n"
                        "    bid ramp rate. Aggregated by summing, but unit level contributions cannot be negative. \n"
                        "  - Ramp Up Max Availability: The availability of a unit based on the lesser of its as bid and \n"
                        "    telemetered ramp rates. Aggregated by summing, but unit level contributions to the  \n"
                        "    aggregate are capped at the unit availability. \n"
                        "  - Ramp Down Min Availability: The minimum operating level of a unit based on the lesser of \n"
                        "    its as bid and telemetered ramp rates. Aggregated by summing, but unit level contributions \n"
                        "    cannot be negative. \n"
                        "  - PASA Availability: The maximum availability of a unit given 24h as submitted by the unit \n"
                        "    as part of the PASA process. Could be useful as an estimate of unit of fleet technical \n"
                        "    availability, i.e. if participants made their entire unit capacities available to the \n"
                        "    market. Aggregated by summing. \n"
                        "  - Max Availability: As bid availability of unit. Aggregated by summing."
                    ),
                    className="tooltiptext",
                ),
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
                    initial_start_date_obj,
                    initial_duration,
                    duid_options,
                    station_options,
                    tech_type_options,
                ),
            ),
        ],
    )
