""" 
TODO:
    Pages and links
    Any other GUI elements, aesthetics
    Documentation
"""

from dash import Dash, dcc, html, Input, Output

def build_banner():
    return html.Div(
        id="banner",
        className="banner",
        children=[
            html.Div(
                id="banner-text",
                children=[
                    html.H5("NEM Dashboard"),
                ],
            ),
        ],
    )

def build_main_app(title:str, settings_content:list, graph_content:list):
    return html.Div(
        id="main-app-container", 
        children=[
            html.Div(
                html.H6(id="graph-name", children=title), 
            ),
            html.Div(
                id="graph-box", 
                children=graph_content,
            ),
            html.Div(
                id="graph-selectors", 
                children=settings_content,
            ), 
        ]
    )


def build (title:str, settings_content:list, graph_content:list):
    return html.Div(
        [
            build_banner(),
            html.Div(
                id="app-container",
                children=[
                    build_main_app(title, settings_content, graph_content),
                ],
            ),
            html.Div(
                className="banner",
                id="footer",
            )
            
        ]
    )

