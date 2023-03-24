from nem_bidding_dashboard import postgres_helpers

market_price_floor = -1000
market_price_cap = 15500

bid_order = [
    "[-2000, -100)",
    "[-100, 0)",
    "[0, 50)",
    "[50, 100)",
    "[100, 200)",
    "[200, 300)",
    "[300, 500)",
    "[500, 1000)",
    "[1000, 5000)",
    "[5000, 10000)",
    "[10000, 16500)",
]

data_source = "local"

con_string = postgres_helpers.build_connection_string(
    hostname="localhost",
    dbname="bidding_dashboard_db",
    username="bidding_dashboard_maintainer",
    password="1234abcd",
    port=5433,
)


def price_to_frac(p):
    return (p - market_price_floor) / (market_price_cap - market_price_floor)


continuous_color_scales = {
    "original": [
        (price_to_frac(market_price_floor), "blue"),
        (price_to_frac(0), "purple"),
        (price_to_frac(100), "red"),
        (price_to_frac(500), "orange"),
        (price_to_frac(1000), "yellow"),
        (price_to_frac(market_price_cap), "green"),
    ]
}

discrete_color_scale = {
    "original": [
        "lightsalmon",
        "yellow",
        "red",
        "orange",
        "#00cc96",
        "#636efa",
        "purple",
        "cyan",
        "fuchsia",
        "palegreen",
        "lightblue",
    ]
}
