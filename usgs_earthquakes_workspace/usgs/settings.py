"""USGS Earthquake Pipeline settings and constants."""

# FDSN event service — full event catalog with time/magnitude filters
FDSN_EVENT_URL = "https://earthquake.usgs.gov/fdsnws/event/1/query"

# Curated GeoJSON feed of significant events from the past 30 days
SIGNIFICANT_MONTH_URL = (
    "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/significant_month.geojson"
)

MIN_MAGNITUDE = 2.5
PAGE_LIMIT = 2000
