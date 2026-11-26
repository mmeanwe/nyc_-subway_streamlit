import html
import io
import time
import zipfile
from datetime import datetime
from zoneinfo import ZoneInfo

import pandas as pd
import requests
import streamlit as st
from google.transit import gtfs_realtime_pb2

# ============================================================
# CONFIGURATION & CONSTANTS
# ============================================================

NYC_TZ = ZoneInfo("America/New_York")
DEFAULT_STATION = "Times Sq-42 St"
MAX_TRAINS_DEFAULT = 10
REQUEST_TIMEOUT = 15
MTA_STATIC_GTFS = "https://rrgtfsfeeds.s3.amazonaws.com/gtfs_subway.zip"

MTA_FEEDS = {
    "1234567S": "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs",
    "ACE": "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-ace",
    "BDFM": "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-bdfm",
    "G": "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-g",
    "JZ": "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-jz",
    "NQRW": "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-nqrw",
    "L": "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-l",
}

ROUTE_COLORS = {
    "1": "#EE352E",
    "2": "#EE352E",
    "3": "#EE352E",
    "4": "#00933C",
    "5": "#00933C",
    "6": "#00933C",
    "7": "#B933AD",
    "7X": "#B933AD",
    "A": "#0039A6",
    "C": "#0039A6",
    "E": "#0039A6",
    "B": "#FF6319",
    "D": "#FF6319",
    "F": "#FF6319",
    "M": "#FF6319",
    "G": "#6CBE45",
    "J": "#996633",
    "Z": "#996633",
    "L": "#A7A9AC",
    "N": "#FCCC0A",
    "Q": "#FCCC0A",
    "R": "#FCCC0A",
    "W": "#FCCC0A",
    "S": "#808183",
    "GS": "#808183",
    "FS": "#808183",
    "H": "#808183",
}


# ============================================================
# HELPERS & STATIC GTFS LOADERS
# ============================================================

def clean(value) -> str:
    if value is None:
        return ""
    cleaned = str(value).strip()
    if cleaned.lower() in {"", "nan", "none"}:
        return ""
    return cleaned


@st.cache_data(ttl=86400, show_spinner="Downloading MTA static GTFS data...")
def load_static_gtfs_data():
    response = requests.get(MTA_STATIC_GTFS, timeout=30)
    response.raise_for_status()

    with zipfile.ZipFile(io.BytesIO(response.content)) as gtfs_zip:
        stops = pd.read_csv(gtfs_zip.open("stops.txt"), dtype=str, keep_default_na=False)
        trips = pd.read_csv(gtfs_zip.open("trips.txt"), dtype=str, keep_default_na=False)

    stop_name = dict(zip(stops["stop_id"], stops["stop_name"]))
    stop_parent = (
        dict(zip(stops["stop_id"], stops["parent_station"]))
        if "parent_station" in stops.columns
        else {}
    )
    trip_destination = (
        dict(zip(trips["trip_id"], trips["trip_headsign"]))
        if "trip_headsign" in trips.columns
        else {}
    )

    station_rows = (
        stops[stops["location_type"] == "1"]
        if "location_type" in stops.columns
        else stops
    )
    station_names = sorted(
        station_rows.loc[station_rows["stop_name"] != "", "stop_name"].unique().tolist()
    )

    return stops, stop_name, stop_parent, trip_destination, station_names


def get_station_stop_ids(stops: pd.DataFrame, station_name: str) -> list[str]:
    station_rows = stops[stops["stop_name"] == station_name]
    parent_ids = set()
    stop_ids = set()

    if "location_type" in station_rows.columns:
        parent_ids.update(
            station_rows.loc[station_rows["location_type"] == "1", "stop_id"]
        )

    if "parent_station" in station_rows.columns:
        for parent in station_rows["parent_station"]:
            parent_clean = clean(parent)
            if parent_clean:
                parent_ids.add(parent_clean)

    for stop_id in station_rows["stop_id"]:
        stop_clean = clean(stop_id)
        if stop_clean.endswith(("N", "S")):
            stop_ids.add(stop_clean)

    if parent_ids and "parent_station" in stops.columns:
        child_rows = stops[stops["parent_station"].isin(parent_ids)]
        for stop_id in child_rows["stop_id"]:
            stop_clean = clean(stop_id)
            if stop_clean.endswith(("N", "S")):
                stop_ids.add(stop_clean)

    return sorted(stop_ids)


def lookup_stop_name(stop_id: str, stop_name_map: dict, stop_parent_map: dict):
    stop_clean = clean(stop_id)
    if not stop_clean:
        return None

    name = clean(stop_name_map.get(stop_clean))
    if name:
        return name

    parent_id = clean(stop_parent_map.get(stop_clean))
    if parent_id:
        parent_name = clean(stop_name_map.get(parent_id))
        if parent_name:
            return parent_name

    if stop_clean.endswith(("N", "S")):
        base_name = clean(stop_name_map.get(stop_clean[:-1]))
        if base_name:
            return base_name

    return None


def get_destination(
    trip_id: str,
    trip_update,
    trip_destination_map: dict,
    stop_name_map: dict,
    stop_parent_map: dict,
) -> str:
    destination = clean(trip_destination_map.get(trip_id))
    if destination:
        return destination

    stop_updates = list(trip_update.stop_time_update)
    if stop_updates:
        stop_updates.sort(key=lambda update: int(update.stop_sequence or 0))
        final_stop_id = clean(stop_updates[-1].stop_id)
        resolved = lookup_stop_name(final_stop_id, stop_name_map, stop_parent_map)
        if resolved:
            return resolved

    return "Destination unavailable"


# ============================================================
# REALTIME FEED FETCHER
# ============================================================

@st.cache_data(ttl=20, show_spinner=False)
def fetch_all_realtime_feeds() -> tuple[dict[str, gtfs_realtime_pb2.FeedMessage], list[str]]:
    feeds: dict[str, gtfs_realtime_pb2.FeedMessage] = {}
    feed_errors: list[str] = []

    for feed_name, feed_url in MTA_FEEDS.items():
        try:
            response = requests.get(
                feed_url,
                timeout=REQUEST_TIMEOUT,
                headers={"User-Agent": "NYC-Subway-Streamlit-App/1.0"},
            )
            response.raise_for_status()
            feed = gtfs_realtime_pb2.FeedMessage()
            feed.ParseFromString(response.content)
            feeds[feed_name] = feed
        except Exception:
            feed_errors.append(feed_name)

    return feeds, feed_errors


def get_arrivals(
    stop_ids: list[str],
    direction: str,
    max_trains: int,
    trip_destination_map: dict,
    stop_name_map: dict,
    stop_parent_map: dict,
):
    now = int(time.time())
    dir_upper = direction.upper()

    selected_stops = {
        stop_id
        for stop_id in stop_ids
        if dir_upper == "BOTH" or stop_id.endswith(dir_upper)
    }

    feeds, feed_errors = fetch_all_realtime_feeds()
    arrivals = []

    for feed_name, feed in feeds.items():
        for entity in feed.entity:
            if not entity.HasField("trip_update"):
                continue

            trip_update = entity.trip_update
            trip = trip_update.trip
            route = clean(trip.route_id) or "?"
            trip_id = clean(trip.trip_id)
            destination = get_destination(
                trip_id,
                trip_update,
                trip_destination_map,
                stop_name_map,
                stop_parent_map,
            )

            for stop_update in trip_update.stop_time_update:
                stop_id = clean(stop_update.stop_id)
                if stop_id not in selected_stops:
                    continue

                arrival_timestamp = 0
                if stop_update.HasField("arrival"):
                    arrival_timestamp = int(stop_update.arrival.time or 0)
                if not arrival_timestamp and stop_update.HasField("departure"):
                    arrival_timestamp = int(stop_update.departure.time or 0)
                if not arrival_timestamp:
                    continue

                seconds_until = arrival_timestamp - now
                if seconds_until < -30:
                    continue

                minutes = 0 if seconds_until <= 30 else int((seconds_until + 59) // 60)
                nyc_arrival_time = datetime.fromtimestamp(
                    arrival_timestamp, tz=NYC_TZ
                ).strftime("%I:%M:%S %p")

                arrivals.append(
                    {
                        "route": route,
                        "trip_id": trip_id,
                        "stop_id": stop_id,
                        "destination": destination,
                        "arrival_timestamp": arrival_timestamp,
                        "arrival_time": nyc_arrival_time,
                        "minutes": minutes,
                        "feed": feed_name,
                    }
                )

    unique = {
        (
            train["route"],
            train["trip_id"],
            train["stop_id"],
            train["arrival_timestamp"],
        ): train
        for train in arrivals
    }

    sorted_arrivals = sorted(
        unique.values(), key=lambda train: train["arrival_timestamp"]
    )
    return sorted_arrivals[:max_trains], feed_errors, sorted(selected_stops)


# ============================================================
# BOARD RENDERER
# ============================================================

def format_arrival(minutes: int) -> str:
    if minutes <= 0:
        return "ARR"
    if minutes == 1:
        return "1 MIN"
    return f"{minutes} MIN"


def render_board_html(station_name: str, arrivals: list[dict]) -> str:
    nyc_now = datetime.now(NYC_TZ).strftime("%I:%M:%S %p")
    station_display = html.escape(station_name)

    rows_html = ""
    if not arrivals:
        rows_html = """
        <div style="padding:40px 0;color:#999;font-size:22px;">
            No upcoming trains found.
        </div>
        """
    else:
        for train in arrivals:
            route = train["route"]
            destination = train["destination"]
            route_color = ROUTE_COLORS.get(route, "#555555")
            route_text_color = "#000000" if route in {"N", "Q", "R", "W"} else "#FFFFFF"
            eta = format_arrival(train["minutes"])
            route_display = html.escape(route)
            destination_display = html.escape(destination)

            rows_html += f"""
            <div style="display:grid;grid-template-columns:74px 1fr 130px;gap:12px;align-items:center;padding:15px 0;border-bottom:1px solid #292929;">
                <div>
                    <div style="background:{route_color};color:{route_text_color};width:52px;height:52px;border-radius:50%;display:flex;align-items:center;justify-content:center;font-size:24px;font-weight:bold;">
                        {route_display}
                    </div>
                </div>
                <div>
                    <div style="font-size:22px;font-weight:bold;">
                        {destination_display}
                    </div>
                    <div style="color:#888;font-size:12px;margin-top:4px;">
                        To {destination_display} &nbsp;&bull;&nbsp; {train["arrival_time"]} ET
                    </div>
                </div>
                <div style="text-align:right;font-size:29px;font-weight:bold;white-space:nowrap;">
                    {eta}
                </div>
            </div>
            """

    return f"""
    <div style="background:#080808;color:#FFFFFF;max-width:950px;width:100%;padding:30px;box-sizing:border-box;border-radius:14px;font-family:Arial,Helvetica,sans-serif;">
        <div style="color:#888;font-size:13px;letter-spacing:3px;">
            NEW YORK CITY SUBWAY
        </div>
        <div style="display:flex;justify-content:space-between;align-items:center;gap:20px;margin-top:7px;margin-bottom:24px;">
            <div style="font-size:34px;font-weight:bold;">
                {station_display}
            </div>
            <div style="font-size:18px;color:#AAA;white-space:nowrap;">
                {nyc_now} ET
            </div>
        </div>
        <div style="display:grid;grid-template-columns:74px 1fr 130px;gap:12px;padding-bottom:8px;border-bottom:1px solid #333;color:#777;font-size:11px;letter-spacing:1px;">
            <div>TRAIN</div>
            <div>DESTINATION</div>
            <div style="text-align:right;">ARRIVAL</div>
        </div>
        {rows_html}
        <div style="display:flex;justify-content:space-between;margin-top:18px;color:#666;font-size:11px;">
            <div>LIVE MTA GTFS-REALTIME</div>
            <div>Updated {nyc_now} ET</div>
        </div>
    </div>
    """


# ============================================================
# STREAMLIT UI
# ============================================================

st.set_page_config(page_title="NYC Subway Real-Time Board", layout="wide")
st.title("NYC Subway Real-Time Board")
st.caption("Live MTA GTFS-Realtime arrivals with static GTFS station & destination lookup")

try:
    stops_df, stop_name_map, stop_parent_map, trip_destination_map, station_names = (
        load_static_gtfs_data()
    )
except Exception as exc:
    st.error(f"Failed to load static GTFS data: {exc}")
    st.stop()

if not station_names:
    st.error("No stations found in static GTFS data.")
    st.stop()

default_index = (
    station_names.index(DEFAULT_STATION) if DEFAULT_STATION in station_names else 0
)

col_station, col_dir, col_count, col_refresh = st.columns([3, 2, 2, 1])

with col_station:
    station_name = st.selectbox(
        "Station",
        options=station_names,
        index=default_index,
    )

with col_dir:
    direction_choice = st.selectbox(
        "Direction",
        options=[
            ("Both directions", "BOTH"),
            ("Northbound / N", "N"),
            ("Southbound / S", "S"),
        ],
        format_func=lambda item: item[0],
        index=0,
    )
    direction = direction_choice[1]

with col_count:
    max_trains = st.slider(
        "Trains",
        min_value=4,
        max_value=20,
        value=MAX_TRAINS_DEFAULT,
        step=1,
    )

with col_refresh:
    st.write("")
    st.write("")
    if st.button("Refresh Trains", type="primary", use_container_width=True):
        fetch_all_realtime_feeds.clear()

stop_ids = get_station_stop_ids(stops_df, station_name)
if not stop_ids:
    st.warning("No directional stop IDs were found for this station.")
    st.stop()

try:
    arrivals, feed_errors, selected_stops = get_arrivals(
        stop_ids=stop_ids,
        direction=direction,
        max_trains=max_trains,
        trip_destination_map=trip_destination_map,
        stop_name_map=stop_name_map,
        stop_parent_map=stop_parent_map,
    )

    status_parts = [
        f"**Station:** {station_name}",
        f"**Stop IDs:** {', '.join(selected_stops) if selected_stops else 'None'}",
        f"**Shown:** {len(arrivals)}",
        f"**NYC Time:** {datetime.now(NYC_TZ).strftime('%I:%M:%S %p')} ET",
    ]
    st.markdown(" &nbsp;|&nbsp; ".join(status_parts))

    if feed_errors:
        st.warning(f"Unavailable feeds: {', '.join(feed_errors)}")

    st.markdown(render_board_html(station_name, arrivals), unsafe_allow_html=True)

except Exception as exc:
    st.error(f"Unable to load subway data: {exc}")
