import sqlite3
import requests
from config import OPENWEATHER_API_KEY

import os

DB_NAME = "final_project2.db"  # alter the name to reset to 25 
print("Database full path:", os.path.abspath(DB_NAME))

def get_connection():
    """Open a connection to the SQLite database and return (conn, cur)."""
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()
    return conn, cur


def create_tables():
    """Create Locations and Weather tables if they do not exist."""
    conn, cur = get_connection()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS Locations (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT,
            country TEXT,
            region TEXT,
            lat REAL,
            lon REAL
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS Weather (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            location_id INTEGER,
            timestamp INTEGER,
            temp REAL,
            feels_like REAL,
            humidity INTEGER,
            clouds INTEGER,
            wind_speed REAL,
            weather_description TEXT,
            FOREIGN KEY(location_id) REFERENCES Locations(id)
        )
    """)

    conn.commit()
    conn.close()


# list of cities

CITIES = [
    {"name": "Ann Arbor", "country": "US", "region": "North America", "lat": 42.2808, "lon": -83.7430},
    {"name": "New York", "country": "US", "region": "North America", "lat": 40.7128, "lon": -74.0060},
    {"name": "Los Angeles", "country": "US", "region": "North America", "lat": 34.0522, "lon": -118.2437},
    {"name": "Chicago", "country": "US", "region": "North America", "lat": 41.8781, "lon": -87.6298},
    {"name": "Houston", "country": "US", "region": "North America", "lat": 29.7604, "lon": -95.3698},
    {"name": "Miami", "country": "US", "region": "North America", "lat": 25.7617, "lon": -80.1918},
    {"name": "Toronto", "country": "CA", "region": "North America", "lat": 43.6532, "lon": -79.3832},
    {"name": "Vancouver", "country": "CA", "region": "North America", "lat": 49.2827, "lon": -123.1207},
    {"name": "San Francisco", "country": "US", "region": "North America", "lat": 37.7749, "lon": -122.4194},
    {"name": "Seattle", "country": "US", "region": "North America", "lat": 47.6062, "lon": -122.3321},
    {"name": "Boston", "country": "US", "region": "North America", "lat": 42.3601, "lon": -71.0589},
    {"name": "Philadelphia", "country": "US", "region": "North America", "lat": 39.9526, "lon": -75.1652},
    {"name": "Phoenix", "country": "US", "region": "North America", "lat": 33.4484, "lon": -112.0740},
    {"name": "Dallas", "country": "US", "region": "North America", "lat": 32.7767, "lon": -96.7970},
    {"name": "Austin", "country": "US", "region": "North America", "lat": 30.2672, "lon": -97.7431},
    {"name": "Detroit", "country": "US", "region": "North America", "lat": 42.3314, "lon": -83.0458},
    {"name": "Baltimore", "country": "US", "region": "North America", "lat": 39.2904, "lon": -76.6122},
    {"name": "Pittsburgh", "country": "US", "region": "North America", "lat": 40.4406, "lon": -79.9959},
    {"name": "Minneapolis", "country": "US", "region": "North America", "lat": 44.9778, "lon": -93.2650},
    {"name": "Denver", "country": "US", "region": "North America", "lat": 39.7392, "lon": -104.9903},
    {"name": "Portland", "country": "US", "region": "North America", "lat": 45.5152, "lon": -122.6784},
    {"name": "Orlando", "country": "US", "region": "North America", "lat": 28.5383, "lon": -81.3792},
    {"name": "Tampa", "country": "US", "region": "North America", "lat": 27.9506, "lon": -82.4572},
    {"name": "Montreal", "country": "CA", "region": "North America", "lat": 45.5017, "lon": -73.5673},
    {"name": "Ottawa", "country": "CA", "region": "North America", "lat": 45.4215, "lon": -75.6972},

    {"name": "London", "country": "GB", "region": "Europe", "lat": 51.5074, "lon": -0.1278},
    {"name": "Paris", "country": "FR", "region": "Europe", "lat": 48.8566, "lon": 2.3522},
    {"name": "Berlin", "country": "DE", "region": "Europe", "lat": 52.5200, "lon": 13.4050},
    {"name": "Madrid", "country": "ES", "region": "Europe", "lat": 40.4168, "lon": -3.7038},
    {"name": "Rome", "country": "IT", "region": "Europe", "lat": 41.9028, "lon": 12.4964},
    {"name": "Amsterdam", "country": "NL", "region": "Europe", "lat": 52.3676, "lon": 4.9041},
    {"name": "Stockholm", "country": "SE", "region": "Europe", "lat": 59.3293, "lon": 18.0686},
    {"name": "Vienna", "country": "AT", "region": "Europe", "lat": 48.2082, "lon": 16.3738},
    {"name": "Zurich", "country": "CH", "region": "Europe", "lat": 47.3769, "lon": 8.5417},
    {"name": "Geneva", "country": "CH", "region": "Europe", "lat": 46.2044, "lon": 6.1432},
    {"name": "Prague", "country": "CZ", "region": "Europe", "lat": 50.0755, "lon": 14.4378},
    {"name": "Budapest", "country": "HU", "region": "Europe", "lat": 47.4979, "lon": 19.0402},
    {"name": "Warsaw", "country": "PL", "region": "Europe", "lat": 52.2297, "lon": 21.0122},
    {"name": "Copenhagen", "country": "DK", "region": "Europe", "lat": 55.6761, "lon": 12.5683},
    {"name": "Helsinki", "country": "FI", "region": "Europe", "lat": 60.1699, "lon": 24.9384},
    {"name": "Oslo", "country": "NO", "region": "Europe", "lat": 59.9139, "lon": 10.7522},
    {"name": "Dublin", "country": "IE", "region": "Europe", "lat": 53.3498, "lon": -6.2603},
    {"name": "Lisbon", "country": "PT", "region": "Europe", "lat": 38.7223, "lon": -9.1393},
    {"name": "Brussels", "country": "BE", "region": "Europe", "lat": 50.8503, "lon": 4.3517},
    {"name": "Hamburg", "country": "DE", "region": "Europe", "lat": 53.5511, "lon": 9.9937},
    {"name": "Munich", "country": "DE", "region": "Europe", "lat": 48.1351, "lon": 11.5820},
    {"name": "Athens", "country": "GR", "region": "Europe", "lat": 37.9838, "lon": 23.7275},
    {"name": "Istanbul", "country": "TR", "region": "Europe", "lat": 41.0082, "lon": 28.9784},

    {"name": "Tokyo", "country": "JP", "region": "Asia", "lat": 35.6762, "lon": 139.6503},
    {"name": "Osaka", "country": "JP", "region": "Asia", "lat": 34.6937, "lon": 135.5023},
    {"name": "Seoul", "country": "KR", "region": "Asia", "lat": 37.5665, "lon": 126.9780},
    {"name": "Beijing", "country": "CN", "region": "Asia", "lat": 39.9042, "lon": 116.4074},
    {"name": "Shanghai", "country": "CN", "region": "Asia", "lat": 31.2304, "lon": 121.4737},
    {"name": "Singapore", "country": "SG", "region": "Asia", "lat": 1.3521, "lon": 103.8198},
    {"name": "Delhi", "country": "IN", "region": "Asia", "lat": 28.7041, "lon": 77.1025},
    {"name": "Mumbai", "country": "IN", "region": "Asia", "lat": 19.0760, "lon": 72.8777},
    {"name": "Hong Kong", "country": "HK", "region": "Asia", "lat": 22.3193, "lon": 114.1694},
    {"name": "Manila", "country": "PH", "region": "Asia", "lat": 14.5995, "lon": 120.9842},
    {"name": "Jakarta", "country": "ID", "region": "Asia", "lat": -6.2088, "lon": 106.8456},
    {"name": "Bangkok", "country": "TH", "region": "Asia", "lat": 13.7563, "lon": 100.5018},
    {"name": "Hanoi", "country": "VN", "region": "Asia", "lat": 21.0278, "lon": 105.8342},
    {"name": "Ho Chi Minh City", "country": "VN", "region": "Asia", "lat": 10.8231, "lon": 106.6297},
    {"name": "Taipei", "country": "TW", "region": "Asia", "lat": 25.0330, "lon": 121.5654},
    {"name": "Karachi", "country": "PK", "region": "Asia", "lat": 24.8607, "lon": 67.0011},
    {"name": "Islamabad", "country": "PK", "region": "Asia", "lat": 33.6844, "lon": 73.0479},
    {"name": "Colombo", "country": "LK", "region": "Asia", "lat": 6.9271, "lon": 79.8612},
    {"name": "Kathmandu", "country": "NP", "region": "Asia", "lat": 27.7172, "lon": 85.3240},
    {"name": "Kuala Lumpur", "country": "MY", "region": "Asia", "lat": 3.1390, "lon": 101.6869},

    {"name": "Sydney", "country": "AU", "region": "Oceania", "lat": -33.8688, "lon": 151.2093},
    {"name": "Melbourne", "country": "AU", "region": "Oceania", "lat": -37.8136, "lon": 144.9631},
    {"name": "Brisbane", "country": "AU", "region": "Oceania", "lat": -27.4698, "lon": 153.0251},
    {"name": "Perth", "country": "AU", "region": "Oceania", "lat": -31.9505, "lon": 115.8605},
    {"name": "Auckland", "country": "NZ", "region": "Oceania", "lat": -36.8485, "lon": 174.7633},
    {"name": "Wellington", "country": "NZ", "region": "Oceania", "lat": -41.2865, "lon": 174.7762},
    {"name": "Adelaide", "country": "AU", "region": "Oceania", "lat": -34.9285, "lon": 138.6007},

    {"name": "Mexico City", "country": "MX", "region": "Latin America", "lat": 19.4326, "lon": -99.1332},
    {"name": "Guadalajara", "country": "MX", "region": "Latin America", "lat": 20.6597, "lon": -103.3496},
    {"name": "São Paulo", "country": "BR", "region": "Latin America", "lat": -23.5505, "lon": -46.6333},
    {"name": "Rio de Janeiro", "country": "BR", "region": "Latin America", "lat": -22.9068, "lon": -43.1729},
    {"name": "Buenos Aires", "country": "AR", "region": "Latin America", "lat": -34.6037, "lon": -58.3816},
    {"name": "Santiago", "country": "CL", "region": "Latin America", "lat": -33.4489, "lon": -70.6693},
    {"name": "Bogotá", "country": "CO", "region": "Latin America", "lat": 4.7110, "lon": -74.0721},
    {"name": "Lima", "country": "PE", "region": "Latin America", "lat": -12.0464, "lon": -77.0428},
    {"name": "Quito", "country": "EC", "region": "Latin America", "lat": -0.1807, "lon": -78.4678},
    {"name": "Caracas", "country": "VE", "region": "Latin America", "lat": 10.4806, "lon": -66.9036},
    {"name": "Montevideo", "country": "UY", "region": "Latin America", "lat": -34.9011, "lon": -56.1645},
    {"name": "La Paz", "country": "BO", "region": "Latin America", "lat": -16.4897, "lon": -68.1193},

    {"name": "Cairo", "country": "EG", "region": "Africa", "lat": 30.0444, "lon": 31.2357},
    {"name": "Johannesburg", "country": "ZA", "region": "Africa", "lat": -26.2041, "lon": 28.0473},
    {"name": "Lagos", "country": "NG", "region": "Africa", "lat": 6.5244, "lon": 3.3792},
    {"name": "Nairobi", "country": "KE", "region": "Africa", "lat": -1.2921, "lon": 36.8219},
    {"name": "Casablanca", "country": "MA", "region": "Africa", "lat": 33.5731, "lon": -7.5898},
    {"name": "Accra", "country": "GH", "region": "Africa", "lat": 5.6037, "lon": -0.1870},
    {"name": "Abuja", "country": "NG", "region": "Africa", "lat": 9.0765, "lon": 7.3986},
    {"name": "Addis Ababa", "country": "ET", "region": "Africa", "lat": 8.9806, "lon": 38.7578},
    {"name": "Dar es Salaam", "country": "TZ", "region": "Africa", "lat": -6.7924, "lon": 39.2083},
    {"name": "Cape Town", "country": "ZA", "region": "Africa", "lat": -33.9249, "lon": 18.4241},

    {"name": "Doha", "country": "QA", "region": "Asia", "lat": 25.276987, "lon": 51.520008},
    {"name": "Reykjavik", "country": "IS", "region": "Europe", "lat": 64.1466, "lon": -21.9426},
    {"name": "Honolulu", "country": "US", "region": "North America", "lat": 21.3069, "lon": -157.8583},
    {"name": "Cape Coast", "country": "GH", "region": "Africa", "lat": 5.1053, "lon": -1.2466},
    {"name": "Hobart", "country": "AU", "region": "Oceania", "lat": -42.8821, "lon": 147.3272},
]


def insert_locations():
    """
    Insert cities into the Locations table if they are not already there.
    Safe to run multiple times (no duplicates).
    """
    conn, cur = get_connection()

    for city in CITIES:
        cur.execute("""
            SELECT id FROM Locations
            WHERE name = ? AND country = ?
        """, (city["name"], city["country"]))
        row = cur.fetchone()

        if row is None:
            cur.execute("""
                INSERT INTO Locations (name, country, region, lat, lon)
                VALUES (?, ?, ?, ?, ?)
            """, (
                city["name"], city["country"], city["region"],
                city["lat"], city["lon"]
            ))

    conn.commit()
    conn.close()


def get_locations_needing_weather(limit=25):
    """
    Return up to `limit` locations that do NOT yet have any Weather rows.
    This is how we enforce the '25 items per run' rule.
    """
    conn, cur = get_connection()
    cur.execute("""
        SELECT id, lat, lon
        FROM Locations
        WHERE id NOT IN (
            SELECT DISTINCT location_id FROM Weather
        )
        LIMIT ?
    """, (limit,))
    locations = cur.fetchall()
    conn.close()
    return locations 


def fetch_weather_for_location(lat, lon):
    """Call OpenWeather CURRENT WEATHER API for one set of coordinates."""
    base_url = "https://api.openweathermap.org/data/2.5/weather"
    params = {
        "lat": lat,
        "lon": lon,
        "units": "metric",
        "appid": OPENWEATHER_API_KEY
    }

    resp = requests.get(base_url, params=params)
    resp.raise_for_status()
    return resp.json()


def store_weather_row(location_id, weather_json):
    """
    Store the current weather for a location using the /data/2.5/weather format.
    Avoid duplicate (location_id, timestamp) pairs.
    """
    timestamp = weather_json["dt"]     

    main = weather_json["main"]
    wind = weather_json.get("wind", {})
    clouds = weather_json.get("clouds", {})

    temp = main["temp"]
    feels_like = main.get("feels_like", temp)
    humidity = main.get("humidity", None)
    cloud_cover = clouds.get("all", None)   
    wind_speed = wind.get("speed", None)
    weather_description = weather_json["weather"][0]["description"]

    conn, cur = get_connection()

    cur.execute("""
        SELECT id FROM Weather
        WHERE location_id = ? AND timestamp = ?
    """, (location_id, timestamp))
    if cur.fetchone() is None:
        cur.execute("""
            INSERT INTO Weather (
                location_id, timestamp, temp, feels_like,
                humidity, clouds, wind_speed,
                weather_description
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            location_id,
            timestamp,
            temp,
            feels_like,
            humidity,
            cloud_cover,
            wind_speed,
            weather_description,
        ))

    conn.commit()
    conn.close()


def get_and_store_weather_batch():
    """
    Main function for this file:
    - get up to 25 locations with no weather
    - fetch their weather
    - store one Weather row for each
    """
    locations = get_locations_needing_weather(limit=25)
    print(f"Fetching weather for {len(locations)} locations...")

    for loc_id, lat, lon in locations:
        try:
            data = fetch_weather_for_location(lat, lon)
            store_weather_row(loc_id, data)
        except Exception as e:
            print(f"Error fetching/storing weather for location {loc_id}: {e}")


if __name__ == "__main__":
    create_tables()
    insert_locations()
    get_and_store_weather_batch()