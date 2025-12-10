import sqlite3
import os

DB_PATH = os.path.join(os.path.dirname(__file__), "final_project.db")

def show_join_sample():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    query = """
    SELECT
        s.id                AS spotify_row_id,
        s.track_name,
        s.artist_name,
        s.genre,
        s.popularity,
        tc.country_code,
        l.name              AS city_name,
        l.region,
        w.temp,
        w.weather_main
    FROM SpotifyTracks AS s
    JOIN TrackCountries AS tc
        ON s.id = tc.track_fk
    JOIN Locations AS l
        ON tc.country_code = l.country
    JOIN Weather AS w
        ON l.id = w.location_id
    LIMIT 20;
    """

    cur.execute(query)
    rows = cur.fetchall()
    for row in rows:
        print(row)

    conn.close()

if __name__ == "__main__":
    show_join_sample()
