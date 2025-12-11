import sqlite3
import requests
import base64
import os
from spotify_secrets import SPOTIFY_CLIENT_ID, SPOTIFY_CLIENT_SECRET


DB_PATH = os.path.join(os.path.dirname(__file__), "final_project.db")


def get_connection():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    return conn, cur


def get_access_token():
    auth_url = "https://accounts.spotify.com/api/token"
    auth_str = f"{SPOTIFY_CLIENT_ID}:{SPOTIFY_CLIENT_SECRET}"
    auth_bytes = auth_str.encode("utf-8")
    auth_b64 = base64.b64encode(auth_bytes).decode("utf-8")

    headers = {"Authorization": f"Basic {auth_b64}"}
    data = {"grant_type": "client_credentials"}

    resp = requests.post(auth_url, headers=headers, data=data)
    resp.raise_for_status()
    return resp.json()["access_token"]


def ensure_trackcountries_table(cur):
    
    cur.execute("""
        CREATE TABLE IF NOT EXISTS TrackCountries (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            track_fk INTEGER,
            country_code TEXT,
            UNIQUE(track_fk, country_code)
        )
    """)


def update_countries_for_existing_tracks(limit=25):
    """
    Add country codes for up to `limit` SpotifyTracks that do NOT yet have
    any rows in TrackCountries. Respects the 25-items-per-run rule.
    """
    token = get_access_token()
    conn, cur = get_connection()

    ensure_trackcountries_table(cur)

    # Pick tracks that don't have countries yet
    cur.execute("""
        SELECT id, track_id
        FROM SpotifyTracks
        WHERE id NOT IN (
            SELECT DISTINCT track_fk FROM TrackCountries
        )
        LIMIT ?
    """, (limit,))
    rows = cur.fetchall()
    print(f"Updating countries for {len(rows)} tracks this run...")

    updated_tracks = 0

    for track_fk, track_id in rows:
        url = f"https://api.spotify.com/v1/tracks/{track_id}"
        headers = {"Authorization": f"Bearer {token}"}

        resp = requests.get(url, headers=headers)
        if resp.status_code != 200:
            print(f"Could not fetch track {track_id}: {resp.status_code}")
            continue

        data = resp.json()
        markets = data.get("available_markets", [])

        for code in markets:
            cur.execute("""
                INSERT OR IGNORE INTO TrackCountries (track_fk, country_code)
                VALUES (?, ?)
            """, (track_fk, code))

        updated_tracks += 1

    conn.commit()
    conn.close()
    print(f"Added country codes for {updated_tracks} tracks.")


if __name__ == "__main__":
    update_countries_for_existing_tracks(limit=25)
