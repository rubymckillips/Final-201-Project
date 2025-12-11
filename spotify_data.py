import base64
import requests
import sqlite3
import os
from spotify_secrets import SPOTIFY_CLIENT_ID, SPOTIFY_CLIENT_SECRET

DB_PATH = os.path.join(os.path.dirname(__file__), "final_project.db")


ARTISTS = [
    "Taylor Swift",
    "Drake",
    "The Weeknd",
    "Billie Eilish",
    "Olivia Rodrigo",
    "Travis Scott",
    "Zach Bryan",
    "Tate McRae",
    "Future",
    "Kendrick Lamar",
    "SZA",
    "Morgan Wallen",
    "Justin Beiber",
    "Chris Brown",
    "21 Savage",
    "Kid Cudi",
    "Sabrina Carpenter"
]


def get_access_token():
    """
    Get an access token from Spotify using Client Credentials flow.
    """
    auth_url = "https://accounts.spotify.com/api/token"
    auth_str = f"{SPOTIFY_CLIENT_ID}:{SPOTIFY_CLIENT_SECRET}"
    auth_bytes = auth_str.encode("utf-8")
    auth_b64 = base64.b64encode(auth_bytes).decode("utf-8")

    headers = {
        "Authorization": f"Basic {auth_b64}",
    }
    data = {
        "grant_type": "client_credentials"
    }

    resp = requests.post(auth_url, headers=headers, data=data)
    resp.raise_for_status()
    token = resp.json()["access_token"]
    return token


def search_artist_id(artist_name, token):
    """
    Given an artist name, return the Spotify artist ID.
    """
    url = "https://api.spotify.com/v1/search"
    headers = {"Authorization": f"Bearer {token}"}
    params = {
        "q": artist_name,
        "type": "artist",
        "limit": 1
    }
    resp = requests.get(url, headers=headers, params=params)
    resp.raise_for_status()
    data = resp.json()
    items = data.get("artists", {}).get("items", [])
    if not items:
        return None
    return items[0]["id"]


def get_top_tracks(artist_id, token, market="US"):
    """
    Get top tracks for an artist ID.
    """
    url = f"https://api.spotify.com/v1/artists/{artist_id}/top-tracks"
    headers = {"Authorization": f"Bearer {token}"}
    params = {"market": market}
    resp = requests.get(url, headers=headers, params=params)
    resp.raise_for_status()
    data = resp.json()
    return data.get("tracks", [])


def get_artist_genre(artist_id, token):
    """
    Fetch the primary genre for an artist.
    """
    url = f"https://api.spotify.com/v1/artists/{artist_id}"
    headers = {"Authorization": f"Bearer {token}"}

    try:
        resp = requests.get(url, headers=headers)
        resp.raise_for_status()
        data = resp.json()
        genres = data.get("genres", [])
        if genres:
            return genres[0]
        return None
    except Exception:
        return None


def ensure_spotifytracks_table(cur):
    """
    Make sure the SpotifyTracks table exists. This lets us point at a fresh DB
    (like final_project_test.db) and still run without errors.
    """
    cur.execute("""
        CREATE TABLE IF NOT EXISTS SpotifyTracks (
            id INTEGER,
            track_id TEXT UNIQUE,
            track_name TEXT,
            artist_name TEXT,
            popularity INTEGER,
            genre TEXT
        )
    """)


def ensure_unique_track_index(cur):
    """
    Ensure track_id is unique in SpotifyTracks so we never
    insert duplicate tracks again.
    """
    cur.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS idx_spotifytracks_trackid
        ON SpotifyTracks(track_id)
    """)


def ensure_trackcountries_table(cur):
    """
    Ensure TrackCountries table exists, with a uniqueness constraint
    so we can't insert the same (track_fk, country_code) twice.
    """
    cur.execute("""
        CREATE TABLE IF NOT EXISTS TrackCountries (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            track_fk INTEGER,
            country_code TEXT,
            UNIQUE(track_fk, country_code)
        )
    """)


def ensure_ids_populated(cur):
    """
    For any existing rows where id is NULL, set id = rowid.
    This cleans up old data when running on an existing DB.
    """
    cur.execute("""
        UPDATE SpotifyTracks
        SET id = rowid
        WHERE id IS NULL
    """)


def store_spotify_data(per_run_limit=25, allow_new_tracks=True):
    """
    Fetch top tracks for a set of artists and store:
      - SpotifyTracks
      - TrackCountries (track <-> country codes)

    LIMIT POLICY:
    - Only per_run_limit (25) *new* SpotifyTracks can be inserted per run.
    - Existing tracks are skipped completely when allow_new_tracks=True.
    """
    token = get_access_token()
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    ensure_spotifytracks_table(cur)
    ensure_unique_track_index(cur)
    ensure_trackcountries_table(cur)
    ensure_ids_populated(cur)

    inserted = 0

    for artist_name in ARTISTS:
        artist_id = search_artist_id(artist_name, token)
        if not artist_id:
            continue

        tracks = get_top_tracks(artist_id, token)

        for t in tracks:
            
            if allow_new_tracks and inserted >= per_run_limit:
                break

            track_id = t["id"]
            track_name = t["name"]
            popularity = t.get("popularity", 0)
            artist_objects = t.get("artists", [])
            artist_names = ", ".join(a["name"] for a in artist_objects)

            primary_artist_id = artist_objects[0]["id"]
            genre = get_artist_genre(primary_artist_id, token)

            available_markets = t.get("available_markets", [])

            track_fk = None

            if allow_new_tracks:
                cur.execute("""
                    INSERT OR IGNORE INTO SpotifyTracks
                    (track_id, track_name, artist_name, popularity, genre)
                    VALUES (?, ?, ?, ?, ?)
                """, (track_id, track_name, artist_names, popularity, genre))

                if cur.rowcount == 0:
                    continue

                inserted += 1

                #  rowid for the brand-new row, and set id = rowid
                new_rowid = cur.lastrowid
                cur.execute("""
                    UPDATE SpotifyTracks
                    SET id = ?
                    WHERE rowid = ?
                """, (new_rowid, new_rowid))
                track_fk = new_rowid

            else:
                cur.execute("""
                    SELECT id FROM SpotifyTracks WHERE track_id = ?
                """, (track_id,))
                row = cur.fetchone()
                if row:
                    track_fk = row[0]

            if track_fk is None:
                # couldn't insert or find the track skip markets
                continue

            for code in available_markets:
                cur.execute("""
                    INSERT OR IGNORE INTO TrackCountries (track_fk, country_code)
                    VALUES (?, ?)
                """, (track_fk, code))

    conn.commit()
    conn.close()
    print(f"Inserted {inserted} new tracks this run.")


if __name__ == "__main__":
    store_spotify_data(per_run_limit=25, allow_new_tracks=True)
