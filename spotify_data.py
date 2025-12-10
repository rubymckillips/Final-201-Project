import base64
import requests
import sqlite3
from spotify_secrets import SPOTIFY_CLIENT_ID, SPOTIFY_CLIENT_SECRET


DB_NAME = "final_project.db"

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
    "Morgan Wallen"
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


def get_audio_features(track_ids, token):
    """
    Get audio features for a list of track IDs.
    """
    if not track_ids:
        return []

    url = "https://api.spotify.com/v1/audio-features"
    headers = {"Authorization": f"Bearer {token}"}
    params = {"ids": ",".join(track_ids)}

    try:
        resp = requests.get(url, headers=headers, params=params)
        resp.raise_for_status()
        data = resp.json()
        return data.get("audio_features", [])
    except Exception as e:
        print("Could not fetch audio features:", e)
        return []


def get_artist_genre(artist_id, token):
    """
    Fetch the primary genre for an artist.
    """
    url = f"https://api.spotify.com/v1/artists/{artist_id}"
    headers = {"Authorization": f"Bearer {token}"}

    resp = requests.get(url, headers=headers)

    try:
        resp.raise_for_status()
        data = resp.json()
        genres = data.get("genres", [])
        if genres:
            return genres[0]   # take the main genre
        else:
            return None
    except Exception:
        return None


def store_spotify_data(per_run_limit=25, allow_new_tracks=True):
    """
    Fetch top tracks for a set of artists and store:
      - SpotifyTracks
      - SpotifyAudioFeatures
      - TrackCountries (track <-> country codes)

    If allow_new_tracks is False, we will NOT insert any new SpotifyTracks rows,
    but we WILL:
      - update audio features for existing tracks
      - insert TrackCountries rows for existing tracks
    """
    token = get_access_token()
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()

    # Make sure the TrackCountries table exists
    cur.execute("""
        CREATE TABLE IF NOT EXISTS TrackCountries (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            track_fk INTEGER,
            country_code TEXT,
            FOREIGN KEY (track_fk) REFERENCES SpotifyTracks(id)
        )
    """)

    inserted = 0

    for artist_name in ARTISTS:
        artist_id = search_artist_id(artist_name, token)
        if not artist_id:
            print(f"Could not find artist ID for {artist_name}")
            continue

        tracks = get_top_tracks(artist_id, token)
        track_ids = [t["id"] for t in tracks]

        features_list = get_audio_features(track_ids, token)
        features_by_id = {f["id"]: f for f in features_list if f}

        for t in tracks:
            track_id = t["id"]
            track_name = t["name"]
            popularity = t.get("popularity", 0)
            artist_objects = t.get("artists", [])
            artist_names = ", ".join(a["name"] for a in artist_objects)

            primary_artist_id = artist_objects[0]["id"]
            genre = get_artist_genre(primary_artist_id, token)

            # NEW: Spotify country codes
            available_markets = t.get("available_markets", [])

            track_fk = None

            # 1) Try to insert a new track if allowed and limit not reached
            if allow_new_tracks and inserted < per_run_limit:
                try:
                    cur.execute("""
                        INSERT INTO SpotifyTracks (track_id, track_name, artist_name, popularity, genre)
                        VALUES (?, ?, ?, ?, ?)
                    """, (track_id, track_name, artist_names, popularity, genre))
                    track_fk = cur.lastrowid
                    inserted += 1
                except sqlite3.IntegrityError:
                    # Already in DB, just fetch its id
                    cur.execute("SELECT id FROM SpotifyTracks WHERE track_id = ?", (track_id,))
                    row = cur.fetchone()
                    if not row:
                        # If somehow missing, skip this track
                        continue
                    track_fk = row[0]
            else:
                # 2) We are NOT inserting new tracks: only work with existing ones
                cur.execute("SELECT id FROM SpotifyTracks WHERE track_id = ?", (track_id,))
                row = cur.fetchone()
                if not row:
                    # Track not already stored; skip it in update-only mode
                    continue
                track_fk = row[0]

            # If we still don't have a track_fk, skip
            if track_fk is None:
                continue

            # Store country codes for this track
            for code in available_markets:
                cur.execute("""
                    INSERT OR IGNORE INTO TrackCountries (track_fk, country_code)
                    VALUES (?, ?)
                """, (track_fk, code))

            # Store or update audio features
            f = features_by_id.get(track_id)
            if f:
                danceability = f.get("danceability")
                energy = f.get("energy")
                valence = f.get("valence")
                tempo = f.get("tempo")

                # Check if we already have features for this track_fk
                cur.execute("SELECT id FROM SpotifyAudioFeatures WHERE track_fk = ?", (track_fk,))
                exists = cur.fetchone()
                if not exists:
                    cur.execute("""
                        INSERT INTO SpotifyAudioFeatures (track_fk, danceability, energy, valence, tempo)
                        VALUES (?, ?, ?, ?, ?)
                    """, (track_fk, danceability, energy, valence, tempo))

    conn.commit()
    conn.close()
    print(f"Inserted {inserted} new tracks (if allowed).")


if __name__ == "__main__":
    store_spotify_data(per_run_limit=25, allow_new_tracks=True)

