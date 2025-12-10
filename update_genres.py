import sqlite3
import requests
from spotify_secrets import SPOTIFY_CLIENT_ID, SPOTIFY_CLIENT_SECRET
import base64

DB_NAME = "spotify_data.db"

def get_access_token():
    auth_str = f"{SPOTIFY_CLIENT_ID}:{SPOTIFY_CLIENT_SECRET}"
    b64_auth = base64.b64encode(auth_str.encode()).decode()
    headers = {"Authorization": f"Basic {b64_auth}"}
    data = {"grant_type": "client_credentials"}

    resp = requests.post(
        "https://accounts.spotify.com/api/token",
        headers=headers,
        data=data
    )
    return resp.json()["access_token"]

def get_artist_genre(artist_id, token):
    url = f"https://api.spotify.com/v1/artists/{artist_id}"
    headers = {"Authorization": f"Bearer {token}"}
    resp = requests.get(url, headers=headers)

    try:
        resp.raise_for_status()
        data = resp.json()
        genres = data.get("genres", [])
        return genres[0] if genres else None
    except Exception:
        return None

def fill_missing_genres():
    token = get_access_token()
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()

    cur.execute("SELECT id, track_id FROM SpotifyTracks WHERE genre IS NULL")
    rows = cur.fetchall()

    updated = 0

    for track_fk, track_id in rows:
        # get artist id for the track
        url = f"https://api.spotify.com/v1/tracks/{track_id}"
        headers = {"Authorization": f"Bearer {token}"}
        resp = requests.get(url, headers=headers)

        if resp.status_code != 200:
            continue

        data = resp.json()
        artist_id = data["artists"][0]["id"]

        genre = get_artist_genre(artist_id, token)

        if genre:
            cur.execute(
                "UPDATE SpotifyTracks SET genre = ? WHERE id = ?",
                (genre, track_fk)
            )
            updated += 1

    conn.commit()
    conn.close()

    print(f"Updated genres for {updated} tracks.")

if __name__ == "__main__":
    fill_missing_genres()
