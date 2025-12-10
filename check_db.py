import sqlite3

DB_NAME = "final_project.db"

def check_counts():
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()

    cur.execute("SELECT COUNT(*) FROM SpotifyTracks")
    track_count = cur.fetchone()[0]

    cur.execute("SELECT COUNT(*) FROM SpotifyAudioFeatures")
    feat_count = cur.fetchone()[0]

    conn.close()

    print(f"Tracks in SpotifyTracks: {track_count}")
    print(f"Rows in SpotifyAudioFeatures: {feat_count}")

if __name__ == "__main__":
    check_counts()
