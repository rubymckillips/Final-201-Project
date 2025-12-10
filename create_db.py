import sqlite3

def create_db(db_name="final_project.db"):
    conn = sqlite3.connect(db_name)
    cur = conn.cursor()  

    cur.execute("""
    CREATE TABLE IF NOT EXISTS SpotifyTracks (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        track_id TEXT UNIQUE,
        track_name TEXT,
        artist_name TEXT,
        popularity INTEGER,
        genre TEXT
    );
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS SpotifyAudioFeatures (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        track_fk INTEGER,
        danceability REAL,
        energy REAL,
        valence REAL,
        tempo REAL,
        FOREIGN KEY (track_fk) REFERENCES SpotifyTracks(id)
    );
    """)

    conn.commit()
    conn.close()
    print("Database created.")

if __name__ == "__main__":
    create_db()
