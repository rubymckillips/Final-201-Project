import sqlite3

conn = sqlite3.connect("final_project.db")
cur = conn.cursor()

cur.execute("SELECT name FROM sqlite_master WHERE type='table'")
print("Tables:", cur.fetchall())

cur.execute("SELECT COUNT(*) FROM SpotifyTracks")
print("Rows in SpotifyTracks:", cur.fetchone()[0])

cur.execute("SELECT COUNT(*) FROM TrackCountries")
print("Rows in TrackCountries:", cur.fetchone()[0])

conn.close()
