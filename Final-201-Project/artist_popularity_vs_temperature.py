import sqlite3
import matplotlib.pyplot as plt
import csv
import os

BASE_DIR = os.path.dirname(__file__)
DB_PATH = os.path.join(BASE_DIR, "final_project.db")

conn = sqlite3.connect(DB_PATH)
cur = conn.cursor()

query = """
WITH CountryTemps AS (
    SELECT 
        l.country AS country_code,
        AVG(w.temp) AS avg_temp
    FROM Weather w
    JOIN Locations l ON w.location_id = l.id
    GROUP BY l.country
),
ArtistCountryTemp AS (
    SELECT
        st.artist_name,
        ct.country_code,
        ct.avg_temp,
        COUNT(*) AS track_count
    FROM SpotifyTracks st
    JOIN TrackCountries tc
        ON st.id = tc.track_fk
    JOIN CountryTemps ct
        ON tc.country_code = ct.country_code
    GROUP BY st.artist_name, ct.country_code
),
ArtistSummary AS (
    SELECT
        artist_name,
        AVG(avg_temp) AS avg_listener_temp,
        SUM(track_count) AS total_tracks
    FROM ArtistCountryTemp
    GROUP BY artist_name
)
SELECT artist_name, avg_listener_temp, total_tracks
FROM ArtistSummary
ORDER BY total_tracks DESC
LIMIT 30;
"""

cur.execute(query)
rows = cur.fetchall()
conn.close()

csv_path = os.path.join(BASE_DIR, "artist_popularity_vs_temperature.csv")
with open(csv_path, "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerow(["artist_name", "avg_listener_temp_c", "total_tracks"])
    for artist, avg_temp, total_tracks in rows:
        writer.writerow([artist, avg_temp, total_tracks])

artists = [r[0] for r in rows]
avg_temps = [r[1] for r in rows]
total_tracks = [r[2] for r in rows]

plt.figure(figsize=(10, 6))
plt.scatter(avg_temps, total_tracks)

for i, name in enumerate(artists[:5]):
    plt.annotate(name, (avg_temps[i], total_tracks[i]),
                 xytext=(5, 5), textcoords="offset points", fontsize=8)

plt.xlabel("Average Listener Temperature (°C)")
plt.ylabel("Total Tracks for Artist")
plt.title("Artist Popularity vs. Average Listener Temperature")
plt.tight_layout()

png_path = os.path.join(BASE_DIR, "artist_popularity_vs_temperature.png")
plt.savefig(png_path)
plt.show()