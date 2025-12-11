import sqlite3
import matplotlib.pyplot as plt
import csv
from collections import defaultdict

conn = sqlite3.connect("final_project.db")
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
CountryClimate AS (
    SELECT
        country_code,
        CASE
            WHEN avg_temp < 5 THEN 'Cold'
            WHEN avg_temp BETWEEN 5 AND 15 THEN 'Cool'
            WHEN avg_temp BETWEEN 15 AND 25 THEN 'Warm'
            ELSE 'Hot'
        END AS climate
    FROM CountryTemps
),
ArtistCountry AS (
    SELECT
        st.artist_name,
        tc.country_code
    FROM SpotifyTracks st
    JOIN TrackCountries tc ON st.id = tc.track_fk
),
ArtistClimate AS (
    SELECT
        ac.artist_name,
        cc.climate,
        COUNT(*) AS track_count
    FROM ArtistCountry ac
    JOIN CountryClimate cc ON ac.country_code = cc.country_code
    GROUP BY ac.artist_name, cc.climate
)
SELECT artist_name, climate, track_count
FROM ArtistClimate;
"""

rows = cur.execute(query).fetchall()
conn.close()

by_climate = defaultdict(list)
for artist, climate, count in rows:
    by_climate[climate].append((artist, count))

with open("artists_by_climate.csv", "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerow(["artist_name", "climate_category", "track_count"])
    for artist, climate, count in rows:
        writer.writerow([artist, climate, count])

climates = ["Cold", "Cool", "Warm", "Hot"]

fig, axes = plt.subplots(2, 2, figsize=(12, 10))
axes = axes.flatten()

for i, climate in enumerate(climates):
    data = by_climate.get(climate, [])
    data.sort(key=lambda x: x[1], reverse=True)
    top = data[:5]
    artists = [a for a, c in top]
    counts = [c for a, c in top]
    axes[i].barh(artists, counts)
    axes[i].set_title(f"Top Artists in {climate} Countries")
    axes[i].invert_yaxis()

plt.subplots_adjust(left=0.18, right=0.97, top=0.92, bottom=0.1, hspace=0.5, wspace=0.35)

plt.savefig("artists_by_climate.png", bbox_inches="tight")
plt.show()






