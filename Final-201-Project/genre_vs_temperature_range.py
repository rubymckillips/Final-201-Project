import sqlite3
import matplotlib.pyplot as plt
import csv

conn = sqlite3.connect("/Users/rubymckillips/Documents/201/Final-201-Project/final_project.db")
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
CountryTempRanges AS (
    SELECT
        country_code,
        avg_temp,
        CASE
            WHEN avg_temp < 5 THEN 'Cold'
            WHEN avg_temp BETWEEN 5 AND 15 THEN 'Cool'
            WHEN avg_temp BETWEEN 15 AND 25 THEN 'Warm'
            ELSE 'Hot'
        END AS temp_range
    FROM CountryTemps
)
SELECT
    ctr.temp_range,
    st.genre,
    COUNT(*) AS track_count
FROM CountryTempRanges ctr
JOIN TrackCountries tc
    ON tc.country_code = ctr.country_code
JOIN SpotifyTracks st
    ON st.id = tc.track_fk
GROUP BY ctr.temp_range, st.genre
ORDER BY ctr.temp_range, track_count DESC;
"""

cur.execute(query)
rows = cur.fetchall()
conn.close()

with open("genre_vs_temperature_range.csv", "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerow(["temp_range", "genre", "track_count"])
    for temp, genre, count in rows:
        writer.writerow([temp, genre, count])

temp_order = ['Cold', 'Cool', 'Warm', 'Hot']
temp_ranges = [t for t in temp_order if any(r[0] == t for r in rows)]

genres = sorted(set(r[1] for r in rows))

data_by_genre = {g: [0] * len(temp_ranges) for g in genres}

for temp, genre, count in rows:
    if temp in temp_ranges:
        idx = temp_ranges.index(temp)
        data_by_genre[genre][idx] = count

genre_totals = [(genre, sum(counts)) for genre, counts in data_by_genre.items()]
genre_totals.sort(key=lambda x: x[1], reverse=True)
top_genres = [g for g, _ in genre_totals[:3]]

print("Top genres:", top_genres)

x = range(len(temp_ranges))
width = 0.2

plt.figure(figsize=(10, 6))

for i, genre in enumerate(top_genres):
    offsets = [xi + (i - (len(top_genres) - 1) / 2) * width for xi in x]
    plt.bar(offsets, data_by_genre[genre], width=width, label=genre)

plt.xticks(list(x), temp_ranges)
plt.xlabel("Temperature range")
plt.ylabel("Number of tracks")
plt.title("Track genres across temperature ranges")
plt.legend(title="Genre")
plt.tight_layout()

plt.savefig("genre_vs_temperature_range.png")
plt.show()