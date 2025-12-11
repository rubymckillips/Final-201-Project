import sqlite3
import matplotlib.pyplot as plt
import csv

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
)
SELECT country_code, avg_temp
FROM CountryTemps
ORDER BY avg_temp;
"""

cur.execute(query)
rows = cur.fetchall()
conn.close()

with open("temperature_across_countries.csv", "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerow(["country_code", "avg_temp_c"])
    for country, temp in rows:
        writer.writerow([country, temp])

countries = [row[0] for row in rows]
temps = [row[1] for row in rows]

plt.figure(figsize=(12, 6))
plt.plot(countries, temps, marker='o', linestyle='-', linewidth=2)

plt.xlabel("Country")
plt.ylabel("Average Temperature (°C)")
plt.title("Temperature Differences Across Sampled Countries")
plt.xticks(rotation=45, ha="right")
plt.grid(True)

plt.tight_layout()
plt.savefig("temperature_across_countries.png")
plt.show()