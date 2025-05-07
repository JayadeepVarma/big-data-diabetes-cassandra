
from cassandra.cluster import Cluster
import matplotlib.pyplot as plt

cluster = Cluster(['127.0.0.1'])
session = cluster.connect('diabetes_ks')

rows = session.execute("SELECT age, diabetes FROM diabetes_data_clean;")

age_bins = {
    "0–19": [0, 0],
    "20–39": [0, 0],
    "40–59": [0, 0],
    "60+": [0, 0]
}

for row in rows:
    if row.age < 20:
        group = "0–19"
    elif row.age < 40:
        group = "20–39"
    elif row.age < 60:
        group = "40–59"
    else:
        group = "60+"

    age_bins[group][0] += 1
    if row.diabetes == 1:
        age_bins[group][1] += 1

insert_stmt = session.prepare("""
    INSERT INTO diabetes_age_group_stats (age_group, total, diabetic, percentage)
    VALUES (?, ?, ?, ?)
""")

labels = list(age_bins.keys())
rates = []

for group in labels:
    total, diabetic = age_bins[group]
    percent = (diabetic / total * 100) if total > 0 else 0
    rates.append(percent)
    session.execute(insert_stmt, (group, total, diabetic, percent))

plt.figure(figsize=(6, 4))
plt.bar(labels, rates, color='orange')
plt.title("Diabetes Percentage by Age Group")
plt.ylabel("Percentage (%)")
plt.xlabel("Age Group")
plt.grid(True, axis='y')
plt.tight_layout()
plt.show()

print("Age group stats inserted into diabetes_age_group_stats.")