import csv
import uuid
from cassandra.cluster import Cluster

cluster = Cluster(['127.0.0.1'])
session = cluster.connect('diabetes_ks')


insert_query = session.prepare("""
    INSERT INTO diabetes_data (
        id, year, gender, age, location,
        race_africanamerican, race_asian, race_caucasian,
        race_hispanic, race_other,
        hypertension, heart_disease,
        smoking_history, bmi, hba1c_level,
        blood_glucose_level, diabetes
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
""")

with open('data/diabetes_dataset.csv', mode='r', encoding='utf-8') as file:
    reader = csv.DictReader(file)
    count = 0
    for row in reader:
        session.execute(insert_query, (
            uuid.uuid4(),
            int(row['year']),
            row['gender'],
            float(row['age']),
            row['location'],
            int(row['race:AfricanAmerican']),
            int(row['race:Asian']),
            int(row['race:Caucasian']),
            int(row['race:Hispanic']),
            int(row['race:Other']),
            int(row['hypertension']),
            int(row['heart_disease']),
            row['smoking_history'],
            float(row['bmi']),
            float(row['hbA1c_level']),
            int(row['blood_glucose_level']),
            int(row['diabetes'])
        ))
        count += 1
        if count % 5000 == 0:
            print(f"{count} rows inserted...")

print("Data ingestion complete.")