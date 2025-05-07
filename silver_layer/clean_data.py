from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement

cluster = Cluster(['127.0.0.1'])
session = cluster.connect('diabetes_ks')

select_stmt = SimpleStatement("SELECT * FROM diabetes_data", fetch_size=1000)
insert_stmt = session.prepare("""
    INSERT INTO diabetes_data_clean (
        id, year, gender, age, location,
        race_africanamerican, race_asian, race_caucasian,
        race_hispanic, race_other,
        hypertension, heart_disease,
        smoking_history, bmi, hba1c_level,
        blood_glucose_level, diabetes
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
""")

cleaned_count = 0
result = session.execute(select_stmt)

while True:
    for row in result.current_rows:
        if (
            0 <= row.age <= 120 and
            row.bmi > 0 and
            3 <= row.hba1c_level <= 20 and
            30 <= row.blood_glucose_level <= 400
        ):
            session.execute(insert_stmt, (
                row.id, row.year, row.gender, row.age, row.location,
                row.race_africanamerican, row.race_asian, row.race_caucasian,
                row.race_hispanic, row.race_other,
                row.hypertension, row.heart_disease,
                row.smoking_history, row.bmi, row.hba1c_level,
                row.blood_glucose_level, row.diabetes
            ))
            cleaned_count += 1
            if cleaned_count % 5000 == 0:
                print(f"{cleaned_count} clean rows inserted...")

    if result.has_more_pages:
        result.fetch_next_page()
    else:
        break

print(f"Done. Total clean rows inserted: {cleaned_count}")