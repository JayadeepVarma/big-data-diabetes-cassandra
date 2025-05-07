from cassandra.cluster import Cluster
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score, classification_report
import uuid

cluster = Cluster(['127.0.0.1'])
session = cluster.connect('diabetes_ks')

query = """
SELECT age, bmi, hba1c_level, blood_glucose_level,
       hypertension, heart_disease, diabetes
FROM diabetes_data_clean;
"""

rows = session.execute(query)

X = []
y = []

for row in rows:
    X.append([
        row.age,
        row.bmi,
        row.hba1c_level,
        row.blood_glucose_level,
        row.hypertension,
        row.heart_disease
    ])
    y.append(row.diabetes)

X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.2, random_state=42
)

model = LogisticRegression(max_iter=1000)
model.fit(X_train, y_train)

y_pred = model.predict(X_test)
accuracy = accuracy_score(y_test, y_pred)

insert_stmt = session.prepare("""
    INSERT INTO diabetes_predictions (
        patient_id, actual, predicted, age, bmi,
        blood_glucose_level, hba1c_level,
        hypertension, heart_disease
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
""")

for i in range(len(X_test)):
    session.execute(insert_stmt, (
        uuid.uuid4(),
        y_test[i],
        int(y_pred[i]),
        X_test[i][0],
        X_test[i][1],
        int(X_test[i][2]),
        X_test[i][3],
        X_test[i][4],
        X_test[i][5]
    ))

print(f"\nLogistic Regression Accuracy: {accuracy:.2f}")
print("\nClassification Report:\n")
print(classification_report(y_test, y_pred))
print("Predictions inserted into diabetes_predictions table.")