
from cassandra.cluster import Cluster
import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd

cluster = Cluster(['127.0.0.1'])
session = cluster.connect('diabetes_ks')

rows = session.execute("""
SELECT age, bmi, hba1c_level, blood_glucose_level, hypertension, heart_disease, diabetes
FROM diabetes_data_clean;
""")

data = {
    "age": [], "bmi": [], "hba1c": [],
    "glucose": [], "hypertension": [],
    "heart": [], "diabetes": []
}

for row in rows:
    data["age"].append(row.age)
    data["bmi"].append(row.bmi)
    data["hba1c"].append(row.hba1c_level)
    data["glucose"].append(row.blood_glucose_level)
    data["hypertension"].append(row.hypertension)
    data["heart"].append(row.heart_disease)
    data["diabetes"].append(row.diabetes)

df = pd.DataFrame(data)
corr = df.corr()

insert_stmt = session.prepare("""
    INSERT INTO diabetes_correlation_matrix (feature_1, feature_2, correlation)
    VALUES (?, ?, ?)
""")

for f1 in corr.columns:
    for f2 in corr.columns:
        session.execute(insert_stmt, (f1, f2, float(corr[f1][f2])))

plt.figure(figsize=(8, 6))
sns.heatmap(corr, annot=True, cmap="coolwarm", fmt=".2f")
plt.title("Correlation Heatmap: Diabetes Risk Factors")
plt.tight_layout()
plt.show()

print("Correlation matrix inserted into diabetes_correlation_matrix.")