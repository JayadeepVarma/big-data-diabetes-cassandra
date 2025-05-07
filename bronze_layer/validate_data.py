from cassandra.cluster import Cluster

cluster = Cluster(['127.0.0.1'])
session = cluster.connect('diabetes_ks')

count_query = "SELECT COUNT(*) FROM diabetes_data;"
rows = session.execute(count_query)
for row in rows:
    print(f"Total rows in diabetes_data table: {row.count}")

schema_query = """
SELECT column_name, type
FROM system_schema.columns
WHERE keyspace_name='diabetes_ks' AND table_name='diabetes_data';
"""
columns = session.execute(schema_query)
print("\nTable Columns:")
for col in columns:
    print(f"- {col.column_name}: {col.type}")