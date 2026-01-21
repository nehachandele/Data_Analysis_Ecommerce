import pandas as pd
import mysql.connector
import os

# ------------------ CSV files and table names ------------------
csv_files = [
    ('customers.csv', 'customers'),
    ('orders.csv', 'orders'),
    ('sellers.csv', 'sellers'),
    ('products.csv', 'products'),
    ('geolocation.csv', 'geolocation'),
    ('payments.csv', 'payments'),
    ('order_items.csv', 'order_items')
]

# ------------------ MySQL connection ------------------
conn = mysql.connector.connect(
    host='localhost',
    user='root',
    password='root',
    database='ecommerce'
)
cursor = conn.cursor()

# ------------------ CSV folder path ------------------
folder_path = r'C:/Users/nehac/OneDrive/Desktop/MernStack/DAProject/ecommerce'

# ------------------ Function to map pandas dtype to SQL ------------------
def get_sql_type(dtype):
    if pd.api.types.is_integer_dtype(dtype):
        return 'INT'
    elif pd.api.types.is_float_dtype(dtype):
        return 'DOUBLE'
    elif pd.api.types.is_bool_dtype(dtype):
        return 'BOOLEAN'
    elif pd.api.types.is_datetime64_any_dtype(dtype):
        return 'DATETIME'
    else:
        return 'TEXT'

# ------------------ PROCESS EACH CSV ------------------
for csv_file, table_name in csv_files:
    file_path = os.path.join(folder_path, csv_file)
    print(f"\nProcessing file: {csv_file}")

    # 🔹 Read CSV in CHUNKS (prevents kernel crash)
    chunk_size = 5000
    csv_iter = pd.read_csv(file_path, chunksize=chunk_size)

    first_chunk = True

    for df in csv_iter:
        # Clean column names
        df.columns = [
            col.strip()
               .replace(' ', '_')
               .replace('-', '_')
               .replace('.', '_')
            for col in df.columns
        ]

        # Replace NaN with None
        df = df.where(pd.notnull(df), None)

        # 🔹 Create table only once
        if first_chunk:
            columns_sql = ', '.join(
                f"`{col}` {get_sql_type(df[col].dtype)}"
                for col in df.columns
            )
            create_table_query = f"""
                CREATE TABLE IF NOT EXISTS `{table_name}` (
                    {columns_sql}
                )
            """
            cursor.execute(create_table_query)
            first_chunk = False

        # 🔹 Prepare INSERT query
        placeholders = ', '.join(['%s'] * len(df.columns))
        columns = ', '.join(f"`{col}`" for col in df.columns)
        insert_query = f"""
            INSERT INTO `{table_name}` ({columns})
            VALUES ({placeholders})
        """

        # 🔹 Convert dataframe to list of tuples
        data = [
            tuple(None if pd.isna(x) else x for x in row)
            for row in df.itertuples(index=False, name=None)
        ]

        # 🔹 BULK INSERT (very important)
        cursor.executemany(insert_query, data)
        conn.commit()

        print(f"Inserted {len(data)} rows into `{table_name}`")

print("\n✅ All CSV files imported successfully!")

# ------------------ Close connection ------------------
cursor.close()
conn.close()
