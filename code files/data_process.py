import pyodbc
import pandas as pd
import io, re, os
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

# ---------- DB ----------

def get_connection():
    return pyodbc.connect(
        f"DRIVER={{{os.getenv('DB_DRIVER')}}};"
        f"SERVER={os.getenv('DB_SERVER')};"
        f"DATABASE={os.getenv('DB_NAME')};"
        "Trusted_Connection=yes;"
    )

# ---------- HELPERS ----------

def clean_table_name(filename):
    name = filename.split('.')[0]
    name = re.sub(r'\W+', '_', name)
    return f"raw_{name}"

def read_file(filename, file_bytes):
    filename = filename.lower()

    if filename.endswith(".csv"):
        return pd.read_csv(io.BytesIO(file_bytes), dtype=str)
    elif filename.endswith(".xlsx"):
        return pd.read_excel(io.BytesIO(file_bytes), dtype=str, engine="openpyxl")
    elif filename.endswith(".txt"):
        return pd.read_csv(io.BytesIO(file_bytes), dtype=str)
    return None

def standardize_columns(df):
    df.columns = [str(col).strip().replace(" ", "_") for col in df.columns]
    return df.loc[:, ~df.columns.duplicated()]

def clean_dataframe(df):
    for col in df.columns:
        df[col] = df[col].astype(object)

        df[col] = df[col].apply(
            lambda x: None if pd.isna(x) else str(x).strip()
        )

    return df

# ---------- SCHEMA (ALL STRING) ----------

def get_schema(df):
    # force everything as NVARCHAR
    return {col: ("NVARCHAR(500)", "str") for col in df.columns}

# ---------- DB OPS ----------

def create_table(cursor, table_name, schema):
    cols = [f"[{c}] {t}" for c, (t, _) in schema.items()]

    cursor.execute(f"""
    IF OBJECT_ID('{table_name}', 'U') IS NOT NULL
        DROP TABLE [{table_name}];

    CREATE TABLE [{table_name}] (
        {', '.join(cols)}
    )
    """)

def insert_data(cursor, table_name, df):
    cols = list(df.columns)
    placeholders = ",".join(["?"] * len(cols))

    cursor.fast_executemany = True

    # FORCE EVERYTHING TO STRING HERE
    rows = [
        [None if v is None else str(v) for v in row]
        for row in df.values.tolist()
    ]

    cursor.executemany(
        f"INSERT INTO [{table_name}] ({','.join(f'[{c}]' for c in cols)}) VALUES ({placeholders})",
        rows
    )
    
# ---------- PIPELINE ----------

def process_file(cursor, filename, file_bytes, source):
    try:
        if not filename:
            filename = f"unknown_{datetime.now():%Y%m%d%H%M%S}.csv"

        df = read_file(filename, file_bytes)

        if df is None or df.empty:
            print(f"Skipped: {filename}")
            return

        print(f"{source} → {filename}")

        df = standardize_columns(df)
        df = clean_dataframe(df)

        schema = get_schema(df)
        table = clean_table_name(filename)

        create_table(cursor, table, schema)
        insert_data(cursor, table, df)

        print(f"Loaded → {table}")

    except Exception as e:
        print(f"Error: {filename} → {e}")