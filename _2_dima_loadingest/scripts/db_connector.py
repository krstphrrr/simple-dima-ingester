import psycopg2
from _2_dima_loadingest.config import DATABASE_CONFIG, SCHEMA
import polars as pl
import logging

logger = logging.getLogger(__name__)


def map_dtype_to_sql(dtype: pl.DataType) -> str:
    if dtype == pl.Int64 or dtype == pl.Int32:
        return "INTEGER"
    elif dtype == pl.Float64 or dtype == pl.Float32:
        return "FLOAT"
    elif dtype == pl.Date or dtype == pl.Datetime:
        return "DATE"
    else:
        return "TEXT"

def create_table_if_not_exists(df: pl.DataFrame, table_name: str):
    conn = None
    try:

        conn = psycopg2.connect(**DATABASE_CONFIG)
        cursor = conn.cursor()

        # Dynamically create a SQL CREATE TABLE statement based on the DataFrame columns
        columns = [f'"{col}" {map_dtype_to_sql(dtype)}' for col, dtype in zip(df.columns, df.dtypes)]
        columns_sql = ", ".join(columns)

        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {SCHEMA}."{table_name}" (
            {columns_sql}
        );
        """

        cursor.execute(create_table_query)
        conn.commit()

        cursor.close()

    except Exception as e:
        if conn:
            conn.rollback()
        logger.info(f"Error creating table {table_name}: {e}")
    finally:
        if conn:
            conn.close()

def insert_dataframe_to_db(df: pl.DataFrame, table_name: str):
    create_table_if_not_exists(df, table_name)  # Ensure table exists before inserting data

    conn = None
    try:
        # Convert DataFrame to list of tuples for insertion
        records = df.to_dicts()

        conn = psycopg2.connect(**DATABASE_CONFIG)
        cursor = conn.cursor()

        # # Assuming the table schema matches the DataFrame's columns


        # Properly format column names for the SQL query
        cols = ", ".join([f'"{col}"' for col in df.columns])
        placeholders = ", ".join(["%s"] * len(df.columns))

        query = f"""
        INSERT INTO {SCHEMA}."{table_name}" ({cols})
        VALUES ({placeholders})
        """

        for record in records:
            cursor.execute(query, tuple(record.values()))

        conn.commit()
        cursor.close()

    except Exception as e:
        if conn:
            conn.rollback()
        logger.error(f"Error inserting DataFrame into DB: {e}")
    finally:
        if conn:
            conn.close()
