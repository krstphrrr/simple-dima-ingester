from _2_dima_loadingest.scripts.data_loader import load_csv
from _2_dima_loadingest.scripts.data_cleaner import add_date_loaded_column, deduplicate_dataframe
from _2_dima_loadingest.scripts.db_connector import insert_dataframe_to_db

import polars as pl
import logging
import os

logger = logging.getLogger(__name__)

def process_csv(file_name: str, file_path: str):
    # Extract source and target from the filename
    base_name = os.path.splitext(file_name)[0]  # Remove file extension
    source, target_table = base_name.split("_")  # Split by underscore

    # Load the CSV into a DataFrame
    df = load_csv(file_path)

    if df is not None:
        # Add the source column based on the extracted source
        df = df.with_columns(pl.lit(source).alias("source"))

        # Add the DateLoadedInDb column
        df = add_date_loaded_column(df)
        df = deduplicate_dataframe(df)

        # Insert the DataFrame into the target table
        insert_dataframe_to_db(df, target_table)
        logger.info(f"Processed {file_name} into table {target_table} with source {source}.")
