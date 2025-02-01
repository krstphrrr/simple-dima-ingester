
from _2_dima_loadingest.scripts.data_cleaner import add_date_loaded_column, deduplicate_dataframe
from _2_dima_loadingest.scripts.db_connector import insert_dataframe_to_db
from _2_dima_loadingest.config import fulljoin_key

import polars as pl
import logging
import os, os.path
from datetime import datetime

logger = logging.getLogger(__name__)

temp_storage = {}
pksources = {}

"""
helper functions for data_loader: process_csv
"""
def load_csv_file(file_path):
    """Loads a CSV file into a Polars DataFrame, handling missing values."""
    try:
        return pl.read_csv(file_path, null_values=["NA", "N/A", "null"], infer_schema_length=10000000)
    except Exception as e:
        logger.error(f"Failed to load CSV: {file_path} | Error: {e}")
        return None

def add_timestamps_and_source(df, source):
    """Adds a current timestamp and source column to a DataFrame."""
    if df is None:
        return None

    # Add Current Timestamp
    current_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    df = df.with_columns(
        pl.lit(current_timestamp).str.strptime(pl.Datetime, "%Y-%m-%d %H:%M:%S").alias("DateLoadedInDB")
    )

    # Add Source Column
    df = df.with_columns(pl.lit(source).alias("DBKey"))

    return df
def store_dataframe(data_type, table_type, df):
    """Stores a DataFrame into temporary storage."""
    if data_type not in temp_storage:
        temp_storage[data_type] = {}
    temp_storage[data_type][table_type] = df

def perform_ordered_joins(data_type):

    """Handles ordered joins and ensures every table gets a PrimaryKey."""
    if data_type not in pksources:
        logger.error(f"PrimaryKey source not found for {data_type}. Skipping joins.")
        return
    if data_type == 'NoPrimaryKey':
        logger.error(f"Found '{data_type}' data type. Skipping joins.")
        return


    pk_source = pksources[data_type]  # Get the pre-joined primary key source

    # Join existing tables with pk_source to propagate PrimaryKey
    for table_type in temp_storage[data_type]:
        if "PrimaryKey" not in temp_storage[data_type][table_type].columns:
            temp_storage[data_type][table_type] = temp_storage[data_type][table_type].join(
                pk_source, on=fulljoin_key[data_type][table_type], how="left"
            )

    # Final PrimaryKey validation
    validate_primary_keys(data_type)



"""
helper functions for create pksource
"""
def create_primary_key(df: pl.DataFrame, key_fields: list) -> pl.DataFrame:
    # Concatenate the fields to create a PrimaryKey
    return df.with_columns((pl.concat_str(key_fields, separator="")).alias("PrimaryKey"))

def load_dataframe(file_list, keyword, data_dir):
    """Find and load a CSV file based on a keyword match."""
    file_name = next((f for f in file_list if keyword.lower() in f.lower()), None)
    if file_name:
        file_path = os.path.normpath(os.path.join(data_dir, file_name))
        return pl.read_csv(file_path, null_values=["NA", "N/A", "null"], infer_schema_length=10000000)
    return None

def format_dates(df):
    """Convert all date columns to YYYY-MM-DD format."""
    if df is not None:
        date_columns = [col for col in df.columns if "date" in col.lower()]
        df = df.with_columns([
            df[col].str.strptime(pl.Date, "%m/%d/%y %H:%M:%S").dt.strftime("%Y-%m-%d").alias(col)
            for col in date_columns
        ])
    return df

def find_and_load_files(data_type, data_dir):
    """Finds and loads all relevant files for the given data_type, with special handling for 'Base'."""
    files = [f for f in os.listdir(data_dir) if data_type in f or "lines" in f.lower() or "plots" in f.lower()]

    # Special handling for "Base" (Uses `tblGap` for Header and Detail)
    if data_type == "Base":
        base_files = [f for f in files if "tblGap" in f or "lines" in f.lower() or "plots" in f.lower()]
        return {
            "header": load_dataframe(base_files, "header", data_dir),
            "detail": load_dataframe(base_files, "detail", data_dir),
            "lines": load_dataframe(base_files, "lines", data_dir),
            "plots": load_dataframe(base_files, "plots", data_dir),
        }
    # filtering soilstab from soilpits
    if data_type == 'tblSoil':
        files = [i for i in files if 'stab' not in i.lower()]

    return {
        "header": load_dataframe(files, "header", data_dir),
        "detail": load_dataframe(files, "detail", data_dir),
        "lines": load_dataframe(files, "lines", data_dir),
        "plots": load_dataframe(files, "plots", data_dir),
        "box": load_dataframe(files, "box.", data_dir),
        "boxcollection": load_dataframe(files, "boxcollection", data_dir),
        "stack": load_dataframe(files, "stack", data_dir),
        "trapcollection": load_dataframe(files, "trapcollection", data_dir),
        "pits": load_dataframe(files, "pits", data_dir),
        "pithorizons": load_dataframe(files, "pithorizons", data_dir),
    }

def join_dataframes(base_df, join_df, join_key, suffix="_right1"):
    """Joins two DataFrames on a given key while removing duplicated suffixes."""
    if join_df is not None:
        base_df = base_df.join(join_df, on=join_key, how="inner", suffix=suffix)
        base_df = base_df.select([col for col in base_df.columns if suffix not in col])
    return base_df

def validate_primary_keys(data_type):
    """Ensures all tables under the data type have a PrimaryKey column."""
    for table_type, df in temp_storage[data_type].items():
        if "PrimaryKey" not in df.columns:
            logger.warning(f"Table {data_type}_{table_type} is missing PrimaryKey!")


def classify_table(file_name: str):
    """Classifies a table by extracting table type and data type from filename."""
    base_name = os.path.splitext(file_name)[0]  # Remove .csv
    parts = base_name.split("_", 1)

    if len(parts) < 2:
        logger.error(f"Filename {file_name} does not follow expected format.")
        return None, None, None

    source, target_table = parts
    table_type = None

    table_mappings = {
        "Header": "Header",
        "Detail": "Detail",
        "CompYield": "CompYield",
        "Box": "Box",
        "BoxCollection": "BoxCollection",
        "Stack": "Stack",
        "TrapCollection": "TrapCollection",
        "Pits": "Pits",
        "PitHorizons": "PitHorizons",
        "Quads": "Quads",
        "Species": "Species",
        # "SpeciesGeneric": "SpeciesGeneric"
        # "Lines": "Lines",
        # "Plots": "Plots"
    }

    for suffix, ttype in table_mappings.items():
        if target_table.endswith(suffix) and "tblSpecies" not in target_table:
            table_type = ttype
            target_table = target_table[: -len(suffix)]
            break
    # Tables that will not get a primary key
    if target_table == "tblLines":
        return source, "Base", "Lines"
    elif target_table == "tblPlots":
        return source, "Base", "Plots"
    elif target_table == "tblPlotNotes":
        return source, "NoPrimaryKey", "PlotNotes"
    elif target_table == "tblPlotHistory":
        return source, "NoPrimaryKey", "PlotHistory"
    elif target_table == "tblSites":
        return source, "NoPrimaryKey", "Sites"
    elif target_table == "tblSpecies":
        return source, "NoPrimaryKey", "Species"
    elif target_table == "tblSpeciesGeneric":
        return source, "NoPrimaryKey", "SpeciesGeneric"
    elif target_table == "tblESDRockFragments":
        return source, "NoPrimaryKey", "tblESDRockFragments"

    if not table_type:
        logger.error(f"Unexpected table type in filename {file_name}")
        return None, None, None

    data_type = target_table  # The remaining table name represents data type
    return source, data_type, table_type
