import polars as pl
import logging
import os, os.path
from datetime import datetime
from _2_dima_loadingest.config import (
    # static config variables
    DATA_DIR,
    fulljoin_key,
    lineplotjoin_key,
    pkdate_source
)
from _2_dima_loadingest.scripts.utils import (
    # process_csv helper functions
    load_csv_file,
    add_timestamps_and_source,
    store_dataframe,
    temp_storage,

    # create_pksource helper functions
    create_primary_key,
    load_dataframe,
    format_dates,
    find_and_load_files,
    join_dataframes,
    validate_primary_keys,
    classify_table,
)

logger = logging.getLogger(__name__)

pksources = {}

def process_csv(file_name: str, project_key: str = None):
    """
    Processes a CSV file:
    1. Classifies table type, data type, and source.
    2. Loads CSV data.
    3. Adds timestamps & source column.
    4. Stores data in temporary storage.
    5. Ensures PrimaryKey source exists and performs ordered joins.
    """
    logger.info(f"Processing file: {file_name}")

    # Classify Table Type
    source, data_type, table_type = classify_table(file_name)
    if not source or not table_type or not data_type:
        logger.warning(f"Skipping {file_name}: Unable to classify table.")
        return

    # Ensure Primary Key Source Exists
    if data_type not in pksources:
        logger.info(f"Creating pksource for table: {table_type}")
        create_pksource_per_datatype(data_type)

    # Load CSV
    filepath = os.path.normpath(os.path.join(DATA_DIR, file_name))
    logger.info(f"Loading CSV: {filepath}")
    csv_df = load_csv_file(filepath)

    if csv_df is None:
        logger.error(f"Skipping {file_name}: Failed to load CSV.")
        return

    # Add Timestamp & Source
    csv_df = add_timestamps_and_source(csv_df, source)

    # Store DataFrame
    store_dataframe(data_type, table_type, csv_df)

    # Perform Ordered Joins
    perform_ordered_joins(data_type)


def create_pksource_per_datatype(data_type):
    """
    Creates a primary key source DataFrame for a given data type by dynamically loading
    and joining relevant files, with special handling for 'Base'.
    """
    logger.info(f"Creating primary key source for data type: {data_type}")
    if data_type == "NoPrimaryKey":
        logger.info("Skipping as no primary key is required.")
        return

    # Load relevant files
    data_files = find_and_load_files(data_type, DATA_DIR)

    if data_files["lines"] is None or data_files["plots"] is None:
        logger.error(f"Missing essential files for {data_type}, skipping...")
        return

    # Create initial Lines-Plots join
    lines_plots_df = join_dataframes(data_files["lines"], data_files["plots"], "PlotKey")

    # Identify the primary join key
    join_key = lineplotjoin_key.get(data_type)

    # Special Handling for "Base" Data Type
    if data_type == "Base":
        if data_files["header"] is not None and data_files["detail"] is not None:
            semifinal_df = join_dataframes(data_files["header"], data_files["detail"], "RecKey")
        else:
            logger.error(f"Missing `tblGap` header/detail for 'Base', skipping...")
            return
    else:
        # Determine what files to join for final dataset
        if data_files["header"] is not None and data_files["detail"] is not None:
            # logger.info("creating header detail semifinal")
            semifinal_df = join_dataframes(data_files["header"], data_files["detail"], "RecKey")
        elif data_files["stack"] is not None and data_files["trapcollection"] is not None:
            # logger.info("creating stack trap semifinal")
            semifinal_df = join_dataframes(data_files["stack"], data_files["trapcollection"], "StackID")
        elif data_files["box"] is not None and data_files["boxcollection"] is not None:
            # logger.info("creating box boxcollection semifinal")
            semifinal_df = join_dataframes(data_files["box"], data_files["boxcollection"], "BoxID")
        elif data_files["pits"] is not None and data_files["pithorizons"] is not None:
            # logger.info("creating pit pithorizons semifinal")
            semifinal_df = join_dataframes(data_files["pits"], data_files["pithorizons"], "SoilKey")
        else:
            logger.error(f"Could not determine primary dataset for {data_type}, skipping...")
            return

    # Join with Lines-Plots
    final_source_df = join_dataframes(semifinal_df, lines_plots_df, join_key)

    # Select only keys and date columns
    key_columns = [col for col in final_source_df.columns if 'key' in col.lower() or 'date' in col.lower()]
    final_source_df = final_source_df.select(key_columns)

    # Format date columns
    final_source_df = format_dates(final_source_df)

    # Create Primary Key
    primary_key_col = pkdate_source.get(data_type, "FormDate")
    final_source_df = create_primary_key(final_source_df, ["PlotKey", primary_key_col])

    # Store in pksources dictionary
    pksources[data_type] = final_source_df
    logger.info(f"Stored primary key source for {data_type}")


def pksources_getter():
    "dictionary getter for debug"
    return pksources

def temp_storage_getter():
    "dictionary getter for debug"
    return temp_storage
