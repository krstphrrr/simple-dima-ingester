import polars as pl
import logging
import os, os.path

from _2_dima_loadingest.config import DATA_DIR


logger = logging.getLogger(__name__)

temp_storage = {} # ingestables
pksources = {}
joinkey_lookup = {
    "Detail":"RecKey",
    "Header":"LineKey",
    "Box" : "BoxID",
    "BoxCollection":"RecKey",
    "Stack": "StackID",
    "TrapCollection": "RecKey",
    "Pit":"SoilKey"
}

def load_csv(file_path: str) -> pl.DataFrame:
    try:
        df = pl.read_csv(file_path)
        return df
    except Exception as e:
        logger.error(f"Error loading CSV: {e}")
        return None
# dust deposition only  if trapcollection.shape>0 or is present
# stack and trapcollection on stackID

# horizontalflux
# box + stack on StackID
# boxstack + boxcollection on BoxID

# soil
# soilpits + soilhorizons on SoilKey

def create_primary_key(df: pl.DataFrame, key_fields: list) -> pl.DataFrame:
    # Concatenate the fields to create a PrimaryKey
    return df.with_columns((pl.concat_str(key_fields, separator="")).alias("PrimaryKey"))


def process_csv(file_name: str, project_key: str = None):
    logger.info(f"Starting process_csv function for file: {file_name}")
    source, table_type, data_type = classify_table(file_name)
    if not source or not table_type or not data_type:
        return  # Skip invalid files


    # Ensure the pksource is created before processing the file
    if data_type not in pksources:
        create_pksource_per_datatype(data_type)

    # Load the CSV into a DataFrame
    filepath = os.path.normpath(os.path.join(DATA_DIR, file_name))
    logger.info(f"Loading CSV from '{filepath}'...")
    csv_df = pl.read_csv(filepath, null_values=["NA", "N/A", "null"], infer_schema_length=10000000)
    date_columns = [i for i in csv_df.columns if "date" in i.lower()]
    csv_df = csv_df.with_columns([
        csv_df[col].str.strptime(pl.Date, "%m/%d/%y %H:%M:%S").dt.strftime("%Y-%m-%d").alias(col)
        for col in date_columns
    ])

    if csv_df is None:
        logger.error(f"Failed to load {file_name}")
        return

    # Add the source column
    df = csv_df.with_columns(pl.lit(source).alias("source"))

    # Store the DataFrame in temporary storage
    if data_type not in temp_storage:
        temp_storage[data_type] = {}

    temp_storage[data_type][table_type] = df

    # Perform ordered joins (now with guaranteed pksource availability)
    perform_ordered_joins(data_type)


def perform_ordered_joins(data_type):
    """Handles ordered joins and ensures every table gets a PrimaryKey."""
    if data_type not in pksources:
        logger.error(f"PrimaryKey source not found for {data_type}. Skipping joins.")
        return

    pk_source = pksources[data_type]  # Get the pre-joined primary key source

    # Join existing tables with pk_source to propagate PrimaryKey
    for table_type in temp_storage[data_type]:
        if "PrimaryKey" not in temp_storage[data_type][table_type].columns:
            temp_storage[data_type][table_type] = temp_storage[data_type][table_type].join(
                pk_source, on="LineKey", how="left"
            )

    # Final PrimaryKey validation
    validate_primary_keys(data_type)

def validate_primary_keys(data_type):
    """Ensures all tables under the data type have a PrimaryKey column."""
    for table_type, df in temp_storage[data_type].items():
        if "PrimaryKey" not in df.columns:
            logger.warning(f"Table {data_type}_{table_type} is missing PrimaryKey!")


def create_pksource_per_datatype(data_type):
    """Creates a primary key source DataFrame for a given data type by joining Header, Detail, and Lines/Plots."""
    logger.info(f"Creating primary key source for data type: {data_type}")

    datatype_files = [i for i in os.listdir(DATA_DIR) if data_type in i or "lines" in i.lower() or 'plots' in i.lower()]

    # Detect available files for this data type
    header_file = next((i for i in datatype_files if 'header' in i.lower()), None)
    detail_file = next((i for i in datatype_files if 'detail' in i.lower()), None)
    lines_file = next((i for i in datatype_files if 'lines' in i.lower()), None)
    plots_file = next((i for i in datatype_files if 'plots' in i.lower()), None)

    box_file = next((i for i in datatype_files if 'box.' in i.lower()), None)
    boxcollection_file = next((i for i in datatype_files if 'boxcollection' in i.lower()), None)
    stack_file = next((i for i in datatype_files if 'stack' in i.lower()), None)
    trapcollection_file = next((i for i in datatype_files if 'trapcollection' in i.lower()), None)

    pit_file = next((i for i in datatype_files if 'header' in i.lower()), None)
    pithorizons_file = next((i for i in datatype_files if 'header' in i.lower()), None)

    if not header_file or not detail_file:
        logger.warning(f"Skipping {data_type}: Missing necessary Header or Detail files")
        return

    # make if / case

    header_path = os.path.normpath(os.path.join(DATA_DIR, header_file))
    detail_path = os.path.normpath(os.path.join(DATA_DIR, detail_file))

    header_df = pl.read_csv(header_path, null_values=["NA", "N/A", "null"], infer_schema_length=10000000)
    detail_df = pl.read_csv(detail_path, null_values=["NA", "N/A", "null"], infer_schema_length=10000000)

    # Join Header with Detail on RecKey
    header_detail_df = header_df.join(detail_df, on="RecKey", how="inner")

    #  add bsne tables and soil tables
    # If available, join with Lines/Plots
    if lines_file and plots_file: # refine
        plots_path = os.path.normpath(os.path.join(DATA_DIR, plots_file))
        lines_path = os.path.normpath(os.path.join(DATA_DIR, lines_file))

        lines_df = pl.read_csv(lines_path, null_values=["NA", "N/A", "null"], infer_schema_length=10000000)
        plots_df = pl.read_csv(plots_path, null_values=["NA", "N/A", "null"], infer_schema_length=10000000)

        # Create the base Lines-Plots table
        lines_plots_df = lines_df.join(plots_df, on="PlotKey", how="inner")

        # Join header-detail with Lines-Plots using LineKey
        final_source_df = header_detail_df.join(lines_plots_df, on="LineKey", how="inner", suffix="_right1")
        final_source_df = final_source_df.select([i for i in final_source_df.columns if "right" not in i])
    else:
        final_source_df = header_detail_df

    # ensure only necessary columns are retained (keys and dates)
    key_columns = [col for col in final_source_df.columns if 'key' in col.lower() or 'date' in col.lower()]
    final_source_df = final_source_df.select(key_columns)
    date_columns = [i for i in final_source_df.columns if "date" in i.lower()]
    final_source_df = final_source_df.with_columns([
        final_source_df[col].str.strptime(pl.Date, "%m/%d/%y %H:%M:%S").dt.strftime("%Y-%m-%d").alias(col)
        for col in date_columns
    ])
    final_source_df = create_primary_key(final_source_df, ["PlotKey", "FormDate"])

    # Store in pksources dictionary
    pksources[data_type] = final_source_df
    logger.info(f"Stored primary key source for {data_type}")


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
        "PitHorizons": "PitHorizons"
        # "Lines": "Lines",
        # "Plots": "Plots"
    }

    for suffix, ttype in table_mappings.items():
        if target_table.endswith(suffix):
            table_type = ttype
            target_table = target_table[: -len(suffix)]
            break
    if "tblLines" in target_table:
        return source, "Base", "Lines"
    elif target_table == "tblPlots":
        return source, "Base", "Plots"

    if not table_type:
        logger.error(f"Unexpected table type in filename {file_name}")
        return None, None, None

    data_type = target_table  # The remaining table name represents data type
    return source, table_type, data_type
