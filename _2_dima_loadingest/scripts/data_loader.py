import polars as pl
import logging
import os, os.path

from _2_dima_loadingest.config import DATA_DIR


logger = logging.getLogger(__name__)

temp_storage = {} # ingestables
pksources = {}


# Which column to join to provide primarykey back to table
fulljoin_key = {
    "tblCompact":{
        "Header": "PlotKey",
        "Detail": "RecKey",
    },
    "tblGap": {
        "Header": "LineKey",
        "Detail": "RecKey",
    },
    "tblLPI": {
        "Header": "LineKey",
        "Detail": "RecKey"
    },
    "tblPlantDen": {
        "Header": "LineKey",
        "Detail": "RecKey",
        "Quads": "RecKey",
        "Species": "RecKey",
    },
    "tblSoilStab": {
        "Header": "LineKey",
        "Detail": "RecKey",
    },
    "tblSpecRich":{
        "Header": "LineKey",
        "Detail": "RecKey",
    },
    "tblSoil": {
        "Pits": "SoilKey",
        "PitHorizons": "SoilKey",
    },
    "tblBSNE": {
        "Box": "BoxID",
        "BoxCollection": "RecKey",
        "Stack": "StackID",
        "TrapCollection": "RecKey",
    },
    "Base":{
        "Lines": "LineKey",
        "Plots": "PlotKey",
    },
}
# which column to join to lineplot
lineplotjoin_key = {
    "tblCompact": "PlotKey",
    "tblGap": "LineKey",
    "tblLPI": "LineKey",
    "tblPlantDen": "LineKey",
    "tblSoil": "PlotKey",
    "tblSoilStab": "PlotKey",
    "tblSpecRich": "LineKey",
    "Base": "LineKey",
}
# date source for pk formation depending on data type
pkdate_source = {
    "tblCompact": "FormDate",
    "tblGap": "FormDate",
    "tblLPI": "FormDate",
    "tblPlantDen": "FormDate",
    "tblSoil" : "DateRecorded",
    "tblSoilStab": "FormDate",
    "tblSpecRich": "FormDate",
    "tblBSNE" : "collectDate",
    "Base": "FormDate",
}

def load_csv(file_path: str) -> pl.DataFrame:
    "to be removed"
    try:
        df = pl.read_csv(file_path)
        return df
    except Exception as e:
        logger.error(f"Error loading CSV: {e}")
        return None



def pksources_getter():
    "dictionary getter for debug"
    return pksources

def temp_storage_getter():
    "dictionary getter for debug"
    return temp_storage

def create_primary_key(df: pl.DataFrame, key_fields: list) -> pl.DataFrame:
    # Concatenate the fields to create a PrimaryKey
    return df.with_columns((pl.concat_str(key_fields, separator="")).alias("PrimaryKey"))


def process_csv(file_name: str, project_key: str = None):
    """
    1. Classifies csv file name on: source, data_type, table_type
    2. Creates a PrimaryKey source per data_type
    3. Appends PrimaryKey back to processed csv

    """
    logger.info(f"Starting process_csv function for file: {file_name}")
    source, data_type, table_type = classify_table(file_name)
    if not source or not table_type or not data_type:
        return  # Skip invalid files


    # Ensure the pksource is created before processing the file
    if data_type not in pksources:
        logger.info(f"for table: {table_type}")
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

def validate_primary_keys(data_type):
    """Ensures all tables under the data type have a PrimaryKey column."""
    for table_type, df in temp_storage[data_type].items():
        if "PrimaryKey" not in df.columns:
            logger.warning(f"Table {data_type}_{table_type} is missing PrimaryKey!")



def create_pksource_per_datatype(data_type):
    """
    Creates a primary key source DataFrame for a given data type by joining Header, Detail, and Lines/Plots.

    dup code smell: refactor
    """
    logger.info(f"Creating primary key source for data type: {data_type}")
    if data_type == "NoPrimaryKey":
        logger.info(f"returning..")
        return

    datatype_files = [i for i in os.listdir(DATA_DIR) if data_type in i or "lines" in i.lower() or 'plots' in i.lower()]
    if data_type == 'tblSoil':
        datatype_files = [i for i in datatype_files if 'stab' not in i.lower()]


    # Detect available files for this data type
    header_file = next((i for i in datatype_files if 'header' in i.lower()), None)
    detail_file = next((i for i in datatype_files if 'detail' in i.lower()), None)

    if data_type == 'Base':
        datatype_files_base = [i for i in os.listdir(DATA_DIR) if "tblGap" in i or "lines" in i.lower() or 'plots' in i.lower()]
        header_file = next((i for i in datatype_files_base if 'header' in i.lower()), None)
        detail_file = next((i for i in datatype_files_base if 'detail' in i.lower()), None)

    lines_file = next((i for i in datatype_files if 'lines' in i.lower()), None)
    plots_file = next((i for i in datatype_files if 'plots' in i.lower()), None)

    box_file = next((i for i in datatype_files if 'box.' in i.lower()), None)
    boxcollection_file = next((i for i in datatype_files if 'boxcollection' in i.lower()), None)
    stack_file = next((i for i in datatype_files if 'stack' in i.lower()), None)
    trapcollection_file = next((i for i in datatype_files if 'trapcollection' in i.lower()), None)

    pit_file = next((i for i in datatype_files if 'pits' in i.lower()), None)
    pithorizons_file = next((i for i in datatype_files if 'pithorizon' in i.lower()), None)

    try:
        if lines_file and plots_file: # refine
            logger.info(f"GOT LINEPLOT")

            plots_path = os.path.normpath(os.path.join(DATA_DIR, plots_file))
            lines_path = os.path.normpath(os.path.join(DATA_DIR, lines_file))

            lines_df = pl.read_csv(lines_path, null_values=["NA", "N/A", "null"], infer_schema_length=10000000)
            plots_df = pl.read_csv(plots_path, null_values=["NA", "N/A", "null"], infer_schema_length=10000000)

            # Create the base Lines-Plots table
            lines_plots_df = lines_df.join(plots_df, on="PlotKey", how="inner")

        # make if / case
        if 'Base' in data_type:
            logger.info(f"BASE IN")
            header_path = os.path.normpath(os.path.join(DATA_DIR, header_file))
            detail_path = os.path.normpath(os.path.join(DATA_DIR, detail_file))

            header_df = pl.read_csv(header_path, null_values=["NA", "N/A", "null"], infer_schema_length=10000000)
            detail_df = pl.read_csv(detail_path, null_values=["NA", "N/A", "null"], infer_schema_length=10000000)

            # Join Header with Detail on RecKey
            semifinal_df = header_df.join(detail_df, on="RecKey", how="inner")

        logger.info(f"{header_file} and {detail_file}")
        if header_file and detail_file:
            logger.info(f"NO BASE BUT HEADER DETAIL")
            header_path = os.path.normpath(os.path.join(DATA_DIR, header_file))
            detail_path = os.path.normpath(os.path.join(DATA_DIR, detail_file))

            header_df = pl.read_csv(header_path, null_values=["NA", "N/A", "null"], infer_schema_length=10000000)
            detail_df = pl.read_csv(detail_path, null_values=["NA", "N/A", "null"], infer_schema_length=10000000)

            # Join Header with Detail on RecKey
            semifinal_df = header_df.join(detail_df, on="RecKey", how="inner")

        # handling dust deposition
        elif stack_file and trapcollection_file:
            print(3)
            stack_path = os.path.normpath(os.path.join(DATA_DIR, stack_file))
            trapcollection_path = os.path.normpath(os.path.join(DATA_DIR, trapcollection_file))

            stack_df = pl.read_csv(stack_path, null_values=["NA", "N/A", "null"], infer_schema_length=10000000)
            trapcollection_df = pl.read_csv(trapcollection_path, null_values=["NA", "N/A", "null"], infer_schema_length=10000000)

            semifinal_df = stack_df.join(trapcollection_df,on="StackID", how="inner")

        # handling horizontal flux
        elif stack_file and box_file and boxcollection_file:
            stack_path = os.path.normpath(os.path.join(DATA_DIR, stack_file))
            box_path = os.path.normpath(os.path.join(DATA_DIR, box_file))
            boxcollection_path = os.path.normpath(os.path.join(DATA_DIR, boxcollection_file))

            stack_df = pl.read_csv(stack_path, null_values=["NA", "N/A", "null"], infer_schema_length=10000000)
            box_df = pl.read_csv(box_path, null_values=["NA", "N/A", "null"], infer_schema_length=10000000)
            boxcollection_df = pl.read_csv(boxcollection_path, null_values=["NA", "N/A", "null"], infer_schema_length=10000000)

            semifinal_df = box_df.join(stack_df, on="StackID", how="inner")
            semifinal_df = semifinal_df.join(boxcollection_df, on="BoxID", how="inner")

        elif pit_file and pithorizons_file:
            pit_path = os.path.normpath(os.path.join(DATA_DIR, pit_file))
            pithorizons_path = os.path.normpath(os.path.join(DATA_DIR, pithorizons_file))

            pit_df = pl.read_csv(pit_path, null_values=["NA", "N/A", "null"], infer_schema_length=10000000)
            pithorizons_df = pl.read_csv(pithorizons_path, null_values=["NA", "N/A", "null"], infer_schema_length=10000000)

            semifinal_df = pit_df.join(pithorizons_df, on="SoilKey", how="inner")
    except e:
        logger.info(f"error: {e}")

    finally:
        # Join header-detail with Lines-Plots using LineKey
        final_source_df = semifinal_df.join(lines_plots_df, on=lineplotjoin_key[data_type], how="inner", suffix="_right1")
        final_source_df = final_source_df.select([i for i in final_source_df.columns if "right" not in i])
        # else:
        #     final_source_df = header_detail_df

        # ensure only necessary columns are retained (keys and dates)
        key_columns = [col for col in final_source_df.columns if 'key' in col.lower() or 'date' in col.lower()]
        final_source_df = final_source_df.select(key_columns)
        date_columns = [i for i in final_source_df.columns if "date" in i.lower()]
        final_source_df = final_source_df.with_columns([
            final_source_df[col].str.strptime(pl.Date, "%m/%d/%y %H:%M:%S").dt.strftime("%Y-%m-%d").alias(col)
            for col in date_columns
        ])
        final_source_df = create_primary_key(final_source_df, ["PlotKey", pkdate_source[data_type]])

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
