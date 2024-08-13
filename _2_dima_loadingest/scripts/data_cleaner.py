from _2_dima_loadingest.config import TODAYS_DATE
import polars as pl
import logging

logger = logging.getLogger(__name__)


def add_date_loaded_column(df: pl.DataFrame) -> pl.DataFrame:
    try:
        df = df.with_columns(pl.lit(TODAYS_DATE).alias("DateLoadedInDb"))
        return df
    except Exception as e:
        logger.error(f"Error adding DateLoadedInDb column: {e}")
        return None

def deduplicate_dataframe(df: pl.DataFrame) -> pl.DataFrame:
    return df.unique()
