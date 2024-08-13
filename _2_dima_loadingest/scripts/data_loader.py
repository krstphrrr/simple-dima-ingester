import polars as pl
import logging

logger = logging.getLogger(__name__)

def load_csv(file_path: str) -> pl.DataFrame:
    try:
        df = pl.read_csv(file_path)
        return df
    except Exception as e:
        logger.error(f"Error loading CSV: {e}")
        return None
