import geopandas as gpd
from sqlalchemy import create_engine
from typing import Optional
from logger import setup_logger
from dotenv import load_dotenv
import os

load_dotenv()

# Load environment variables for database connection
DATABASE = os.getenv("DATABASE_URL")
TABLE_NAME = os.getenv("TABLE_NAME")

# Setup logger
logger = setup_logger("ETL_PIPELINE")

def save_to_csv(gdf: gpd.GeoDataFrame, file_path: str) -> None:
    """Save a GeoDataFrame to a CSV file."""
    try:
        gdf.to_csv(file_path, index=False)
        logger.info(f"Saved GeoDataFrame to CSV at {file_path}.")
    except Exception as e:
        logger.error(f"Failed to save GeoDataFrame to CSV: {e}")

def save_to_shapefile(gdf: gpd.GeoDataFrame, file_path: str) -> None:
    """Save a GeoDataFrame to a Shapefile."""
    try:
        gdf.to_file(file_path, driver='ESRI Shapefile')
        logger.info(f"Saved GeoDataFrame to Shapefile at {file_path}.")
    except Exception as e:
        logger.error(f"Failed to save GeoDataFrame to Shapefile: {e}")

def save_to_geojson(gdf: gpd.GeoDataFrame, file_path: str) -> None:
    """Save a GeoDataFrame to a GeoJSON file."""
    try:
        gdf.to_file(file_path, driver='GeoJSON')
        logger.info(f"Saved GeoDataFrame to GeoJSON at {file_path}.")
    except Exception as e:
        logger.error(f"Failed to save GeoDataFrame to GeoJSON: {e}")

def push_to_postgres(gdf: gpd.GeoDataFrame, 
                     table_name: Optional[str] = None, 
                     db_url: Optional[str] = None) -> None:
    """Push a GeoDataFrame to a PostgreSQL database."""
    if gdf.empty:
        logger.warning("GeoDataFrame is empty. No data to push to PostgreSQL.")
        return

    # Set CRS if not already set
    if gdf.crs is None:
        logger.warning("CRS not defined. Setting to EPSG:4326.")
        gdf.set_crs(epsg=4326, inplace=True)

    table_name = table_name or TABLE_NAME  # Use provided table name or environment variable
    db_url = db_url or DATABASE  # Use provided db URL or environment variable
    
    try:
        engine = create_engine(db_url)
        gdf.to_postgis(table_name, engine, if_exists='replace')
        logger.info(f"Pushed GeoDataFrame to PostgreSQL table '{table_name}'.")
    except Exception as e:
        logger.error(f"Failed to push GeoDataFrame to PostgreSQL: {e}")