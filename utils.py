import os
import re
from shapely import Point
from dotenv import load_dotenv
import geopandas as gpd
from arcgis.gis import GIS
from logger import setup_logger

def login_to_arcgis():
    """Login to ArcGIS Online or Enterprise using environment variables for credentials."""
    # Load environment variables
    load_dotenv()
    url = os.getenv("URL")
    username = os.getenv("USER_NAME")
    password = os.getenv("PASSWORD")

    # Setup logger
    logger = setup_logger("ETL_PIPELINE")

    # Check for missing credentials and log an error if any are missing
    if not all([url, username, password]):
        logger.error("Missing URL, USER_NAME, or PASSWORD in environment variables.")
        return None

    try:
        # Authenticate using ArcGIS Online/Enterprise credentials
        gis = GIS(url, username, password)
        if gis:
            logger.info("Successfully authenticated.")
            token = gis._con.token  # Access the token for confirmation
            return token
        else:
            raise Exception("Authentication failed. Please check credentials.")

    except Exception as e:
        logger.error(f"Login failed: {e}")
        return None

def get_last_logged_id():
    """Extracts the last feature ID from the log file."""
    logger = setup_logger("ETL_PIPELINE")  # Initialize logger
    try:
        with open("logs/etl_log.txt", "r") as log_file:
            log_lines = log_file.readlines()
            for line in reversed(log_lines):
                if "last ID" in line:
                    match = re.search(r"last ID: (\d+)", line)
                    if match:
                        return int(match.group(1))
    except FileNotFoundError:
        logger.warning("Log file not found. Starting full load.")
    except Exception as e:
        logger.error(f"Error reading log file: {e}")
    return None

def update_last_logged_id(last_id):
    """Updates the last logged ID in the storage file."""
    logger = setup_logger("ETL_PIPELINE")  # Initialize logger
    with open("logs/etl_log.txt", 'a') as file:  # Use append mode to retain previous log entries
        file.write(f"last ID: {last_id}\n")  # Write the last ID in the log
        logger.info(f"Updated last logged ID to: {last_id}")

def convert_to_geodataframe(features):
    """Converts the fetched features into a GeoDataFrame."""
    logger = setup_logger("ETL_PIPELINE")  # Initialize logger
    if not features:
        logger.warning("No features to convert to GeoDataFrame.")
        return gpd.GeoDataFrame()

    # Create lists for geometries and properties
    geometries = []
    properties = []

    for feature in features:
        geom = feature.get('geometry')
        properties.append(feature.get('properties', {}))  # Safely append properties
        # Create a shapely geometry based on the geometry type
        if geom is not None:
            if geom['type'] == 'Point':
                geometries.append(Point(geom['coordinates'][0], geom['coordinates'][1]))
            else:
                # Handle other geometry types if needed
                logger.warning(f"Unsupported geometry type: {geom['type']}")
                geometries.append(None)  # Append None for unsupported geometries
        else:
            logger.warning("No geometry found for feature.")
            geometries.append(None)  # Append None if no geometry is found

    # Filter out any None geometries to avoid length mismatch
    valid_indices = [i for i, geom in enumerate(geometries) if geom is not None]
    filtered_properties = [properties[i] for i in valid_indices]
    filtered_geometries = [geometries[i] for i in valid_indices]

    # Combine geometries and properties into a GeoDataFrame
    gdf = gpd.GeoDataFrame(filtered_properties, geometry=filtered_geometries)
    return gdf