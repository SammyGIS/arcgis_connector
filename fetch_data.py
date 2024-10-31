import httpx
import json
import os
from dotenv import load_dotenv
from config import QUERY_FETCH_PARAMS, QUERY_COUNT_PARAMS, INCREMENTAL_ID
from logger import setup_logger
from utils import login_to_arcgis, get_last_logged_id, update_last_logged_id, convert_to_geodataframe

# Load environment variables from a .env file
load_dotenv()

# Configure the logger to log messages to a file
logger = setup_logger("ArcGIS_Feature_Collector", log_file="etl_log.txt")

# Service URL for the ArcGIS FeatureServer
service_url = os.getenv("ARCGIS_SERVICE_URL")
query_url = f"{service_url}/0/query"

# Global variable to store the ArcGIS token
token = None

def get_token():
    """Retrieve or generate the ArcGIS token."""
    global token
    if not token:
        try:
            token = login_to_arcgis()  # Log in to get a new token
            logger.info("Token successfully generated.")
        except Exception as e:
            logger.error(f"Error generating token: {e}")
    return token  # Return the token

def count_features(query_url, query_count_params):
    """Counts the total number of features available at the given service URL."""
    try:
        query_count_params['token'] = get_token()  # Include the token in the query parameters
        count_response = httpx.get(query_url, params=query_count_params)
        count_response.raise_for_status()

        # Extract the total count of features from the response
        data_length = count_response.json().get('features', [])[0].get('properties', {}).get('COUNT', 0)
        logger.info(f"Total features to fetch: {data_length}")  
        return data_length 
    except httpx.HTTPStatusError as e:
        logger.error(f"HTTP error occurred while counting features: {e.response.status_code} - {e.response.text}") 
    except Exception as e:
        logger.error(f"Error occurred while counting features: {e}") 
    return 0  

def fetch_features(query_url, fetch_params, total_features, batch_size=2000):
    """Fetches features from the service URL in batches and returns them as a list along with the last ID."""
    all_features = []  
    result_offset = 0  
    last_id = None 

    while result_offset < total_features:  
        try:
            fetch_params['resultOffset'] = result_offset  # Set the current offset for the request
            fetch_params['token'] = get_token() 
            response = httpx.get(query_url, params=fetch_params)  
            response.raise_for_status()  

            response_data = response.json()  # Parse the JSON response
            if 'features' not in response_data or len(response_data['features']) == 0:
                logger.warning("No features found in the current batch.") 
                break 

            all_features += response_data['features']  # Append the fetched features to the list
            last_id = response_data['features'][-1]['properties'][INCREMENTAL_ID]  # Update last_id with the latest ID
            logger.info(f"Fetched {len(response_data['features'])} features, last ID: {last_id}") 
            result_offset += batch_size 

        except httpx.HTTPStatusError as e:
            logger.error(f"HTTP error occurred while fetching data: {e.response.status_code} - {e.response.text}") 
            break  # Exit the loop on error
        except Exception as e:
            logger.error(f"Error occurred while fetching data: {e}")  # Log any other errors
            break  # Exit the loop on error

    logger.info("Data collection complete")  # Log completion of data collection
    return all_features, last_id  # Return the collected features and the last ID

def incremental_load_as_json():
    """Loads only the new data from the last feature ID logged and returns as JSON."""
    last_id = get_last_logged_id()  # Get the last logged ID
    if last_id:
        logger.info(f"Starting incremental load from last ID: {last_id}")
        QUERY_FETCH_PARAMS['where'] = f"id > {last_id}"  # Filter to get only new records

    total_features = count_features(query_url, QUERY_COUNT_PARAMS) 
    if total_features > 0: 
        features, new_last_id = fetch_features(query_url, QUERY_FETCH_PARAMS, total_features)  
        logger.info(f"Total new features collected: {len(features)}")  

        features_json = json.dumps(features, indent=4)  # Convert features to JSON format
        # print(features_json)

        if new_last_id:
            logger.info(f"Updating last ID to: {new_last_id}")
            update_last_logged_id(new_last_id)  # Store the new last ID for future use

        return features_json  # Return the JSON output
    else:
        logger.warning("No new features found.")  
        return None 

def incremental_load_as_geodataframe():
    """Loads only the new data from the last feature ID logged and returns as a GeoDataFrame."""
    last_id = get_last_logged_id()  # Get the last logged ID
    if last_id:
        logger.info(f"Starting incremental load from last ID: {last_id}")
        QUERY_FETCH_PARAMS['where'] = f"id > {last_id}"  # Filter to get only new records

    total_features = count_features(query_url, QUERY_COUNT_PARAMS) 
    if total_features > 0: 
        features, new_last_id = fetch_features(query_url, QUERY_FETCH_PARAMS, total_features)  # Fetch features
        logger.info(f"Total new features collected: {len(features)}")  

        gdf = convert_to_geodataframe(features) 
        # print(gdf)  

        if new_last_id:
            logger.info(f"Updating last ID to: {new_last_id}")
            update_last_logged_id(new_last_id)  # Store the new last ID for future use

        return gdf 
    else:
        logger.warning("No new features found.")  # Log if no new features are found
        return None 

def full_load_as_json():
    """Full load to load all the data in the feature service layer and return as JSON."""
    total_features = count_features(query_url, QUERY_COUNT_PARAMS) 

    if total_features > 0:  # Proceed if there are features to fetch
        features, last_id = fetch_features(query_url, QUERY_FETCH_PARAMS, total_features) 

        features_json = json.dumps(features, indent=4)  # Convert features to JSON format
        print(features_json)  
        if last_id:
            logger.info(f"Last ID after full load: {last_id}")
            update_last_logged_id(last_id)  # Store the last ID after full load

        return features_json 
    else:
        logger.warning("No features found.")  
        return None 

def full_load_as_geodataframe():
    """Full load to load all the data in the feature service layer and return as a GeoDataFrame."""
    total_features = count_features(query_url, QUERY_COUNT_PARAMS)

    if total_features > 0:  # Proceed if there are features to fetch
        features, last_id = fetch_features(query_url, QUERY_FETCH_PARAMS, total_features)
        logger.info(f"Total features collected: {len(features)}") 

        gdf = convert_to_geodataframe(features)  # Convert features to a GeoDataFrame
        # print(gdf) 

        if last_id:
            logger.info(f"Last ID after full load: {last_id}")
            update_last_logged_id(last_id)  # Store the last ID after full load

        return gdf 
    else:
        logger.warning("No features found.")  
        return None  