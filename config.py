# Unique identifier field for incremental loading
INCREMENTAL_ID = 'FID'

# Filter to select only validated features (Validation status '1' means validated)
QUERY_VALIDATION_STATUS = "VALIDATION_STATUS='1'"

# List of fields to retrieve for each feature in the query
QUERY_FEATURE_FIELDS = (
    "OBJECTID, _ID, CLIENT_ID, FORM_ID, USER_LONG, USER_LAT, USER_DISTANCE, "
    "USER_NAME, USER_PHONE, USER_FRAME_ID, LONGITUDE, LATITUDE, "
    "DATE_SUBMITTED, ZONE_NAME, ZONE_CODE, STATE_NAME, FRAME_ID, LGA_NAME, "
    "LGA_CODE, RA_NAME, RA_CODE, STORE_ID, TYPE_OF_ADDR, HOUSE_NO, "
    "HOUSE_ADDRESS, STREET_NAME, OTHER_LOCATION, STORE_TWO_ADD, MORE_STORES, "
    "STORE_THREE_ADD, NAME, PHONE_NUMBER, RESPOND_STAT, NAME_BUS_OWNER, "
    "BUSINESS_PHONE, GENDER, DATE_REPORT, BUSINESS_NAME, DATE_ESTABLISH, "
    "PIC, CreationDate, Creator, EditDate, Editor, BUSINESS_AVAIL, "
    "BUS_TYPE, INSPIC, PERMIS_PIC, VALIDATION_STATUS, WILLING, ONBOARDED, "
    "BOARDED_PHONE, MUID, PROD_CAT"
)

# Parameters to count features in the dataset
QUERY_COUNT_PARAMS = {
    "where": '1=1',  # Condition to load all records
    'groupByFieldsForStatistics': '', 
    'orderByFields': '', 
    'time': 300,  
    'returnDistinctValues': 'true',  
    'outStatistics': [[
        {
            "statisticType": "count",  # Count the number of records
            "onStatisticField": INCREMENTAL_ID,  # Field to count
            "outStatisticFieldName": "COUNT" 
        }
    ]],
    'f': 'geojson'  # Return results in GeoJSON format
}

# Parameters for fetching feature data based on a query
QUERY_FETCH_PARAMS = {
    "where": "1=1",  # Load condition (default is to load all records)
    "geometryType": "esriGeometryEnvelope",  
    "spatialRel": "esriSpatialRelIntersects", 
    "units": "esriSRUnit_Meter",  # Unit of measure (meters)
    "relationParam": "",  
    "outFields": "*",  # Retrieve all specified fields
    "returnGeometry": "true",  # Include geometry in the result
    "featureEncoding": "esriDefault",  #
    "f": "geojson",  
    "resultOffset": 0,  # Start at the beginning of the result set
    "maxAllowableOffset": 100, 
    "returnExceededLimitFeatures": "true"  # Return all features even if limit is exceeded
}
