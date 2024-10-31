# ETL Project for ArcGIS Feature Collection
This project is an ETL pipeline designed to extract data from an ArcGIS Feature Service, transform it into various formats (JSON, GeoDataFrame), and load it into storage destinations such as a PostgreSQL database, CSV, Shapefile, or GeoJSON.

## Table of Contents

- [Setup](#setup)
- [Project Structure](#project-structure)
- [Environment Variables](#environment-variables)
- [Scripts Overview](#scripts-overview)
  - [fetch_data.py](#fetch_datapy)
  - [layer_saver.py](#layer_saverpy)
  - [config.py](#configpy)
  - [utils.py](#utilspy)
- [Usage](#usage)
  - [Running Data Fetch and Load](#running-data-fetch-and-load)
- [Dependencies](#dependencies)

---

## Setup

1. Clone the repository and navigate to the project directory:

   ```bash
   git clone https://github.com/your-username/ETL_Project.git
   cd ETL_Project
   ```

2. Install the required dependencies using `pip`:

   ```bash
   pip install -r requirements.txt
   ```

3. Configure the environment variables in the `.env` file.

## Project Structure

```plaintext
ETL_Project/
├── .env                   # Environment variables for credentials and configuration
├── README.md              # Project documentation
├── config.py              # Configuration parameters
├── fetch_data.py          # Fetch data from ArcGIS
├── layer_saver.py         # Save data to various formats
├── utils.py               # Utility functions (e.g., authentication, data conversion)
├── logger.py              # Logger setup
└── requirements.txt       # Python dependencies
```

## Environment Variables

Set up your `.env` file with the following details for ArcGIS and PostgreSQL authentication:

```plaintext
# ArcGIS Online Credentials
URL = "https://your-arcgis-url.com"
USER_NAME="YourUsername"
PASSWORD="YourPassword"

# ArcGIS Service URL
ARCGIS_SERVICE_URL='https://services.arcgis.com/your_service_url/FeatureServer'

# PostgreSQL Database Credentials
DATABASE_URL="postgresql://username:password@localhost:5432/your_database"
```

## Scripts Overview

### `fetch_data.py`
This script handles:
- Token generation and authentication to ArcGIS.
- Counting features in the ArcGIS Feature Service.
- Fetching features incrementally or fully based on the last logged ID.
- Converting fetched data to JSON or GeoDataFrame format for further processing.

### `layer_saver.py`

This script allows saving the GeoDataFrame in various formats:
- **CSV**: Saves data as a CSV file.
- **Shapefile**: Saves data in ESRI Shapefile format.
- **GeoJSON**: Saves data as a GeoJSON file.
- **PostgreSQL**: Pushes data to a PostgreSQL database.

### `config.py`

Defines configuration parameters such as:
- Fields to fetch from the feature service.
- Query parameters for data retrieval.

### `utils.py`

Contains utility functions for:
- ArcGIS login and token management.
- Converting feature data to GeoDataFrame.
- Additional helper functions.

## Usage

### Running Data Fetch and Load
## Usage

You can use the following commands in a Python script to perform both full loads and incremental loads.

### Full Load as JSON

Run the following code to perform a full load and output the results in JSON format:

```python
from your_module import full_load_as_json

def main():
    # Full Load as JSON
    print("Performing full load as JSON...")
    full_data_json = full_load_as_json()
    if full_data_json:
        print("Full Load Data (JSON):")
        print(full_data_json)

if __name__ == "__main__":
    main()
```

### Full Load as GeoDataFrame

To perform a full load and output the results as a GeoDataFrame, use the following code:

```python
from your_module import full_load_as_geodataframe

def main():
    # Full Load as GeoDataFrame
    print("\nPerforming full load as GeoDataFrame...")
    full_data_gdf = full_load_as_geodataframe()
    if full_data_gdf is not None:
        print("Full Load Data (GeoDataFrame):")
        print(full_data_gdf)

if __name__ == "__main__":
    main()
```

### Incremental Load as JSON

To perform an incremental load and output new data in JSON format, use the following code:

```python
from your_module import incremental_load_as_json

def main():
    # Incremental Load as JSON
    print("\nPerforming incremental load as JSON...")
    incremental_data_json = incremental_load_as_json()
    if incremental_data_json:
        print("Incremental Load Data (JSON):")
        print(incremental_data_json)

if __name__ == "__main__":
    main()
```

### Incremental Load as GeoDataFrame

For an incremental load that outputs new data as a GeoDataFrame, use this code:

```python
from your_module import incremental_load_as_geodataframe

def main():
    # Incremental Load as GeoDataFrame
    print("\nPerforming incremental load as GeoDataFrame...")
    incremental_data_gdf = incremental_load_as_geodataframe()
    if incremental_data_gdf is not None:
        print("Incremental Load Data (GeoDataFrame):")
        print(incremental_data_gdf)

if __name__ == "__main__":
    main()
```

## Function Descriptions

- **full_load_as_json()**: Fetches all features from the ArcGIS Feature Service and returns them as a JSON object.
- **full_load_as_geodataframe()**: Fetches all features from the ArcGIS Feature Service and returns them as a GeoDataFrame.
- **incremental_load_as_json()**: Fetches only new features added since the last recorded ID and returns them as a JSON object.
- **incremental_load_as_geodataframe()**: Fetches only new features added since the last recorded ID and returns them as a GeoDataFrame.

3. **Save Data**: After fetching data, save it using `layer_saver.py`:

   ```bash
   python layer_saver.py --output-format csv --file-path "output.csv"
   ```

## Dependencies

Install dependencies from `requirements.txt`:

```bash
pip install -r requirements.txt
```

