import json
import sys
import logging
import csv
import requests
import ckanapi
import urllib3


urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


# ----------------------------
# Logging setup
# ----------------------------
logging.basicConfig(
    filename="city_group_migration.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger()


# ----------------------------
# Load Configuration file
# ----------------------------

def load_config(config_file):
    """Load configuration from a JSON file"""
    try:
        with open(config_file, 'r') as f:
            config = json.load(f)
        
        required_keys = ["SOURCE_CKAN_URL", "TARGET_CKAN_URL", "TARGET_API_KEY"]
        missing_keys = [key for key in required_keys if key not in config]
        
        if missing_keys:
            logger.error(f"Configuration file is missing the following required keys: {', '.join(missing_keys)}")
            return None
        
        return config
    except Exception as e:
        logger.error(f"Error loading configuration file: {e}")
        return None


# Returns datsets names for given city
def get_datasets_by_city(city, config):
    
    SOURCE_CKAN_URL = config.get("SOURCE_CKAN_URL")

    try:
        params = {
            "q": f'?city="{city}"',
            "rows": 1000
        }
        r = requests.get(f"{SOURCE_CKAN_URL}/package_search", params=params, timeout=20)
        r.raise_for_status()
        data = r.json()

        if not data.get("success"):
            logger.warning(f"Search API failed for city '{city}': {data}")
            return []

        results = [pkg["name"] for pkg in data["result"]["results"]]
        logger.info(f"Found {len(results)} datasets for city '{city}'")
        return results

    except Exception as e:
        logger.error(f"Error fetching datasets for city '{city}': {e}")
        return []


# Creates a city-dataset JSON file
def create_dataset_by_city(config):
    #city list csv file path
    city_list_file = "city_list.csv"

    # Load cities from the csv file
    cities = []    
    try:
        with open(city_list_file, newline='', encoding='utf-8') as csvfile:
            data = csv.DictReader(csvfile)
            cities = list(data)
    except Exception as e:
        logger.error(e)


    # Create city dataset mapping
    city_dataset_map = {}
    for record in cities:
        city = record.get("city", "").strip()
        label = record.get("label", "").strip()
        if not city:
            continue

        logger.info(f"Processing city: {city} ({label})")

        dataset = get_datasets_by_city(city, config)
        city_dataset_map[city] = dataset
    
    # Save city-dataset mapping to JSON file
    OUTPUT_JSON_PATH = "datasets_by_city.json"
    try:
        with open(OUTPUT_JSON_PATH, "w", encoding="utf-8") as f:
            json.dump(city_dataset_map, f, indent=2, ensure_ascii=False)
            logging.info(f"Saved dataset mapping to {OUTPUT_JSON_PATH}")
    except Exception as e:
        logging.error(f"Error writing JSON file: {e}")



# Creates Group and attaches its datasets on target CKAN site
def create_group_with_dataset(config):
    
    # city dataset json file path
    json_path = 'datasets_by_city.json'

    # Load CKAN config (URL + API token)
    ckan_url = config.get("TARGET_CKAN_URL")
    api_token = config.get("TARGET_API_KEY")

    if not (ckan_url and api_token):
        print("Missing TARGET_CKAN_URL or TARGET_API_KEY in config.")
        sys.exit(1)

    # Create a session that skips SSL verification
    session = requests.Session()
    session.verify = False

    # Connect to CKAN
    ckan = ckanapi.RemoteCKAN(ckan_url, apikey=api_token, session=session)

    logger.info("******* Starting Group And Dataset Creation *********")

    # Load city → datasets mapping
    with open(json_path) as f:
        city_map = json.load(f)

    for city, datasets in city_map.items():
        group_name = city.lower().replace(" ", "-")
        logger.info(f"Processing city: {city}  (group: {group_name})")

        # Ensure group exists or create it safely
        try:
            ckan.action.group_show(id=group_name)
            logger.info(f"Group '{group_name}' already exists.")
        except ckanapi.errors.NotFound:
            logger.info(f"Creating group '{group_name}'...")
            try:
                ckan.action.group_create(
                    name=group_name,
                    title=city,
                    description=f"Datasets related to {city}"
                )
                logger.info(f"Created group '{group_name}' successfully.")
            except ckanapi.errors.ValidationError as e:
                # Handle race condition where group exists but wasn't visible earlier
                if "Group name already exists" in str(e.error_dict):
                    logger.info(f"Group '{group_name}' already exists (caught during creation). Continuing...")
                else:
                    logger.error(f"Failed to create group '{group_name}': {e.error_dict}")
                    continue  # Skip to next city

        # Add datasets to group
        for dataset_name in datasets:
            try:
                pkg = ckan.action.package_show(id=dataset_name)
                logger.info(f"Attaching dataset: {dataset_name}")
                ckan.action.member_create(
                    id=group_name,
                    object=pkg["id"],
                    object_type="package",
                    capacity="public"
                )
            except ckanapi.errors.NotFound:
                logger.warning(f"Dataset '{dataset_name}' not found, skipping.")
            except ckanapi.errors.ValidationError as e:
                # Often occurs if dataset is already in the group
                logger.info(f"Skipping '{dataset_name}' (Validation error: {e.error_dict})")

    logger.info("Completed Group and Dataset Creation!")




if __name__ == "__main__":
        
    # configuration file path
    config_file = "config.json"

    # Load configuration from file
    config = load_config(config_file)
    
    if not config:
        logger.error(f"Could not load configuration from {config_file}")
        sys.exit(1)

    logger.info("Loaded Configuration file")

    # generate city dataset json file
    create_dataset_by_city(config)

    # create group and attach dataset on target site
    create_group_with_dataset(config)



    

    
