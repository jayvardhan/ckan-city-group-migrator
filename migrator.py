import json
import sys
import logging
import csv
import requests
import ckanapi
import urllib3


urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


# ----------------------------
# Load Configuration file
# ----------------------------

def load_config(config_file, logger):
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
def get_datasets_by_city(city, config, logger):
    
    SOURCE_CKAN_URL = config.get("SOURCE_CKAN_URL")

    try:
        params = {
            "q": f'?city="{city}"',
            "rows": 1000
        }
        r = requests.get(f"{SOURCE_CKAN_URL}/api/3/action/package_search", params=params, timeout=20)
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
def create_dataset_by_city(config, logger):
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

        dataset = get_datasets_by_city(city, config, logger)
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
def create_group_with_dataset(config, logger):
    
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





def export_groups_to_json(config, output_file, logger):
    source_ckan_url = config.get("SOURCE_CKAN_URL")
    
    # Ensure proper base URL
    base_api_url = source_ckan_url.rstrip('/') + '/api/3/action/'

    headers = {
        'Content-Type': 'application/json'
    }

    results = []

    logger.info(f"*****Starting source group dataset fetch**********")

    try:
        # Get group list
        group_list_url = base_api_url + 'group_list'
        params = {
            "all_fields": True
        }
        resp = requests.get(group_list_url, headers=headers, params=params, timeout=20)
        resp.raise_for_status()
        group_list = resp.json()
        if not group_list.get('success'):
            logger.error(f"Failed to get group list: {group_list}")
            return []

        groups = group_list['result']
        
        # print(json.dumps(groups, indent=2, ensure_ascii=False))
        logger.info(f"Found {len(groups)} groups on source CKAN.")

        # Get each group's datasets
        for group in groups:
            group_show_url = base_api_url + 'group_show'
            group_name = group.get('name')
            try:
                params = {
                    'id': group_name,
                    'include_dataset_count':True,
                    'include_datasets':True,
                    'include_extras':False,
                    'include_users':False,
                    'include_groups':False,
                    'include_tags':False,
                    'include_followers':False
                }
                r = requests.get(group_show_url, headers=headers, params=params, timeout=15)
                r.raise_for_status()
                data = r.json()
                
                if not data.get('success'):
                    logger.warning(f"Skipping group '{group_name}' due to API error: {data}")
                    continue

                group_info = data['result']
                
                datasets = [pkg['name'] for pkg in group_info.get('packages', [])]
                results.append({
                    "group_name": group_name,
                    "datasets": datasets
                })
                logger.info(f"Fetched {len(datasets)} datasets for group '{group_name}'")

            except requests.exceptions.RequestException as e:
                logger.error(f"Network error fetching group '{group_name}': {e}")
            except (KeyError, ValueError) as e:
                logger.error(f"Malformed response for group '{group_name}': {e}")
            except Exception as e:
                logger.error(f"Unexpected error fetching '{group_name}': {e}")

        # Write to JSON file
        try:
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(results, f, indent=2, ensure_ascii=False)
            logger.info(f"Export completed successfully. File saved to '{output_file}'.")
        except Exception as e:
            logger.error(f"Failed to write JSON file '{output_file}': {e}")

        return results

    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to connect to CKAN at '{source_ckan_url}': {e}")
    except Exception as e:
        logger.error(f"Unexpected top-level error: {e}")

    return []




if __name__ == "__main__":


    # Logging setup
    logging.basicConfig(
        filename="migration.log",
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s"
    )
    logger = logging.getLogger()

        
    # Configuration setup
    config_file = "config.json"
    config = load_config(config_file, logger)
    
    if not config:
        logger.error(f"Could not load configuration from {config_file}")
        sys.exit(1)

    logger.info("Loaded Configuration file")


    # generate city dataset json file
    # create_dataset_by_city(config, logger)

    # create group and attach dataset on target site
    # create_group_with_dataset(config, logger)

    # generate goup datasets json file
    export_groups_to_json(config, "group_dataset.json", logger)



    

    
