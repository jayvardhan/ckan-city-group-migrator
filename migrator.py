import json
import sys
import logging
import csv
import requests
import ckanapi
import urllib3
from collections import defaultdict


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
    SOURCE_API_KEY = config.get("SOURCE_API_KEY")

    try:
        params = {
            "q": f'city:"{city}"',
            "rows": 1000
        }
        headers = {"Authorization": SOURCE_API_KEY}
        r = requests.get(f"{SOURCE_CKAN_URL}/api/3/action/package_search", headers=headers, params=params)
        
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


# Delete existing tags from the site
def delete_tags(config, logger):
    json_path = 'source_tag_list.json'

    # Load CKAN config (URL + API token)
    ckan_url = config.get("STAGING_CKAN_URL")
    api_token = config.get("STAGING_API_KEY")

    if not (ckan_url and api_token):
        print("Missing TARGET_CKAN_URL or TARGET_API_KEY in config.")
        sys.exit(1)

    # Create a session that skips SSL verification
    session = requests.Session()
    session.verify = False

    # Connect to CKAN
    ckan = ckanapi.RemoteCKAN(ckan_url, apikey=api_token, session=session)

    logger.info(f"******* Starting Tag Deletion on {ckan_url}*********")

    # Load city → datasets mapping
    with open(json_path) as f:
        tags = json.load(f)
    
    for tag in tags['result']:
        try:
            ckan.action.tag_delete(id=tag)
            logger.info(f"Deleted tag {tag}")
        except Exception as e:
            logger.error(f"Exception occured while deleting tag {tag} : {e}")



# attach tags(previously theme) to its dataset
def patch_dataset_with_tag(config, logger):
    # dataset-tag json file path
    json_path = 'dataset_tag.json'

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

    logger.info(f"******* Patching Dataset with tag on {ckan_url} *********")

    # Load city → datasets mapping
    with open(json_path) as f:
        data = json.load(f)

    for dataset_id, tags_list in data.items():
        # Format the tags list into the required CKAN API format: [{'name': 'Tag1'}, ...]
        api_tags_format = [{'name': tag_name} for tag_name in tags_list]
    
        data_dict = {
            'id': dataset_id,
            'tags': api_tags_format
        }
        
        try:
            logger.info(f"Patching dataset '{dataset_id}' with tags: {tags_list}")
            ckan.call_action('package_patch', data_dict)
            logger.info(f"Successfully patched '{dataset_id}'.")
            
        except ckanapi.errors.CKANAPIError as e:
            logger.error(f"Error patching dataset: '{dataset_id}': {e}")
    
    logger.info("Completed Patching Dataset with Tags!")



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


# export group(theme) and its associated dataset to json file
def export_groups_to_json(config, output_file, logger):
    source_ckan_url = config.get("SOURCE_CKAN_URL")
    source_ckan_api = config.get("SOURCE_CKAN_API")

    results = []

    logger.info("*****Starting source group dataset fetch using ckanapi**********")

    # Create a session that skips SSL verification
    session = requests.Session()
    session.verify = False

    try:
        # Create CKAN API client
        ckan = ckanapi.RemoteCKAN(
            address=source_ckan_url,
            apikey=source_ckan_api,
            session=session
        )

        # 1. Fetch all groups
        try:
            groups = ckan.call_action('group_list', {"all_fields": True})
            logger.info(f"Found {len(groups)} groups on source CKAN.")
        except ckanapi.CKANAPIError as e:
            logger.error(f"Failed to get group list: {str(e)}")
            return []

        # 2. Iterate over groups
        for group in groups:
            group_name = group.get('name')

            logger.info(f"Fetching dataset list for group '{group_name}'")

            try:
                params = {
                    'id': group_name,
                    'include_dataset_count': True,
                    'include_datasets': True,
                    'include_extras': False,
                    'include_users': False,
                    'include_groups': False,
                    'include_tags': False,
                    'include_followers': False
                }

                group_info = ckan.call_action('group_show', params)

                datasets = [pkg['name'] for pkg in group_info.get('packages', [])]

                results.append({
                    "group_name": group_name,
                    "datasets": datasets
                })

                logger.info(f"Fetched {len(datasets)} datasets for group '{group_name}'")

            except ckanapi.NotFound:
                logger.warning(f"Group '{group_name}' not found, skipping.")
            except ckanapi.CKANAPIError as e:
                logger.error(f"API error fetching group '{group_name}': {e}")
            except Exception as e:
                logger.error(f"Unexpected error fetching '{group_name}': {e}")

        # 3. Save JSON
        try:
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(results, f, indent=2, ensure_ascii=False)

            logger.info(f"Export completed successfully. File saved to '{output_file}'.")
        except Exception as e:
            logger.error(f"Failed to write JSON file '{output_file}': {e}")

        return results

    except Exception as e:
        logger.error(f"Unexpected top-level error connecting to CKAN: {e}")

    return []



# helper function to create dataset-tag-mapping from group-dataset data
def prepare_dataset_tag_mapping(logger):
    json_input_path = 'group_dataset.json'
    json_output_path = 'dataset_tag.json'

    logger.info("******* Starting Dataset Tag mapping file *********")

    # Load city → datasets mapping
    with open(json_input_path) as f:
        input_data = json.load(f)

    # Use defaultdict to automatically create an empty list for a new dataset ID
    dataset_tags_map = defaultdict(list)

    for entry in input_data:
        tag = entry['tag_name']
        for dataset_id in entry['datasets']:
            # Append the tag to the dataset's list of tags
            dataset_tags_map[dataset_id].append(tag)

    # Convert defaultdict back to a regular dict for final output
    output_structure = dict(dataset_tags_map)

    try:
        with open(json_output_path, "w", encoding="utf-8") as f:
            json.dump(output_structure, f, indent=2, ensure_ascii=False)
            logging.info(f"Saved dataset-tag mapping to {json_output_path}")
    except Exception as e:
        logging.error(f"Error writing JSON file: {e}")





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
    # export_groups_to_json(config, "group_dataset.json", logger)
    
    # delete existing tags from the target site
    # delete_tags(config, logger)

    # prepare datset tag file
    # prepare_dataset_tag_mapping(logger)

    # patch dataset to attach tags(theme)
    # patch_dataset_with_tag(config, logger)


