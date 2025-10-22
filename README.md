## OpenCity - CKAN data migrator

This script automates custom data migration from source site running on CKAN 2.8 to target CKAN 2.11 site for the OpenCity data portal. 

### Key Responsibilities

* Fetch datasets by city from a **source CKAN** instance.
* Generate a JSON file mapping each city to its datasets.
* Create a **group for each city** on the **target CKAN** instance.
* Attach datasets to their respective city groups.

* Fetch groups and its datasets from a **source CKAN** instance and generates JSON file.
* Create a **tag for each group** on the **target CKAN** instance.
* Attach datasets to their respective tag.

* Log all actions to `migration.log`.

### Files

* `config.json` – CKAN URLs and API key.
* `city_list.csv` – List of cities to process(city,label).
* `datasets_by_city.json` – Auto-generated city–dataset mapping.
* `group_dataset.json` – Auto-generated group–dataset mapping.
* `migration.log` – Log file with detailed activity.

### Usage

```bash
activate virtual environment
pip install ckanapi
python migrator.py
```

---
