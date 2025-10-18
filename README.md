## CKAN City Group Migrator

This script automates creating **city-based groups** and **attaching existing datasets** to them on a CKAN 2.11 site.

### Key Responsibilities

* Fetch datasets by city from a **source CKAN** instance.
* Generate a JSON file mapping each city to its datasets.
* Create a **group for each city** on the **target CKAN** instance.
* Attach datasets to their respective city groups.
* Skip existing groups or dataset links safely (idempotent).
* Log all actions to `city_group_migration.log`.

### Files

* `config.json` – CKAN URLs and API key.
* `city_list.csv` – List of cities to process(city,label).
* `datasets_by_city.json` – Auto-generated city–dataset mapping.
* `city_group_migration.log` – Log file with detailed activity.

### Usage

```bash
activate virtual environment
pip install ckanapi
python migrator.py
```

The script first builds `datasets_by_city.json` from the source CKAN and then creates groups and attaches datasets on the target CKAN.

---
