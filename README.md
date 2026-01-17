# smallfish-database
## Overview
- Python scripts are designed to clean, process, and validate data to be loaded into a small-bodied fish database for the Upper Colorado Endangered Fish Recovery Program.
- This work streamlines the data prep and cleaning process for up to 10 different projects spanning 40+ years of sampling and includes tens of thousands of fish, habitat, and environmental data points.
- The cleaned data and database facilitates query and look up abilities for native aquatic biologists working on endangered species recovery.

## Methods
- This workflow was developed using GitHub in collaboration with the database manager 

## Workflow
- Read site, haul, fish, and count CSVs
- Rename and reorder columns to match database schema
- Expand count data to encounter-level records
- Export cleaned staging tables

## How to Run
```bash
pip install -r requirements.txt
python scripts/run_data_processing.py
