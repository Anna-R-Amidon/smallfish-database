{
 "cells": [
  {
   "cell_type": "markdown",
   "id": "0dc1cb3c-0380-4f4f-9a48-2fc15ac3fd2e",
   "metadata": {},
   "source": [
    "# 138 Data Processing Script 🐟🐠🐡\n",
    "\n",
    "This script takes raw source 138 data (Data Plus) and converts it into a format suitable for database import.\n",
    "\n",
    "## Steps\n",
    "\n",
    "### 1. Import Modules and Set Working Directories\n",
    "- Define the path to your **raw source data**.\n",
    "- Define the path for **outputs from data renaming and reordering**.\n",
    "\n",
    "### 2. Rename and Reorder CSV Files\n",
    "- Process the `site`, `haul`, `fish`, and `count` CSV files to match the **database import schema**.\n",
    "- **Check for duplicate columns** in each dataframe.\n",
    "- **Print column headers and number of rows** to cross-check with the original data.\n",
    "\n",
    "### 2.5 Expand Count Data\n",
    "- Transform `count` data so that **each counted fish receives its own unique `enc_key`** and row of data.\n",
    "\n",
    "### 3. Prepare Staging Tables\n",
    "- Organize cleaned and restructured data into **import-ready tables** for `site`, `haul`, `fish`, `count`, and `encounters`.\n",
    "\n",
    "### 4. Save Output\n",
    "- Save all cleaned and staged tables to the **defined output paths**.\n",
    "- This final output path is seperate from the inputs and outputs from the data cleaning and reordering\n"
   ]
  },
  {
   "cell_type": "markdown",
   "id": "c9918cfd-f5f6-4695-9e37-b2d0700c9951",
   "metadata": {},
   "source": [
    "# 1. Import Modules and Set Working Directories"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "id": "625be393",
   "metadata": {},
   "outputs": [],
   "source": [
    "import sys\n",
    "import pandas as pd\n",
    "import numpy as np\n",
    "from pathlib import Path\n",
    "\n",
    "\n",
    "# Add project root to Python path so we can import schema_config\n",
    "project_root = Path.cwd().parent\n",
    "sys.path.insert(0, str(project_root))"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "id": "4e6c1342",
   "metadata": {},
   "outputs": [],
   "source": [
    "from schema_config import (\n",
    "    STAGING_SITE_COLUMNS,\n",
    "    STAGING_HAUL_COLUMNS,\n",
    "    STAGING_ENCOUNTER_COLUMNS,\n",
    "    RAW_TO_DB_MAPPING\n",
    ")\n",
    "\n",
    "#sys.path.insert(0, str(project_root))"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "id": "98ebffc1-d1c5-4e10-bf97-48be886640ab",
   "metadata": {},
   "outputs": [],
   "source": [
    "# Set relative filepaths\n",
    "notebook_dir = Path.cwd()\n",
    "project_root = notebook_dir.parent\n",
    "\n",
    "input_data_dir = project_root / 'data/processed_data'\n",
    "output_data_dir = project_root / 'data/clean_data'\n",
    "\n",
    "#print(input_data_dir)\n",
    "print(input_data_dir / 'count_readable.csv')"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "id": "a8a77def-71ed-4fe1-8620-f99f41454062",
   "metadata": {},
   "outputs": [],
   "source": [
    "# Read CSVs into dictionary\n",
    "dfs = {file.stem: pd.read_csv(file) for file in input_data_dir.glob('*.csv')}\n",
    "df_count = dfs['count_readable']\n",
    "df_fish  = dfs['fish_readable']\n",
    "df_haul  = dfs['haul_readable']\n",
    "df_site  = dfs['site_readable']\n",
    "\n",
    "print(\"Data loaded successfully.\")"
   ]
  },
  {
   "cell_type": "markdown",
   "id": "c3f8ec9b-14ea-4c6c-9395-64b0a66ecae3",
   "metadata": {},
   "source": [
    "# 2. Rename and Reorder CSV Files to match db schema\n",
    "- Process the `site`, `haul`, `fish`, and `count` CSV files to match the **database import schema**.\n",
    "- **Check for duplicate columns** in each dataframe.\n",
    "- **Print column headers and number of rows** to cross-check with the original data.\n",
    "  \n",
    "### lines with # exist in the raw data but no match in the db"
   ]
  },
  {
   "cell_type": "markdown",
   "id": "9d495079-fc2f-4647-8430-62bfaf9aa2a5",
   "metadata": {},
   "source": [
    "## Site"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "id": "8289c3a9-11df-4a05-942b-5c2090e5fa9d",
   "metadata": {},
   "outputs": [],
   "source": [
    "# Rename columns\n",
    "df_site_cleaned = df_site.rename(columns=RAW_TO_DB_MAPPING['site'])\n",
    "\n",
    "# Check for duplicate columns \n",
    "duplicate_cols = df_site_cleaned.columns[df_site_cleaned.columns.duplicated()].tolist()\n",
    "if duplicate_cols:\n",
    "    print(\"⚠️ Duplicate columns found in df_site_cleaned:\", duplicate_cols)\n",
    "else:\n",
    "    print(\"No duplicate columns found in df_site_cleaned.\")\n",
    "\n",
    "# Validate columns - check for missing columns, add and fill with NA\n",
    "missing_cols = set(STAGING_SITE_COLUMNS) - set(df_site_cleaned.columns)\n",
    "if missing_cols:\n",
    "    print(\"\\nColumns missing from site dataframe: \")\n",
    "    for col in missing_cols:\n",
    "        print(\"- \", col)\n",
    "\n",
    "# Add missing data\n",
    "df_site_cleaned['biologist'] = 'Amadon, Anna'\n",
    "df_site_cleaned['org_code'] = 'UDWR-M'\n",
    "df_site_cleaned['study_code'] = 138\n",
    "df_site_cleaned['gear_type_code'] = 'SE'\n",
    "#df_site_cleaned['record_status'] = 'Valid'\n",
    "\n",
    "df_site_cleaned['habitat_key'] = df_site_cleaned['sample_key'].astype(str) + \"_hab\"     # For seining habitat_key == sample_key\n",
    "\n",
    "conditions = [\n",
    "    (df_site_cleaned['hydro_area_code'] == 'CO') & (df_site_cleaned['estimated_river_mile'] <= 115),\n",
    "    (df_site_cleaned['hydro_area_code'] == 'GR') & (df_site_cleaned['estimated_river_mile'] > 128),\n",
    "    (df_site_cleaned['hydro_area_code'] == 'GR') & (df_site_cleaned['estimated_river_mile'] <= 128),\n",
    "]\n",
    "choices = ['Lower Colorado River', 'Middle Green River', 'Lower Green River']\n",
    "\n",
    "df_site_cleaned['site_or_reach_name'] = np.select(conditions, choices, default='Unknown')\n",
    "\n",
    "# Reindex: adds missing columns (NaN), removes extras, reorders\n",
    "df_site_cleaned = df_site_cleaned.reindex(columns=STAGING_SITE_COLUMNS)\n",
    "\n",
    "# Save the cleaned CSV\n",
    "df_site_cleaned.to_csv(output_data_dir / 'site_cleaned.csv', index=False)   # <-- actually saves the CSV\n",
    "\n",
    "print(\"\\ndf_site cleaned, columns renamed, and saved\")\n",
    "print(\"Columns in cleaned df_site:\", df_site_cleaned.columns.tolist())\n",
    "print(\"Number of rows in table:\", len(df_site_cleaned))\n",
    "# print(df_site_cleaned.head())"
   ]
  },
  {
   "cell_type": "markdown",
   "id": "a16d6acc-eba3-4d58-a677-de849cd0aa9f",
   "metadata": {},
   "source": [
    "## Haul"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "id": "a26be6c9-19d4-4d38-952f-0c91b0ba4e52",
   "metadata": {},
   "outputs": [],
   "source": [
    "# Apply renaming \n",
    "df_haul_cleaned = df_haul.rename(columns=RAW_TO_DB_MAPPING['haul'])\n",
    "\n",
    "# Check for duplicate columns \n",
    "duplicate_cols = df_haul_cleaned.columns[df_haul_cleaned.columns.duplicated()].tolist()\n",
    "\n",
    "if duplicate_cols:\n",
    "    print(\"⚠️ Duplicate columns found in df_haul_cleaned:\", duplicate_cols)\n",
    "else:\n",
    "    print(\"No duplicate columns found in df_haul_cleaned.\")\n",
    "\n",
    "# Validate columns\n",
    "missing_cols = set(STAGING_HAUL_COLUMNS) - set(df_haul_cleaned.columns)\n",
    "if missing_cols:\n",
    "    print(\"\\nColumns missing from haul dataframe: \")\n",
    "    for col in missing_cols:\n",
    "        print(\"- \", col)\n",
    "\n",
    "# Add missing data\n",
    "# df_haul_cleaned['record_status'] = 'Valid'\n",
    "df_haul_cleaned['ismp_seine_method'] = df_haul_cleaned['ismp_seine_method'].replace({'P': 'Parallel', 'A': 'Across'})\n",
    "\n",
    "# Reindex: adds missing columns (NaN), removes extras, reorders\n",
    "df_haul_cleaned = df_haul_cleaned.reindex(columns=STAGING_HAUL_COLUMNS)\n",
    "\n",
    "# Save the cleaned CSV\n",
    "df_haul_cleaned.to_csv(output_data_dir / 'haul_cleaned.csv', index=False)\n",
    "\n",
    "print(\"\\ndf_haul cleaned, columns renamed, and saved\")\n",
    "print(\"Columns in cleaned df_haul:\", df_haul_cleaned.columns.tolist())\n",
    "print(\"Number of rows in table:\", len(df_haul_cleaned))"
   ]
  },
  {
   "cell_type": "markdown",
   "id": "ef99814d-648d-4d26-b538-fdb525f28b2e",
   "metadata": {},
   "source": [
    "## Fish"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "id": "eb8e4699-a0c8-408e-b4c8-67e62adabb7b",
   "metadata": {},
   "outputs": [],
   "source": [
    "# Rename columns\n",
    "df_fish_cleaned = df_fish.rename(columns=RAW_TO_DB_MAPPING['fish'])\n",
    "\n",
    "# Check for duplicate columns \n",
    "duplicate_cols = df_fish_cleaned.columns[df_fish_cleaned.columns.duplicated()].tolist()\n",
    "\n",
    "if duplicate_cols:\n",
    "    print(\"⚠️ Duplicate columns found in df_fish_cleaned:\", duplicate_cols)\n",
    "else:\n",
    "    print(\"No duplicate columns found in df_fish_cleaned.\")\n",
    "\n",
    "# Validate that all expected columns exist\n",
    "missing_cols = set(STAGING_ENCOUNTER_COLUMNS) - set(df_fish_cleaned.columns)\n",
    "if missing_cols:\n",
    "    print(\"\\nColumns missing from fish dataframe: \")\n",
    "    for col in missing_cols:\n",
    "        print(\"- \", col)\n",
    "\n",
    "# Add required data\n",
    "df_fish_cleaned['fish_count'] = 1\n",
    "\n",
    "# Reindex: adds missing columns (NaN), removes extras, reorders\n",
    "df_fish_cleaned = df_fish_cleaned.reindex(columns=STAGING_ENCOUNTER_COLUMNS)\n",
    "\n",
    "# Save the cleaned CSV\n",
    "df_fish_cleaned.to_csv(output_data_dir / 'fish_cleaned.csv', index=False)\n",
    "\n",
    "print(\"\\ndf_fish cleaned, columns renamed, and saved\")\n",
    "print(\"Columns in cleaned df_fish:\", df_fish_cleaned.columns.tolist())\n",
    "print(\"Number of rows in staging table:\", len(df_fish_cleaned))"
   ]
  },
  {
   "cell_type": "markdown",
   "id": "0b00c671-a184-4573-a42a-d5b6cf6b4e9c",
   "metadata": {},
   "source": [
    "## Count"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "id": "1a32f1d5-25c4-4290-b23e-d99cf8a64a56",
   "metadata": {},
   "outputs": [],
   "source": [
    "#  Rename columns ---\n",
    "df_count_cleaned = df_count.rename(columns=RAW_TO_DB_MAPPING['count'])\n",
    "\n",
    "# Remove duplicate columns just in case ---\n",
    "duplicate_cols = df_count_cleaned.columns[df_count_cleaned.columns.duplicated()].tolist()\n",
    "\n",
    "if duplicate_cols:\n",
    "    print(\"⚠️ Duplicate columns found in df_count_cleaned:\", duplicate_cols)\n",
    "else:\n",
    "    print(\"No duplicate columns found in df_count_cleaned.\")\n",
    "\n",
    "# Validate expected columns\n",
    "missing_cols = set(STAGING_ENCOUNTER_COLUMNS) - set(df_count_cleaned.columns)\n",
    "if missing_cols:\n",
    "    print(\"\\nColumns missing from count dataframe: \")\n",
    "    for col in missing_cols:\n",
    "        print(\"- \", col)\n",
    "\n",
    "\n",
    "# Reindex: adds missing columns (NaN), removes extras, reorders\n",
    "df_count_cleaned = df_count_cleaned.reindex(columns=STAGING_ENCOUNTER_COLUMNS)\n",
    "\n",
    "# Save the cleaned CSV\n",
    "df_count_cleaned.to_csv(output_data_dir / 'count_cleaned.csv', index=False)\n",
    "print(\"\\ndf_count cleaned, columns renamed, and saved\")\n",
    "print(\"Columns in cleaned df_count:\", df_count_cleaned.columns.tolist())\n",
    "print(\"Number of rows in table:\", len(df_count_cleaned))"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "id": "26fcb7d6",
   "metadata": {},
   "outputs": [],
   "source": [
    "# create encounter dataframe\n",
    "\n",
    "df_encounter_cleaned = pd.concat([df_fish_cleaned, df_count_cleaned], ignore_index=True)\n",
    "df_encounter_cleaned = df_encounter_cleaned.sort_values(['haul_key', 'encounter_key']).reset_index(drop=True)\n",
    "\n",
    "df_encounter_cleaned['record_status'] = 'Valid'\n",
    "\n",
    "df_encounter_cleaned.to_csv(output_data_dir / 'encounter_cleaned.csv', index=False)\n",
    "\n",
    "print(df_encounter_cleaned.head())"
   ]
  },
  {
   "cell_type": "markdown",
   "id": "842c583c-14be-4d58-91dc-f645da787dde",
   "metadata": {},
   "source": [
    "# 🎉 Woohoo! Data Processing Complete! 🎉\n",
    "\n",
    "Congratulations! 🥳  "
   ]
  }
 ],
 "metadata": {
  "kernelspec": {
   "display_name": "Python [conda env:base] *",
   "language": "python",
   "name": "conda-base-py"
  },
  "language_info": {
   "codemirror_mode": {
    "name": "ipython",
    "version": 3
   },
   "file_extension": ".py",
   "mimetype": "text/x-python",
   "name": "python",
   "nbconvert_exporter": "python",
   "pygments_lexer": "ipython3",
   "version": "3.13.9"
  }
 },
 "nbformat": 4,
 "nbformat_minor": 5
}
