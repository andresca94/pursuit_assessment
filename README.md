# Pursuit Data Engineer Technical Assessment

This repository contains the solution for the Pursuit Data Engineer Technical Assessment. It includes both system design documentation and a sample data pipeline implementation that ingests, cleans, and loads public sector contact and entity data into PostgreSQL while enabling efficient query capabilities.

## Table of Contents

- [Project Overview](#project-overview)
- [Project Structure](#project-structure)
- [Installation Instructions](#installation-instructions)
- [Data Exploration](#data-exploration)
- [Usage](#usage)
- [Future Improvements](#future-improvements)
- [Conclusion](#conclusion)

## Project Overview

This project is designed to:

- **Design the Data Model:**  
  Create schema diagrams and an indexing strategy for storing contacts, entities, and relationships, including customer-specific mappings (e.g., Customer A: SALESFORCE_ID, Customer B: Hubspot_ID).

- **Implement a Data Pipeline:**  
  Build a pipeline that:
  1. Reads sample CSV files (contacts, entities, techstacks, and customer mappings).
  2. Cleans and scrubs the data.
  3. Loads the data into a PostgreSQL database.
  4. Creates a materialized view that flattens the data (joins contacts, entities, techstacks, and customer mappings), aggregates technology names, and builds a full‑text search column for efficient querying.
  
- **Enable Fast Search:**  
  The interactive CLI supports custom shorthand queries (such as filtering by title, email, range, or customer mapping) against the flattened data.

## Project Structure

```plaintext
pursuit_assessment/
├── data/                   
│   ├── contacts.csv         # Sample contacts dataset (~200k records)
│   ├── places.csv           # Entities dataset (~1k records) (also known as entities.csv)
│   ├── techstacks.csv       # Technology stack data
│   ├── customerA_mapping.csv  # Mapping file with fake Salesforce IDs
│   └── customerB_mapping.csv  # Mapping file with fake Hubspot IDs
├── scripts/                
│   └── data_pipeline.py     # Data pipeline and interactive CLI for PostgreSQL queries
├── environment.yml          # Conda environment file (see above)
├── etl_log.txt              # Log file generated by the pipeline
├── PursuitAssessment.ipynb  # Jupyter Notebook for data exploration and prototyping
├── README.md                # This file
└── SystemDesign.md          # System design and technical considerations
```


## Installation Instructions
### Prerequisites
* Anaconda or Miniconda installed on your system.
* Git (optional, for version control).
* PostgreSQL installed and running (for the database).

### Step-by-Step Setup
1. Clone the Repository
Clone the repository to your local machine:
```bash
git clone https://github.com/andresca94/pursuit_assessment.git
cd pursuit_assessment
```
2. add Sample datasets to data folder:
    - contacts.csv (~200k records)
        
        [contacts.csv](https://prod-files-secure.s3.us-west-2.amazonaws.com/858677c5-434e-41d6-b731-d308ed435b22/5bad77aa-b0ad-4059-9129-2ae9b0b3768c/contacts.csv)
        
    - entities.csv (~1k records)
        
        [places.csv](https://prod-files-secure.s3.us-west-2.amazonaws.com/858677c5-434e-41d6-b731-d308ed435b22/8bcfc398-5f97-42d6-b65d-b884065e238a/places.csv)
        
    - technologies
3. Create the Conda Environment
Use the provided environment.yml file to create a new Conda environment:
```bash
conda env create -f environment.yml
```
4. Activate the Environment
Once the environment is created, activate it:
bash
Copy
conda activate pursuit_assessment
5. Configure **PostgreSQL**:

* Installation:
If PostgreSQL is not yet installed, download and install it from PostgreSQL [Downloads](https://www.postgresql.org/download/).
* Create a Database:
Open your terminal and run the following commands (adjust as needed):
```bash
psql -U postgres
```
Once in the PostgreSQL shell, create a new database:
```bash
CREATE DATABASE pursuit_db;
\q
```
* Update Connection String:
In scripts/data_pipeline.py, update the DB_URL variable to match your PostgreSQL credentials. For example:
```bash
DB_URL = 'postgresql+psycopg2://username:password@localhost:5432/pursuit_db'
```

## Data Exploration
The repository includes a Jupyter Notebook for data exploration. To open and explore the data:
1. Start Jupyter Notebook from the project root:
```bash
jupyter notebook
```
2. Open the notebook located at notebooks/data_exploration.ipynb.
3. Run the cells sequentially to inspect the datasets, perform exploratory data analysis, and validate data quality.

## Usage
The main data pipeline script is located at scripts/data_pipeline.py. This script will:
1. Ensures the customer mapping CSVs exist (or creates them with random IDs).
2. Loads the CSV files from the data/ folder.
3. Cleans and scrubs the data.
4. Loads the data into PostgreSQL.
5. Creates a materialized view (flattened_data) that flattens contacts, entities, techstacks, and customer mapping data. This view includes:
* Aggregated technology names (as a space‑separated, lower‑case string).
* A full‑text search column (document) built from emails, title, and tech_names.
* A GIN index on the document column for efficient searching.
6. Provides an interactive CLI for executing SQL queries.
To run the pipeline, simply execute:
```bash
python scripts/data_pipeline.py
```
### Running the Pipeline
* Provides an interactive CLI for executing SQL queries.
To run the pipeline, simply execute:
```bash
python scripts/data_pipeline.py pipeline --interactive
```
### Example Queries
* Search for contacts with titles containing "finance":
```bash
PG> title: finance
```
* Filter contacts with emails containing "bob", technology containing "Accela", and population > 10,000:
```bash
PG> filter: bob Accela >10000
```
* Retrieve entities synced with Customer A’s CRM:
```bash
PG> crm: a
```



