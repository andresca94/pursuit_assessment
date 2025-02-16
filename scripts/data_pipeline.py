#!/usr/bin/env python
"""
data_pipeline.py

Pursuit Data Engineer Technical Assessment – Data Pipeline + Interactive CLI

This code now:
1) Ensures customer mapping CSVs exist (customerA_mapping.csv, customerB_mapping.csv).
   If empty, it populates them with random SFDC/HubSpot IDs for a few random place_ids.
2) Loads sample CSV data (contacts, entities, techstacks, customer mappings) into PostgreSQL tables.
   – If the contacts CSV does not include an “id” column, one is generated.
3) Creates a materialized view “flattened_data” that:
      - Joins contacts, entities, techstacks, and customer mappings.
      - Aggregates techstacks (tech_names) as a space‑separated lower‑case string.
      - Computes a full‑text search column "document" from lower‑case emails, title, and tech_names.
   A GIN index is created on the document column.
4) Provides an interactive CLI for PostgreSQL queries.
      
USAGE:
  1) Run pipeline only:
         python data_pipeline.py pipeline

  2) Run pipeline, then go interactive:
         python data_pipeline.py pipeline --interactive

  3) Skip pipeline, just do queries:
         python data_pipeline.py --postgres
"""

import os
import sys
import logging
import argparse
import pandas as pd
import random
import string
import re
import numpy as np
import json
from datetime import datetime
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

# ---------------------------------------------------------------------------
# Logging configuration (INFO messages only on console)
# ---------------------------------------------------------------------------
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
log_file = os.path.join(BASE_DIR, '..', 'etl_log.txt')
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filename=log_file,
    filemode='w'
)
console = logging.StreamHandler()
console.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
console.setFormatter(formatter)
logging.getLogger('').addHandler(console)

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
DATA_DIR = os.path.join(BASE_DIR, '..', 'data')
DB_URL = 'postgresql+psycopg2://kreespyn:123456@localhost:5432/pursuit_db'

# ---------------------------------------------------------------------------
# Helper functions: Random ID Generation and ensuring customer mapping files
# ---------------------------------------------------------------------------
def random_sfdc_id():
    return '00' + ''.join(random.choices(string.ascii_uppercase + string.digits, k=10))

def random_hubspot_id():
    return ''.join(random.choices(string.ascii_letters + string.digits, k=8))

def ensure_customer_mapping_files():
    """Ensure customer mapping CSVs exist; if empty, create them with random IDs."""
    place_csv = os.path.join(DATA_DIR, 'places.csv')
    if os.path.exists(place_csv):
        df = pd.read_csv(place_csv)
        df.columns = [col.lower() for col in df.columns]
        all_place_ids = df['place_id'].dropna().unique().tolist()
    else:
        all_place_ids = []
    random_count = min(5, len(all_place_ids))
    mapping_files = {
        'customerA_mapping.csv': ['sfdc_id', 'place_id'],
        'customerB_mapping.csv': ['hubspot_id', 'place_id']
    }
    for fname, headers in mapping_files.items():
        fpath = os.path.join(DATA_DIR, fname)
        try:
            current_df = pd.read_csv(fpath)
            current_df.columns = [col.lower() for col in current_df.columns]
        except Exception:
            current_df = pd.DataFrame()
        if current_df.empty and random_count > 0:
            sample_ids = random.sample(all_place_ids, random_count)
            if 'sfdc_id' in headers:
                new_rows = [{'sfdc_id': random_sfdc_id(), 'place_id': pid} for pid in sample_ids]
            else:
                new_rows = [{'hubspot_id': random_hubspot_id(), 'place_id': pid} for pid in sample_ids]
            new_df = pd.DataFrame(new_rows, columns=headers)
            new_df.to_csv(fpath, index=False)
            logging.info("Created %d random IDs in %s.", len(new_df), fname)
            print(f"Created {len(new_df)} random IDs in {fname}.")

# ---------------------------------------------------------------------------
# Step 1: Load CSV Data
# ---------------------------------------------------------------------------
def load_csv_data():
    files = {
        'contacts': os.path.join(DATA_DIR, 'contacts.csv'),
        'entities': os.path.join(DATA_DIR, 'places.csv'),
        'techstacks': os.path.join(DATA_DIR, 'techstacks.csv'),
        'customerA': os.path.join(DATA_DIR, 'customerA_mapping.csv'),
        'customerB': os.path.join(DATA_DIR, 'customerB_mapping.csv')
    }
    data = {}
    for key, path in files.items():
        try:
            df = pd.read_csv(path)
            df.columns = [col.lower() for col in df.columns]
            logging.info("Loaded '%s' with shape %s", key, df.shape)
            data[key] = df
        except Exception as e:
            logging.error("Error loading %s: %s", key, e)
            data[key] = pd.DataFrame()
    return data

# ---------------------------------------------------------------------------
# Step 2: Clean Data
# ---------------------------------------------------------------------------
def clean_data(data):
    for key in data:
        if not data[key].empty:
            data[key].columns = [col.lower() for col in data[key].columns]
    # Clean contacts
    contacts = data['contacts'].copy()
    if not contacts.empty:
        contacts['created_at'] = (contacts['created_at'].astype(str)
                                  .str.replace(r'\(.*?\)', '', regex=True)
                                  .str.strip())
        contacts['created_at'] = pd.to_datetime(contacts['created_at'], errors='coerce')
        for col in contacts.select_dtypes(include='object').columns:
            if col != 'created_at':
                contacts[col] = contacts[col].str.strip()
        if 'id' not in [col.lower() for col in contacts.columns]:
            contacts.insert(0, 'id', range(1, len(contacts)+1))
            logging.info("Generated surrogate 'id' for contacts.")
            print("Generated surrogate 'id' for contacts.")
        contacts.drop_duplicates(inplace=True)
        logging.info("Contacts => %d rows", contacts.shape[0])
    else:
        logging.warning("No contacts data found.")
    # Clean entities
    entities = data['entities'].copy()
    if not entities.empty:
        for col in entities.select_dtypes(include='object').columns:
            entities[col] = entities[col].str.strip()
        entities.drop_duplicates(inplace=True)
        logging.info("Entities => %d rows", entities.shape[0])
    else:
        logging.warning("No entities data found.")
    # Clean techstacks
    techstacks = data['techstacks'].copy()
    if not techstacks.empty:
        for col in techstacks.select_dtypes(include='object').columns:
            techstacks[col] = techstacks[col].str.strip()
        techstacks.drop_duplicates(inplace=True)
        logging.info("Techstacks => %d rows", techstacks.shape[0])
    else:
        logging.warning("No techstacks data found.")
    # Clean customer mappings
    for key in ['customerA', 'customerB']:
        df = data.get(key, pd.DataFrame())
        if not df.empty:
            for col in df.select_dtypes(include='object').columns:
                df[col] = df[col].str.strip()
            df.drop_duplicates(inplace=True)
            data[key] = df
    return {
        'contacts': contacts,
        'entities': entities,
        'techstacks': techstacks,
        'customerA': data.get('customerA', pd.DataFrame()),
        'customerB': data.get('customerB', pd.DataFrame())
    }

# ---------------------------------------------------------------------------
# Step 3: Load Data into PostgreSQL
# ---------------------------------------------------------------------------
def drop_table(engine, table_name):
    try:
        with engine.begin() as conn:
            conn.execute(text(f"DROP TABLE IF EXISTS {table_name} CASCADE;"))
            logging.info("Dropped table '%s'.", table_name)
    except Exception as e:
        logging.error("Error dropping table %s: %s", table_name, e)

def load_data_to_db(engine, cleaned_data):
    for table in ['entities', 'contacts', 'techstacks', 'customerA', 'customerB']:
        drop_table(engine, table)
    try:
        cleaned_data['entities'].to_sql('entities', engine, if_exists='replace', index=False)
        cleaned_data['contacts'].to_sql('contacts', engine, if_exists='replace', index=False)
        cleaned_data['techstacks'].to_sql('techstacks', engine, if_exists='replace', index=False)
        cleaned_data['customerA'].to_sql('customera', engine, if_exists='replace', index=False)
        cleaned_data['customerB'].to_sql('customerb', engine, if_exists='replace', index=False)
        logging.info("Data loaded into PostgreSQL successfully.")
        print("Data loaded into PostgreSQL successfully.")
    except Exception as e:
        logging.error("Error loading data to DB: %s", e)
        print("Error loading data to DB:", e)

def verify_relationships(engine):
    with engine.connect() as conn:
        try:
            counts = {}
            for table in ['entities', 'contacts', 'techstacks', 'customera', 'customerb']:
                result = conn.execute(text(f"SELECT count(*) FROM {table}"))
                counts[table] = result.scalar()
            logging.info("Table counts: %s", counts)
            print("Table counts:", counts)
        except Exception as e:
            logging.error("Error verifying relationships: %s", e)
            print("Error verifying relationships:", e)

def drop_materialized_view(engine):
    try:
        with engine.begin() as conn:
            conn.execute(text("DROP MATERIALIZED VIEW IF EXISTS flattened_data CASCADE;"))
            logging.info("Dropped materialized view 'flattened_data'.")
    except Exception as e:
        logging.error("Error dropping materialized view: %s", e)

def create_materialized_view(engine):
    mv_sql = """
    DROP MATERIALIZED VIEW IF EXISTS flattened_data CASCADE;
    CREATE MATERIALIZED VIEW flattened_data AS
      SELECT
        c.id AS "contact_id",
        lower(c.emails) AS "emails",
        lower(c.title) AS "title",
        lower(e.display_name) AS "display_name",
        e.pop_estimate_2022,
        e.lat,
        e.long,
        coalesce(string_agg(lower(t.name), ' '), '') AS "tech_names",
        ca.sfdc_id,
        cb.hubspot_id,
        to_tsvector('english', 
           coalesce(lower(c.emails), '') || ' ' ||
           coalesce(lower(c.title), '') || ' ' ||
           coalesce(string_agg(lower(t.name), ' '), '')
        ) AS "document"
      FROM contacts c
      JOIN entities e ON c.place_id = e.place_id
      LEFT JOIN techstacks t ON e.place_id = t.place_id
      LEFT JOIN customera ca ON e.place_id = ca.place_id
      LEFT JOIN customerb cb ON e.place_id = cb.place_id
      GROUP BY c.id, c.emails, c.title, e.display_name, e.pop_estimate_2022, e.lat, e.long, ca.sfdc_id, cb.hubspot_id;
    """
    index_sql = "CREATE INDEX flattened_document_idx ON flattened_data USING GIN(\"document\");"
    try:
        with engine.begin() as conn:
            conn.execute(text(mv_sql))
            logging.info("Materialized view 'flattened_data' created.")
            conn.execute(text(index_sql))
            logging.info("GIN index on 'document' created.")
    except Exception as e:
        logging.error("Error creating materialized view: %s", e)

# ---------------------------------------------------------------------------
# Step 4: Flatten Data (for PostgreSQL search)
# ---------------------------------------------------------------------------
def flatten_data(cleaned_data):
    logging.info("Flattening data for query purposes.")
    contacts = cleaned_data['contacts']
    entities = cleaned_data['entities']
    techstacks = cleaned_data['techstacks']
    customera = cleaned_data['customerA']
    customerb = cleaned_data['customerB']

    if contacts.empty or entities.empty:
        logging.error("Missing contacts or entities; cannot flatten.")
        return pd.DataFrame()

    merged = pd.merge(contacts, entities, on='place_id', how='left', suffixes=('_contact', '_entity'))
    if not techstacks.empty:
        grouped = techstacks.groupby('place_id').agg({'name': lambda x: list(x)}).reset_index().rename(columns={'name': 'tech_names'})
        merged = pd.merge(merged, grouped, on='place_id', how='left')
    else:
        merged['tech_names'] = None

    if not customera.empty:
        merged = pd.merge(merged, customera, on='place_id', how='left')
    if not customerb.empty:
        merged = pd.merge(merged, customerb, on='place_id', how='left')
    logging.info("Flattened data has %s rows.", merged.shape[0])
    return merged

# ---------------------------------------------------------------------------
# Pipeline: Run all Steps
# ---------------------------------------------------------------------------
def run_pipeline():
    logging.info("=== Running Pipeline ===")
    print("Running Pipeline...")
    ensure_customer_mapping_files()
    raw_data = load_csv_data()
    cleaned = clean_data(raw_data)
    engine = create_engine(DB_URL)
    drop_materialized_view(engine)
    load_data_to_db(engine, cleaned)
    verify_relationships(engine)
    create_materialized_view(engine)
    logging.info("PostgreSQL steps complete.")
    # Flattening is used for query purposes here
    flat_df = flatten_data(cleaned)
    logging.info("Pipeline Done.")
    print("Pipeline Done.")
    engine.dispose()

# ---------------------------------------------------------------------------
# Interactive CLI for PostgreSQL
# ---------------------------------------------------------------------------
def parse_custom_query(user_query):
    lower_query = user_query.lower()
    if lower_query.startswith("title:"):
        term = user_query.split(":", 1)[1].strip()
        # Modified: search only in the title field (case-insensitive)
        return f"SELECT * FROM flattened_data WHERE lower(title) ILIKE '%{term.lower()}%';"
    elif lower_query.startswith("emails:"):
        term = user_query.split(":", 1)[1].strip().lower()
        return f"SELECT * FROM flattened_data WHERE \"emails\" ILIKE '%{term}%';"
    elif lower_query.startswith("range:"):
        parts = user_query[6:].strip().split()
        if len(parts) == 2:
            field = parts[0]
            m = re.match(r'^(>=|>|<=|<)(\d+(?:\.\d+)?)$', parts[1])
            if m:
                op = m.group(1)
                val = m.group(2)
                return f"SELECT * FROM flattened_data WHERE {field} {op} {val};"
        elif len(parts) == 3:
            field, op, val = parts
            return f"SELECT * FROM flattened_data WHERE {field} {op} {val};"
        else:
            return "SELECT 'Invalid range query' AS error;"
    elif lower_query.startswith("filter:"):
        try:
            parts = user_query[len("filter:"):].strip().split()
            if len(parts) != 3:
                return "SELECT 'Invalid filter query format' AS error;"
            email_term, tech_term, pop_expr = parts
            if pop_expr.startswith(">="):
                op = ">="
                pop_val = pop_expr[2:]
            elif pop_expr.startswith(">"):
                op = ">"
                pop_val = pop_expr[1:]
            elif pop_expr.startswith("<="):
                op = "<="
                pop_val = pop_expr[2:]
            elif pop_expr.startswith("<"):
                op = "<"
                pop_val = pop_expr[1:]
            else:
                return "SELECT 'Invalid population expression' AS error;"
            return (f"SELECT * FROM flattened_data WHERE \"emails\" ILIKE '%{email_term}%' "
                    f"AND \"tech_names\" ILIKE '%{tech_term}%' AND pop_estimate_2022 {op} {pop_val};")
        except Exception as e:
            return f"SELECT 'Error parsing filter query: {e}' AS error;"
    elif lower_query.startswith("crm:"):
        param = user_query.split(":", 1)[1].strip().lower()
        if param == "a":
            return "SELECT * FROM flattened_data WHERE sfdc_id IS NOT NULL;"
        elif param == "b":
            return "SELECT * FROM flattened_data WHERE hubspot_id IS NOT NULL;"
        elif param == "all":
            return "SELECT * FROM flattened_data WHERE sfdc_id IS NOT NULL OR hubspot_id IS NOT NULL;"
        else:
            return "SELECT 'Invalid crm parameter' AS error;"
    elif ":" in user_query:
        field, term = user_query.split(":", 1)
        return f"SELECT * FROM flattened_data WHERE {field.strip()} ILIKE '%{term.strip().lower()}%';"
    else:
        return f"SELECT * FROM flattened_data WHERE \"document\" @@ plainto_tsquery('english', '{user_query}');"

def interactive_postgres():
    engine = create_engine(DB_URL)
    print("PostgreSQL mode. Type SQL or shorthand queries (or 'exit').")
    try:
        while True:
            user_input = input("PG> ").strip()
            if user_input.lower() in ("exit", "quit"):
                print("Exiting PostgreSQL mode.")
                break
            if (user_input.lower().startswith("title:") or
                user_input.lower().startswith("emails:") or
                user_input.lower().startswith("range:") or
                user_input.lower().startswith("filter:") or
                user_input.lower().startswith("crm:") or
                ":" in user_input):
                sql_query = parse_custom_query(user_input)
                print("SQL:", sql_query)
            else:
                sql_query = user_input
            try:
                with engine.connect() as conn:
                    result = conn.execute(text(sql_query))
                    rows = result.fetchall()
                    print(f"Returned {len(rows)} rows.")
                    for row in rows[:10]:
                        print(row)
            except Exception as e:
                print("Error executing query:", e)
    except EOFError:
        print("EOF detected. Exiting.")
    finally:
        engine.dispose()

# ---------------------------------------------------------------------------
# CLI Main
# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(
        description="Pursuit Data Pipeline + Interactive PostgreSQL Queries"
    )
    parser.add_argument("command", nargs="?", default=None,
                        help="Options: pipeline, or omit for interactive queries.")
    parser.add_argument("--interactive", action="store_true",
                        help="After pipeline, go into interactive query mode.")
    parser.add_argument("--postgres", action="store_true",
                        help="Go directly into PostgreSQL interactive mode (skip pipeline).")
    args = parser.parse_args()

    if args.command == "pipeline":
        run_pipeline()
        if args.interactive or args.postgres:
            interactive_postgres()
        return

    if args.postgres:
        interactive_postgres()
    else:
        print("No command specified. Use '--postgres' for interactive mode, or 'pipeline' to run the full pipeline.")
        parser.print_help()

if __name__ == '__main__':
    main()
