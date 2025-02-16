# Pursuit Data Engineer Technical Assessment

## Overview

Design and partially implement a system to handle Pursuit's public sector contact and entity data, focusing on data modeling and efficient querying capabilities. The solution will address both the architectural design and a sample implementation of a data pipeline that ingests, cleans, and loads data into a PostgreSQL database with an optimized search layer.

## Part 1: System Design

### Dataset Context

- **Contacts:** Approximately 4.5 million records containing information such as first name, last name, emails, phone, title, department, and creation date.
- **Entities:** Approximately 150k records (sampled as ~1k in the provided CSV) representing public sector entities. Each entity record includes attributes like `place_id`, geographic information (latitude, longitude, state abbreviation), population estimates, and address details.
- **Relationships:** A many-to-one relationship exists between contacts and entities, linked by the `place_id` field.
- **Customer-Specific Mappings:**  
  - **Customer A:** Associates a fake Salesforce ID (`sfdc_id`) with an entity’s `place_id`.  
  - **Customer B:** Associates a fake Hubspot ID (`hubspot_id`) with an entity’s `place_id`.
- **Entity Technology Stack Data:** Contains details on the technologies used by entities, which can be used for filtering (for example, “Accela”).

### Challenge

1. **Design the Data Model**
    - **Schema Diagrams:** Create diagrams that illustrate how contacts, entities, techstacks, and customer mapping data are stored and related.
    - **Indexing Strategy:**  
      - Use primary key indexes for unique identification.
      - Index foreign keys (such as `place_id`) to optimize join operations.
      - Create additional indexes on searchable fields (e.g., `title`, `emails`, `pop_estimate_2022`, and customer-specific IDs) to accelerate queries.
      - Implement a GIN index on the full‑text search column (document) in the materialized view.
    - **Handling Customer-Specific Mappings:**  
      - Store customer-specific external IDs in dedicated mapping tables (CustomerA and CustomerB) linked via `place_id`.
      - This allows queries like “All entities with Account Owner Bob” to be executed efficiently by filtering on these external IDs.

2. **Search Architecture**
    - **Enabling Fast Searches Across:**
        - **Contact Attributes:** Support queries on fields like `title` and `emails`.
        - **Entity Relationships:** Leverage joins on `place_id` to connect contacts with entities.
        - **Customer-Specific IDs:** Allow filtering based on external IDs from customer mapping tables.
        - **Custom Filters:** Enable dynamic filtering (for instance, filtering by technology stack and population thresholds) using a flexible query interface.
    - **Performance at Scale:**  
      - Use PostgreSQL’s full‑text search capabilities by creating a materialized view (`flattened_data`) that aggregates and denormalizes data for fast querying.
      - Employ a GIN index on the full‑text search vector column to speed up complex text searches.
      - Optimize queries with targeted indexing on frequently searched fields and by precomputing expensive joins and aggregations in the materialized view.

### Data Model: Schema Diagrams

#### Core Tables

1. **Contacts Table**
   - **Columns:**  
     - `id` (Primary Key; generated if missing)  
     - `place_id` (Foreign Key → Entities)  
     - `first_name`  
     - `last_name`  
     - `emails`  
     - `phone`  
     - `url` (Contact URL)  
     - `title`  
     - `department`  
     - `created_at`
   - **Purpose:** Stores individual contact records.

2. **Entities Table**
   - **Columns:**  
     - `place_id` (Primary Key)  
     - `state_abbr`  
     - `lat` (Latitude)  
     - `long` (Longitude)  
     - `pop_estimate_2022`  
     - `place_fips`  
     - `sum_lev`  
     - `url`  
     - `lsadc`  
     - `display_name`  
     - `parent_id`  
     - `address`
   - **Purpose:** Contains public sector entity details.

3. **Techstacks Table**
   - **Columns:**  
     - (Auto-generated Primary Key)  
     - `place_id` (Foreign Key → Entities)  
     - `name` (Technology Name)  
     - `type` (Technology Type, e.g., CMS, payment, permitting)
   - **Purpose:** Records technology stack details for each entity (one entity may have multiple techstack entries).

4. **Customer Mapping Tables**
   - **CustomerA_Mapping Table**
     - **Columns:**  
       - `sfdc_id` (Fake Salesforce ID)  
       - `place_id` (Foreign Key → Entities)
   - **CustomerB_Mapping Table**
     - **Columns:**  
       - `hubspot_id` (Fake Hubspot ID)  
       - `place_id` (Foreign Key → Entities)
   - **Purpose:** Associates external customer-specific IDs with entities.

#### Flattened Materialized View
A key part of the design is the materialized view (flattened_data), which pre-joins contacts, entities, techstacks, and customer mappings. This denormalized view simplifies querying by aggregating and lower-casing critical fields and generating a full‑text search vector.

The `flattened_data` materialized view consolidates data from the above tables:
- **Columns:**
  - `contact_id` (from Contacts)
  - `emails` (lower-case version)
  - `title` (lower-case version)
  - `display_name` (from Entities, lower-case)
  - `pop_estimate_2022`, `lat`, `long` (from Entities)
  - `tech_names` (aggregated lower-case technology names from Techstacks)
  - `sfdc_id` (from CustomerA_Mapping)
  - `hubspot_id` (from CustomerB_Mapping)
  - `document` (a full‑text search vector built from emails, title, and tech_names)
- **Indexing:**  
  A GIN index is created on the `document` column for efficient full‑text search.

#### Textual ER Diagram

```bash
                +---------------------------------------------------+
                |                      Entity                       |
                +---------------------------------------------------+
                | PK: place_id                                      |
                | state_abbr                                        |
                | lat                                               |
                | long                                              |
                | pop_estimate_2022                                 |
                | place_fips                                        |
                | sum_lev                                           |
                | url                                               |
                | lsadc                                             |
                | display_name                                      |
                | parent_id                                         |
                | address                                           |
                +---------------------------------------------------+
                  /           |                   \             \
     1-to-many /             |1-to-many           \1-to-1       \1-to-1
               /              |                     \             \
  +-----------------+   +-----------------+   +-----------------+   +-----------------+
  |    Contact      |   |   Techstack     |   |   CustomerA     |   |   CustomerB     |
  +-----------------+   +-----------------+   +-----------------+   +-----------------+
  | PK: id          |   | (auto PK)       |   | PK: sfdc_id     |   | PK: hubspot_id  |
  | FK: place_id    |   | FK: place_id    |   | FK: place_id    |   +-----------------+
  | first_name      |   | name            |
  | last_name       |   | type            |
  | emails          |   +-----------------+
  | phone           |
  | url             |
  | title           |
  | department      |
  | created_at      |
  +-----------------+
                       |
                       | (Flattened materialized view joins all tables)
                       v
                +-----------------------------------------------------+
                |                flattened_data                     |
                +-----------------------------------------------------+
                | contact_id          (from Contact)                |
                | emails              (lower-case)                  |
                | title               (lower-case)                  |
                | display_name        (from Entity)                 |
                | pop_estimate_2022   (from Entity)                 |
                | lat                 (from Entity)                 |
                | long                (from Entity)                 |
                | tech_names          (aggregated Techstack names)  |
                | sfdc_id             (from CustomerA)              |
                | hubspot_id          (from CustomerB)              |
                | document            (full‑text search vector)     |
                +-----------------------------------------------------+
```
### 3. Indexing Strategy

**Primary and Foreign Keys:**  
Primary keys are automatically indexed. Index foreign key columns (e.g., `place_id` in Contacts and Techstacks) to optimize join operations.

**Additional Indexes on Searchable Fields:**
The document recommends additional indexes on fields such as title, emails, and numeric fields like pop_estimate_2022 to accelerate queries.

**GIN Index on Full‑Text Search Vector:**:
A critical performance enhancement is the creation of a GIN index on the full‑text search column (document) in the materialized view. This allows fast, complex text searches.

* **Customer Mapping**:
Index external ID columns (sfdc_id and hubspot_id) to facilitate rapid lookup for customer-specific queries.

* **Materialized View**:
Create a GIN index on the full‑text search vector (document) for efficient full‑text queries:
```sql
CREATE INDEX flattened_document_idx ON flattened_data USING GIN("document");
```

### 4. Search Architecture

**PostgreSQL Full‑Text Search**

- **Full‑Text Search Vector:**  
  The system utilizes PostgreSQL’s built‑in full‑text search capabilities. By generating a search vector from lower‑case versions of contact emails, titles, and aggregated techstacks, the system supports robust keyword searches.
  The `document` column in the `flattened_data` view is a full‑text search vector built from lower‑case contact emails, titles, and technology names.

- **GIN Index:**  
  A GIN index on the `document` column enables rapid full‑text searches.

- **Field-Specific Queries:**  
  The interactive CLI supports shorthand queries that generate SQL statements targeting specific fields. For example:

  - **Example:**  
    `title: finance` is converted to:
    ```sql
    SELECT * FROM flattened_data WHERE lower(title) ILIKE '%finance%';
    ```
  
  - Similarly, queries for emails, filters, and customer mappings (via `crm:`) generate precise SQL commands.

### Data Pipeline and Synchronization

- **ETL Process:**  
  The pipeline reads sample CSV files (contacts, entities, techstacks, customer mappings), cleans and transforms the data, and then loads it into PostgreSQL tables.

- **Materialized View:**  
  Data from the individual tables is pre-joined and aggregated into the `flattened_data` materialized view. This denormalized view is indexed (with a GIN index on the `document` column) for fast ad‑hoc querying

- **Interactive CLI:**  
  An interactive CLI allows users to execute both standard SQL queries and shorthand queries (such as `title:`, `filter:`, or `crm:` commands) to perform complex searches on the consolidated data.

Example Queries:
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

### Performance Considerations and Tradeoffs

- **Normalization vs. Denormalization:**  
  While a normalized schema ensures data integrity and reduces redundancy, it can lead to expensive joins. The materialized view denormalizes the data—precomputing joins and aggregations—to speed up query responses.

- **Batch Processing and Incremental Updates:**  
  The ETL pipeline processes data in batches, a necessity for scaling to 4.5 million contacts. In a production environment, techniques like Change Data Capture (CDC) can be used to update the materialized view incrementally rather than rebuilding it entirely.

- **System Complexity::**  
  By relying solely on PostgreSQL for both data storage and full‑text search, the architecture remains simpler and easier to manage compared to solutions that integrate external search engines (e.g., Elasticsearch).

### Future Improvements

- **Enhanced Data Validation:**  
  Use schema validation libraries (e.g., Pandera) to enforce data quality.

- **Distributed Processing:**  
  For processing the full-scale dataset, consider leveraging distributed frameworks like Apache Spark.

- **Monitoring and Alerting:**  
  Implement monitoring tools (such as Prometheus and Grafana) to track system performance and the health of the ETL pipeline.

- **Containerization:**  
  Containerize the application (using Docker) for easier deployment and scalability.

- **API Layer:**  
  Develop a RESTful API to expose search and ingestion endpoints, decoupling the front end from the backend systems.

### Conclusion

This design leverages PostgreSQL’s robust ACID compliance, advanced indexing, and full‑text search capabilities to efficiently manage and query Pursuit’s public sector data. By consolidating data into a flattened materialized view with a GIN index, the solution supports fast, field‑specific querying through an interactive CLI. The approach carefully balances normalization for data integrity with denormalization for query performance. Future improvements, such as enhanced data validation, distributed processing, and containerization, will further scale and secure the solution.
