# Data Integration and ETL Pipeline

## Table of Contents
1. [Introduction](#1-introduction)
2. [Data Sources](#2-data-sources)
3. [Data Sink](#3-data-sink)
4. [Tech Stack](#4-tech-stack)
5. [Requirement Gathering and Solutions](#5-requirement-gathering-and-solutions)
6. [Data Pipeline Design](#6-data-pipeline-design)
7. [How to Use This Project](#7-how-to-use-this-project)
   - [Preparations](#71-preparations)
   - [Running the Pipeline](#72-running-the-pipeline)
8. [Dataset & Database Schema](#8-dataset--database-schema)
9. [Project Setup](#9-project-setup)

---

## 1. Introduction
This project automates data extraction, transformation, and loading (ETL) from multiple sources, ensuring data integrity. It includes error logging and object dumping to MinIO object storage. It provides an overview of the data pipeline, encompassing data sources, database and object storage sinks, requirement gathering, proposed solutions, and the final data pipeline design.

## 2. Data Sources
- **PostgreSQL Database**: A relational database storing structured data from various business operations.
- **Google Spreadsheets**: A cloud-based spreadsheet used for data collection, collaboration, and manual entry.

## 3. Data Sink
All extracted data will be dumped into:
- **PostgreSQL Staging Database**: A temporary storage layer where extracted data is processed before being loaded into the data warehouse.
- **PostgreSQL Data Warehouse**: The final destination where structured and transformed data is stored for analytics and reporting.
- **MinIO Object Storage**: Used as a failover mechanism to store raw data in case of pipeline failures.

## 4. Tech Stack
- **Python** for scripting and automation
- **PostgreSQL** structured data storage for staging and data warehousing
- **Docker** for containerization and environment consistency
- **Pangres** a library for inserting/upserting data efficiently into PostgreSQL
- **Google Cloud Auth** used to authenticate and retrieve data from Google Spreadsheets
- **MinIO** an S3-compatible object storage

## 5. Requirement Gathering and Solutions
To build an efficient data pipeline and data warehouse, a series of user interviews were conducted. Below are the key findings and the proposed solutions:

### User Interview Questions & Responses
1. **What problem are you facing that led you to build this data pipeline and data warehouse?**
   - _We need a centralized system to store and process data from multiple sources to ensure consistency and enable better decision-making._

2. **How many data sources do you have?**
   - _We currently extract data from a PostgreSQL database and Google Spreadsheets._

3. **What is the purpose of this data pipeline or data warehouse?**
   - _To integrate disparate data sources, clean and transform the data, and make it available for analytics and reporting._

4. **What data do you store in this database?**
   - _Sales transactions, customer details, product information, and other operational data._

5. **During some initial data profiling, we found inconsistencies such as negative values in price and incorrect role data. Can we remove or fix them?**
   - _Yes, negative values should be treated as errors and corrected or removed, while role inconsistencies need business rules to standardize._

6. **Do you need to retain any information about that failure, and if so, how would you like to store it?**
   - _Yes, we need to store information about pipeline failures for debugging, auditing, and ensuring data quality. We'd like to store it in object storage._

7. **Regarding the log, staging, and data warehouse databases, will we need to make any modifications to their existing structures?**
   - _No, please keep the databases as they are._

## 6. Data Pipeline Design
Based on the gathered requirements, the following data pipeline design is proposed:

### **Source Layer**
- Extract data from PostgreSQL and Google Spreadsheets.
- Log extraction process and errors.
- Store failure pipeline raw data in MinIO.

### **Staging Layer**
- Store extracted data in a PostgreSQL staging database.
- Perform initial data validation and cleansing.
- Log transformation steps and errors.

### **Warehouse Layer**
- Load cleansed and transformed data into the PostgreSQL data warehouse.
- Implement indexing and partitioning for performance optimization.
- Ensure data integrity and consistency.

### **Monitoring & Error Handling**
- Log all pipeline events (success, failure, and warnings).
- Store failed records in MinIO for troubleshooting.
- Send alerts for critical failures.

## 7. How to Use This Project
### 7.1 Preparations
1. **Clone the repository:**
   ```bash
   git clone https://github.com/hudiyaresa/ELT-pipeline-paccafe.git
   ```
2. **Set up the environment variables (`.env` file):**
   ```env
   # PostgreSQL Credentials
   SRC_POSTGRES_DB=...
   SRC_POSTGRES_HOST=...
   SRC_POSTGRES_USER=...
   SRC_POSTGRES_PASSWORD=...
   SRC_POSTGRES_PORT=...

   STG_POSTGRES_DB=paccafe_stg
   STG_POSTGRES_HOST=...
   STG_POSTGRES_USER=...
   STG_POSTGRES_PASSWORD=...
   STG_POSTGRES_PORT=...

   WH_POSTGRES_DB=paccafe_dwh
   WH_POSTGRES_HOST=...
   WH_POSTGRES_USER=...
   WH_POSTGRES_PASSWORD=...
   WH_POSTGRES_PORT=...

   LOG_POSTGRES_DB=paccafe_log
   LOG_POSTGRES_HOST=...
   LOG_POSTGRES_USER=...
   LOG_POSTGRES_PASSWORD=...
   LOG_POSTGRES_PORT=...

   # MinIO Credentials
   MINIO_ACCESS_KEY=...
   MINIO_SECRET_KEY=...

   # Google Spreadsheet Credentials
   CRED_PATH='YOUR_PATH/.creds/pipeline-paccafe.json'
   KEY_SPREADSHEET="place_your_spreadsheet_key_here"
   ```

### 7.2 Running the Pipeline
Start all services:
```bash
docker compose up -d
```

Restart services (if needed):
```bash
docker compose down --volumes && docker compose up -d
```

## 8. Dataset & Database Schema
- **[Paccafe Database](https://github.com/Kurikulum-Sekolah-Pacmann/data_pipeline_paccafe)**
- **[Duplicate Spreadsheet](https://docs.google.com/spreadsheets/d/1kNHT10oy2w-I4AGxWHhBHSxo0vrVchBqxAe6e53xfi4/edit?usp=drive_link)**

### Database Schemas:
- **[Source Schema](https://github.com/hudiyaresa/ELT-pipeline-paccafe/blob/main/source_data/init.sql)**
- **[Staging Schema](https://github.com/hudiyaresa/ELT-pipeline-paccafe/blob/main/staging_data/init.sql)**
- **[Log Schema](https://github.com/hudiyaresa/ELT-pipeline-paccafe/blob/main/log_data/init.sql)**

## 9. Project Setup
1. Save your Google Service Account credentials.
2. Configure MinIO credentials (Bucket: `error-paccafe`).
3. Set up PostgreSQL databases (`log` and `staging`).
4. Prepare the `.env` file.