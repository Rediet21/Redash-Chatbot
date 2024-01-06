**Project Directory**

This directory contains Python scripts and SQL scripts for setting up and loading data into a database. Follow the steps below to create tables and load data in the correct order.

**Directory Structure**
```bash
project_directory/
|-- python_scripts/
| |-- create_tables.py
| |-- load_data.py
|-- sql_scripts/
| |-- create_table_script.sql
|-- data/
| |-- data_file.csv
|-- README.md
```

**1. Create Tables**

Before loading data, you need to create the necessary tables in the database.

**Command:**

```bash
cd dataloading

```

```bash
python run_sql.py 
```

This Python script (create_tables.py) utilizes the SQL script (create_table_script.sql) to create tables in the specified database.

2. Load Data

After creating tables, you can load data into the database.

```bash
python dataload.py 
```
Replace <database_uri> with your actual database connection URI.

This Python script (load_data.py) uses Pandas to load data from the specified CSV file (data/data_file.csv) into the corresponding tables in the database.

Note: Make sure to execute the commands in the given order to avoid trying to load data before creating tables.