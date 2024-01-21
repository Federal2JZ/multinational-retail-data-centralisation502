# Multinational Retail Data Centralisation

## Description
This project addresses the need of a multinational company to centralise its sales data, currently spread across various sources, into a single, easily accessible, and analysable database. The goal is to create a centralised system that acts as the single source of truth for sales data, enabling the organisation to become more data-driven.

## Installation
1. **Set up a local database:**
    ```bash
    DATABASE_TYPE = 'postgresql'
            DBAPI = 'psycopg2'
            HOST = 'localhost'
            USER = 'postgres'
            PASSWORD = 'admin123'
            DATABASE = 'sales_data'
            PORT = 5432
    ```

2. **Clone the repository:**
    ```bash
    git clone https://github.com/Federal2JZ/multinational-retail-data-centralisation502.git
    cd multinational-retail-data-centralisation502
    ```

3. **Set up Database Credentials:**
    Have the `db_creds.yaml` file which contain the RDS database credentials and place it in the "multinational-retail-data-centralisation502/" folder.

## Usage
1. **Update Configuration:**
    Modify (if needed) the necessary API endpoints and S3 links in the `main.py` file.

2. **Run the Scripts:**
    ```bash
    python main.py
    python table_updates.py
    ```

3. **Data Querying**: Query the data using the sql files in the Data querying folder.

## File Structure
```plaintext
multinational-retail-data-centralisation502/
│
├── Data querying/
    ├── t1_country_store_counts.sql         # How many stores does the business have in which contries?
    ├── t2_locations_with_most_stores.sql   # Which locations currently have the most stores?
    ├── t3_total_sales_per_month.sql        # Which months produced the largest amount of sales?
    ├── t4_sales_locations.sql              # How many sales are coming from online?
    ├── t5_percentage_of_store_type.sql     # What percentage of sales come through each type of store?
    ├── t6_highest_cost_year_month.sql      # Which month in each year produced the highest cost of sales?
    ├── t7_staff_headcount.sql              # What is our staff headcount?
    ├── t8_german_stores.sql                # Which German store type is selling the most?
    ├── t9_sales_speed.sql                  # How quickly is the company making sales?
├── table updates/
    ├── dim_card_details.sql       # Update script for card details dimension table
    ├── dim_date_times.sql         # Update script for date and time dimension table
    ├── dim_products.sql           # Update script for products dimension table
    ├── dim_store_details.sql      # Update script for store details dimension table
    ├── dim_users_table.sql        # Update script for users dimension table
    ├── foreign_keys.sql           # Script to add foreign key constraints
    ├── orders_table.sql           # Update script for the orders table
    ├── primary_keys.sql           # Script to add primary key constraints
├── .gitignore                  # Git configuration file for ignored files
├── README.md                   # Project documentation
├── data_cleaning.py            # Python script for data cleaning operations
├── data_extraction.py          # Python script for data extraction operations
├── database_utils.py           # Python script with utility functions for database operations
├── main.py                     # Main Python script for running the project
└── table_updates.py            # Python script for running the table updates after running main.py
```

## License
Accessed via the AiCore programme.

The author of the work is Darpan Vinodrai.