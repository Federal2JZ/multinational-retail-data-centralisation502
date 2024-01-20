# Multinational Retail Data Centralization

## Description
This project addresses the need of a multinational company to centralize its sales data, currently spread across various sources, into a single, easily accessible, and analyzable database. The goal is to create a centralized system that acts as the single source of truth for sales data, enabling the organization to become more data-driven.

## Installation
1. **Clone the repository:**
    ```bash
    git clone https://github.com/Federal2JZ/multinational-retail-data-centralisation502.git
    cd multinational-retail-data-centralisation502
    ```

2. **Set up Database Credentials:**
    Have the `db_creds.yaml` file which contain the database credentials.

## Usage
1. **Update Configuration:**
    Modify (if needed) the necessary API endpoints and S3 links in the `main.py` file.

2. **Run the Script:**
    ```bash
    python main.py
    ```

## File Structure
```plaintext
multinational-retail-data-centralisation502/
│
├── table updates/
    ├── dim_card_details.sql       # Update script for card details dimension table
    ├── dim_date_times.sql         # Update script for date and time dimension table
    ├── dim_products.sql           # Update script for products dimension table
    ├── dim_store_details.sql      # Update script for store details dimension table
    ├── dim_users_table.sql        # Update script for users dimension table
    ├── foreign_keys.sql           # Script to add foreign key constraints
    ├── orders_table.sql           # Update script for the orders table
    ├── primary_keys.sql           # Script to add primary key constraints
├── .gitignore                      # Git configuration file for ignored files
├── data_cleaning.py                # Python script for data cleaning operations
├── data_extraction.py              # Python script for data extraction operations
├── database_utils.py               # Python script with utility functions for database operations
├── main.py                         # Main Python script for running the project
└── README.md                       # Project documentation
```
## License
Accessed via the AiCore programme.

The author of the work is Darpan Vinodrai.