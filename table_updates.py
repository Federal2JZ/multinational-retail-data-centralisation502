import os
import psycopg2


def execute_sql_file(file_path, database_url):
    conn = psycopg2.connect(database_url)
    cursor = conn.cursor()

    try:
        # Read and execute each query separately
        with open(file_path, "r") as sql_file:
            queries = sql_file.read()
            cursor.execute(queries)

        # Commit changes after all queries
        conn.commit()
        print(f"Successfully executed: {file_path}")

    except Exception as e:
        print(f"Error executing {file_path}: {e}")
        conn.rollback()

    finally:
        cursor.close()
        conn.close()

directory_path = "table updates"
database_url = "postgresql://postgres:admin123@localhost:5432/sales_data"

# Specify the order in which you want to execute the SQL files
order_of_execution = ["t1_orders_table.sql", "t2_dim_users_table.sql", "t3_dim_store_details.sql",
                      "t4_t5_dim_products.sql", "t6_dim_date_times.sql", "t7_dim_card_details.sql", "t8_primary_keys.sql", 
                      "t9_foreign_keys.sql"]

# Run SQL files one by one
for filename in order_of_execution:
    file_path = os.path.join(directory_path, filename)
    execute_sql_file(file_path, database_url)
