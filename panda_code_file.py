import sqlite3
import pandas as pd

def extract_and_write_to_csv():
    try:
        # Connect to the SQLite database
        conn = sqlite3.connect('Data Engineer_ETL Assignment.db')

        # SQL query to extract data
        query = """
            SELECT c.customer_id, c.age, i.item_name, SUM(COALESCE(o.quantity, 0)) AS total_quantity
            FROM customers c
            JOIN Sales s ON c.customer_id = s.customer_id
            JOIN Orders o ON s.sales_id = o.sales_id
            JOIN Items i ON o.item_id = i.item_id
            WHERE c.age BETWEEN 18 AND 35
            GROUP BY c.customer_id, c.age, i.item_name
            HAVING SUM(COALESCE(o.quantity, 0)) > 0
        """

        # Execute the query and read into pandas DataFrame
        df = pd.read_sql_query(query, conn)

        # Write DataFrame to CSV file
        df.to_csv('panda.csv', index=False, sep=';')

    except sqlite3.Error as e:
        print("SQLite error:", e)

    except Exception as e:
        print("Error:", e)

    finally:
        # Close connection
        conn.close()

# Call the function to execute the code
extract_and_write_to_csv()