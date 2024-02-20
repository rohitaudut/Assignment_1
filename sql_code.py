import sqlite3
import csv

def extract_and_write_to_csv():
    try:
        # Connect to the SQLite database
        conn = sqlite3.connect('data Engineer_ETL Assignment.db')
        cursor = conn.cursor()

        # SQL query to extract total quantities of each item bought per customer aged 18-35
        query = """
            SELECT c.customer_id, c.age, i.item_name, SUM(COALESCE(o.quantity, 0))
            FROM customers c
            JOIN Sales s ON c.customer_id = s.customer_id
            JOIN Orders o ON s.sales_id = o.sales_id
            JOIN Items i ON o.item_id = i.item_id
            WHERE c.age BETWEEN 18 AND 35
            GROUP BY c.customer_id, c.age, i.item_name
            HAVING SUM(COALESCE(o.quantity, 0)) > 0
        """

        # Execute the query
        cursor.execute(query)

        # Fetch all results
        results = cursor.fetchall()

        # Write results to CSV file
        with open('sql.csv', 'w', newline='') as csvfile:
            csv_writer = csv.writer(csvfile, delimiter=';')
            csv_writer.writerow(['Customer', 'Age', 'Item', 'Quantity'])
            csv_writer.writerows(results)

    except sqlite3.Error as e:
        print("SQLite error:", e)

    except Exception as e:
        print("Error:", e)

    finally:
        # Close cursor and connection
        cursor.close()
        conn.close()

# Call the function to execute the code
extract_and_write_to_csv()