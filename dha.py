import sqlite3

# Database name
DATABASE = 'xselldb.sqlite'

def display_data():
    """Display all data from each table."""
    try:
        conn = sqlite3.connect(DATABASE)
        cursor = conn.cursor()

        # List of tables to display
        tables = ['users', 'uploads', 'picklist', 'error', 'converted_file']

        for table in tables:
            print(f"\n🔹 Data from table: {table}")
            
            cursor.execute(f"SELECT * FROM {table}")
            rows = cursor.fetchall()

            if rows:
                # Display column names
                column_names = [description[0] for description in cursor.description]
                print(" | ".join(column_names))
                print("-" * 50)

                # Display each row
                for row in rows:
                    print(" | ".join(str(value) for value in row))
            else:
                print("No data found in this table.")
            
            print("=" * 50)

    except sqlite3.Error as e:
        print("❌ Error:", e)
    finally:
        conn.close()

# Run the display function
if __name__== "__main__":
    display_data()