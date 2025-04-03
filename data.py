import sqlite3

# Database name
DATABASE = 'xselldb.sqlite'

# Connect to the SQLite database
def insert_data():
    try:
        conn = sqlite3.connect(DATABASE)
        cursor = conn.cursor()

        # Insert data into users table
        cursor.executemany('''
        INSERT INTO users (uname, email, password) VALUES (?, ?, ?)
        ''', [
            ('Hemavathy S', 'hemavathys1874@gmail.com', '$2b$12$lzdZc8wvB8Tz/0kI2ob8f.PrN/7jBjffKH1nxhls.6Vj4MvXGCw.2')
        ])

        # Commit the changes and close the connection
        conn.commit()
        print("✅ Data inserted successfully!")

    except sqlite3.Error as e:
        print("❌ Error:", e)
    finally:
        conn.close()

# Run the insertion
if __name__ == "__main__":
    insert_data()