import sqlite3

DATABASE = 'xselldb.sqlite'  # SQLite database file

def get_db():
    """Connect to the SQLite database and return the connection and cursor."""
    conn = sqlite3.connect(DATABASE)
    conn.row_factory = sqlite3.Row  # Access results by column name
    return conn

def init_db():
    """Initialize the database by creating tables if they don't exist."""
    with get_db() as conn:
        cursor = conn.cursor()
        
        # Create tables if they don't exist
        cursor.executescript('''
        CREATE TABLE IF NOT EXISTS users (
            uid INTEGER PRIMARY KEY AUTOINCREMENT,
            uname TEXT NOT NULL,
            email TEXT NOT NULL UNIQUE,
            password TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS uploads (
            fid INTEGER PRIMARY KEY AUTOINCREMENT,
            fname TEXT NOT NULL,
            fpath TEXT NOT NULL,
            ftype TEXT NOT NULL,
            uploaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS picklist (
            pid INTEGER PRIMARY KEY AUTOINCREMENT,
            pname TEXT NOT NULL,
            ptype TEXT NOT NULL,
            ppath TEXT DEFAULT NULL,
            uploaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS error (
            eid INTEGER PRIMARY KEY AUTOINCREMENT,
            ename TEXT NOT NULL,
            epath TEXT DEFAULT NULL,
            type TEXT NOT NULL,
            generated_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            fid INTEGER NOT NULL,
            FOREIGN KEY (fid) REFERENCES uploads (fid) ON DELETE CASCADE
        );

        CREATE TABLE IF NOT EXISTS converted_file (
            cid INTEGER PRIMARY KEY AUTOINCREMENT,
            cname TEXT NOT NULL,
            cpath TEXT DEFAULT NULL,
            ctype TEXT NOT NULL,
            generated_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            f_tid INTEGER NOT NULL,
            FOREIGN KEY (f_tid) REFERENCES uploads (fid) ON DELETE CASCADE
        );
        ''')
        
        conn.commit()
        print("✅ SQLite Database initialized successfully!")

def close_db(conn):
    """Close the database connection."""
    if conn:
        conn.close()
if __name__ == '__main__':
    init_db()  # Initialize th