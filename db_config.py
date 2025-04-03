from flask_mysqldb import MySQL

def init_db(app):
    app.config['MYSQL_HOST'] = 'localhost'
    app.config['MYSQL_USER'] = 'root'  # Change if needed
    app.config['MYSQL_PASSWORD'] = 'Hema@mysql2004'  # Set your MySQL password
    app.config['MYSQL_DB'] = 'xselldb'
    app.config['MYSQL_CURSORCLASS'] = 'DictCursor'

    mysql = MySQL(app)
    return mysql
