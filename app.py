from flask import Flask, render_template, request, redirect, url_for, session, flash, send_from_directory
from flask_bcrypt import Bcrypt
from flask_session import Session
from flask_mail import Mail, Message
import sqlite3
import os
import io
import zipfile
import requests
from datetime import datetime

from Code import importing
from Code import profiling
from Code import non_partner
from Code import content_syndication
from Code import partner
from Code import sales_campaign
from Code import campaign_memberstatus
from Code import qa_update

app = Flask(__name__)
app.secret_key = '5ed53f087125401bb74c2c00bf673ece'  # Use a secure secret key

# SQLite Configuration
app.config['DATABASE'] = 'xselldb.sqlite'  # SQLite database file

# Email configuration for Flask-Mail
app.config['MAIL_SERVER'] = 'smtp.gmail.com'
app.config['MAIL_PORT'] = 465
app.config['MAIL_USE_TLS'] = False
app.config['MAIL_USE_SSL'] = True
app.config['MAIL_USERNAME'] = 'kiruthikasermadurai2004@gmail.com'
app.config['MAIL_PASSWORD'] = 'cdvw hfss ppcy ruzg'
app.config['MAIL_DEFAULT_SENDER'] = 'kiruthikasermadurai2004@gmail.com'

mail = Mail(app)

# Session configuration
app.config["SESSION_PERMANENT"] = False
app.config["SESSION_TYPE"] = "filesystem"
Session(app)

bcrypt = Bcrypt(app)

# Ensure upload folder exists
UPLOAD_FOLDER = os.path.join(os.getcwd(), 'uploads')
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER

# Ensure the folder exists
os.makedirs(UPLOAD_FOLDER, exist_ok=True)


# SQLite Database Connection
def get_db_connection():
    conn = sqlite3.connect(app.config['DATABASE'])
    conn.row_factory = sqlite3.Row  # Access results as dictionaries
    return conn


@app.route('/')
def login():
    return render_template('login.html')


@app.route('/login', methods=['POST'])
def login_post():
    if request.method == 'POST':
        email = request.form['email']
        password = request.form['password']

        # Use SQLite connection
        with get_db_connection() as conn:
            cursor = conn.cursor()

            # Select user by email
            cursor.execute("SELECT * FROM users WHERE Email = ?", (email, ))
            user = cursor.fetchone()

        # Verify user and password
        if user and bcrypt.check_password_hash(
                user[3], password):  # Index 3 → password column
            session['admin_logged_in'] = True
            session['admin_name'] = user[1]  # Index 1 → username column
            session['admin_email'] = user[2]  # Index 2 → email column
            return redirect(url_for('dashboard'))
        else:
            flash("Invalid email or password!", "danger")
            return redirect(url_for('login'))


@app.route('/admin', methods=['GET', 'POST'])
def admin():
    print("admin route triggered")

    if request.method == 'POST':
        name = request.form['name']
        email = request.form['email']
        password = request.form['password']

        # Hash the password before storing it
        hashed_password = bcrypt.generate_password_hash(password).decode(
            'utf-8')

        try:
            conn = get_db_connection()
            cursor = conn.cursor()

            # Insert the admin into the users table
            sql_query = """
                INSERT INTO users (uname, email, password) 
                VALUES (?, ?, ?)
            """
            values = (name, email, hashed_password)

            print("Executing SQL:", sql_query)
            print("Values:", values)

            cursor.execute(sql_query, values)
            conn.commit()
            cursor.close()

            flash("Admin added successfully!", "success")
            return redirect(url_for('admin'))

        except Exception as e:
            conn.rollback()
            flash("Database error: " + str(e), "danger")
            return redirect(url_for('admin'))

    return render_template('admin.html')


### Upload Route (SQLite)
@app.route('/upload', methods=['GET', 'POST'])
def upload():
    print("Upload route triggered!")

    if 'admin_logged_in' not in session:
        print("User not logged in, redirecting to login")
        return redirect(url_for('login'))

    if request.method == 'POST':
        print("POST request received!")

        if 'file' not in request.files:
            print("No file part in request!")
            flash("No file part", "danger")
            return redirect(request.url)

        file = request.files['file']

        if file.filename == '':
            flash("No file selected!", "danger")
            return redirect(request.url)

        template = request.form.get('template')

        if not template:
            flash("Please select a template!", "danger")
            return redirect(request.url)

        if not file.filename.endswith('.xlsx'):
            flash("Invalid file format! Only .xlsx files allowed.", "danger")
            return redirect(request.url)

        # Save the file in the uploads folder
        filename = file.filename
        filepath = os.path.join(app.config['UPLOAD_FOLDER'], filename)

        print("Saving file to:", filepath)

        file.save(filepath)
        print("File saved successfully!")

        # Generate the public URL
        public_url = f"{request.host_url}uploads/{filename}"

        try:
            conn = get_db_connection()
            cursor = conn.cursor()

            sql_query = """
                INSERT INTO uploads (fname, fpath, ftype, uploaded_at) 
                VALUES (?, ?, ?, datetime('now'))
            """

            # Store the public URL instead of the internal file path
            values = (filename, public_url, template)

            print("Executing Query:", sql_query)
            print("Values:", values)

            cursor.execute(sql_query, values)
            conn.commit()
            cursor.close()

            flash(f"File uploaded successfully! Accessible at: {public_url}",
                  "success")
            return redirect(url_for('dashboard'))

        except Exception as e:
            conn.rollback()
            print("Database Error:", str(e))
            flash("Database error: " + str(e), "danger")
            return redirect(request.url)

    return render_template('upload.html', admin_name=session.get('admin_name'))


@app.route('/uploads/<filename>', methods=['GET'])
def get_file(filename):
    """Serve uploaded files publicly"""
    return send_from_directory(app.config['UPLOAD_FOLDER'], filename)


@app.route('/picklist', methods=['GET', 'POST'])
def picklist():
    print("Picklist route triggered!")

    if 'admin_logged_in' not in session:
        print("User not logged in, redirecting to login")
        return redirect(url_for('login'))

    if request.method == 'POST':
        print("POST request received!")

        if 'file' not in request.files:
            print("No file part in request!")
            flash("No file part", "danger")
            return redirect(request.url)

        file = request.files['file']

        if file.filename == '':
            flash("No file selected!", "danger")
            return redirect(request.url)

        template = request.form.get('template')

        if not template:
            flash("Please select a template!", "danger")
            return redirect(request.url)

        if not file.filename.endswith('.xlsx'):
            flash("Invalid file format! Only .xlsx files allowed.", "danger")
            return redirect(request.url)

        # Save the file in the global picklist folder
        filename = file.filename
        filepath = os.path.join(app.config['UPLOAD_FOLDER'], filename)

        print("Saving file to:", filepath)

        file.save(filepath)
        print("File saved successfully!")

        # Generate the public URL
        public_url = f"{request.host_url}uploads/{filename}"

        try:
            conn = get_db_connection()
            cursor = conn.cursor()

            # Check if the template already exists
            cursor.execute("SELECT * FROM picklist WHERE ptype = ?",
                           (template, ))
            existing_file = cursor.fetchone()

            if existing_file:
                # Remove the old file from disk
                old_path = existing_file['ppath']
                if os.path.exists(old_path):
                    os.remove(old_path)

                # Remove the old entry from the database
                cursor.execute("DELETE FROM picklist WHERE ptype = ?",
                               (template, ))
                conn.commit()

            # Insert the new file with public URL in the database
            sql_query = """
                INSERT INTO picklist (pname, ppath, ptype, uploaded_at) 
                VALUES (?, ?, ?, datetime('now'))
            """
            values = (filename, public_url, template)

            print("Executing Query:", sql_query)
            print("Values:", values)

            cursor.execute(sql_query, values)
            conn.commit()
            cursor.close()

            flash(
                f"Picklist uploaded successfully! Accessible at: {public_url}",
                "success")
            return redirect(url_for('dashboard'))

        except Exception as e:
            conn.rollback()
            print("Database Error:", str(e))
            flash("Database error: " + str(e), "danger")
            return redirect(request.url)

    return render_template('picklist.html',
                           admin_name=session.get('admin_name'))


@app.route('/dashboard')
def dashboard():
    if 'admin_logged_in' not in session:
        return redirect(url_for('login'))

    conn = get_db_connection()
    cursor = conn.cursor()

    # Fetch all uploaded files
    cursor.execute("SELECT fid, fname FROM uploads ORDER BY uploaded_at DESC")
    files = cursor.fetchall()

    # Fetch all processed file IDs from converted_file (f_tid column)
    cursor.execute("SELECT f_tid FROM converted_file")
    processed_fids = {row[0]
                      for row in cursor.fetchall()
                      }  # Convert to a set for quick lookup

    cursor.close()

    return render_template('dashboard.html',
                           admin_name=session.get('admin_name'),
                           files=files,
                           processed_fids=processed_fids)


@app.route('/view_file/<int:fid>')
def view_file(fid):
    conn = get_db_connection()
    cursor = conn.cursor()

    # Fetch the file path for the given file ID
    cursor.execute("SELECT fpath FROM uploads WHERE fid = ?", (fid, ))
    file_data = cursor.fetchone()

    if not file_data:
        return "File not found", 404

    # Since SQLite returns a tuple, access the first element
    file_path = os.path.join(UPLOAD_FOLDER, os.path.basename(file_data[0]))
    filename = os.path.basename(file_path)

    print("Serving File:", os.path.join(UPLOAD_FOLDER, filename))  # Debugging

    return send_from_directory(UPLOAD_FOLDER, filename)


@app.route('/process/<int:fid>')
def process(fid):
    conn = get_db_connection()
    cursor = conn.cursor()

    # Fetch file path and type from uploads table
    cursor.execute("SELECT fpath, ftype FROM uploads WHERE fid = ?", (fid, ))
    file_data = cursor.fetchone()

    if not file_data:
        return "File not found", 404

    # Extract file path and type
    file_path = file_data[0]
    file_type = file_data[1]
    filename = os.path.basename(file_path)

    # Handle URL or local file path
    if file_path.startswith("http://") or file_path.startswith("https://"):
        print("Downloading file from URL...")
        response = requests.get(file_path)
        if response.status_code != 200:
            return "Failed to download file", 500

        # Store the downloaded file in a temporary in-memory buffer
        excel_file = io.BytesIO(response.content)
    else:
        print("Using local file path...")
        if not os.path.exists(file_path):
            return f"Local file not found: {file_path}", 404

        # Open the local file
        excel_file = file_path

    # Convert Excel to CSV in-memory
    converter = importing.ExcelToCSVConverter(excel_file)
    csv_stream = converter.convert_to_csv()  # In-memory CSV stream

    try:
        print("Profiling...")
        csv_stream.seek(0)
        profiler = profiling.DataProfiler(csv_stream)
        profiling_logs = profiler.generate_profiling_report()

    except Exception as e:
        profiling_logs = f"Error during profiling: {str(e)}"

    # Reset stream position for cleaning
    csv_stream.seek(0)

    # Create the directory if it doesn't exist
    output_dir = os.path.join(app.config['UPLOAD_FOLDER'], 'output')
    os.makedirs(output_dir, exist_ok=True)
    # Picklist processing
    picklist_path = None

    if file_type in [
            "non-partner", "content-syndication", "partner", "sales-campaign"
    ]:
        cursor.execute("SELECT ppath FROM picklist WHERE ptype = ?",
                       (file_type, ))
        picklist_row = cursor.fetchone()

        if not picklist_row:
            flash("Picklist not found for the given type.", "danger")
            return redirect(url_for('dashboard'))

        picklist_path = picklist_row[0]

    # Process file by type with in-memory CSV
    if file_type == "non-partner":
        print("Cleaning...")
        cleaner = non_partner.DataCleaner(csv_stream, picklist_path,
                                          output_dir, fid, conn)
        cleaner.process()

    elif file_type == "content-syndication":
        cleaner = content_syndication.DataCleaner(csv_stream, picklist_path,
                                                  output_dir, fid, conn)
        cleaner.process()

    elif file_type == "partner":
        cleaner = partner.DataCleaner(csv_stream, picklist_path, output_dir,
                                      fid, conn)
        cleaner.process()

    elif file_type == "sales-campaign":
        cleaner = sales_campaign.DataCleaner(csv_stream, picklist_path,
                                             output_dir, fid, conn)
        cleaner.process()

    elif file_type == "campaign-memberstatus":
        cleaner = campaign_memberstatus.DataCleaner(csv_stream, output_dir,
                                                    fid, conn)
        cleaner.process()

    elif file_type == "qa-update":
        cleaner = qa_update.DataCleaner(csv_stream, output_dir, fid, conn)
        cleaner.process()

    # Send email notification to the admin
    admin_email = session.get('admin_email')

    if admin_email:
        subject = "File Cleaning Process Completed"
        body = f"""
        Hello {session.get('admin_name')},

        The file '{filename}' has been successfully cleaned.
        You can now download the cleaned and error files from the dashboard.

        Regards,
        XSELL Team
        """

        msg = Message(subject, recipients=[admin_email])
        msg.body = body
        mail.send(msg)

    return render_template('process.html',
                           admin_name=session.get('admin_name'),
                           logs=profiling_logs,
                           file={'fid': fid})


@app.route('/remove/<int:fid>')
def remove(fid):
    conn = get_db_connection()
    cursor = conn.cursor()

    # Fetch file path from uploads table
    cursor.execute("SELECT fpath FROM uploads WHERE fid = ?", (fid, ))
    file_data = cursor.fetchone()

    if not file_data:
        flash("File not found!", "danger")
        return redirect(url_for('dashboard'))

    file_path = file_data[0]

    try:
        # Remove file from storage
        if os.path.exists(file_path):
            os.remove(file_path)
            flash("File deleted successfully!", "success")
        else:
            flash("File does not exist on the server!", "warning")

        # Delete file entry from the database
        cursor.execute("DELETE FROM uploads WHERE fid = ?", (fid, ))
        cursor.execute("DELETE FROM converted_file WHERE f_tid = ?", (fid, ))
        cursor.execute("DELETE FROM error WHERE fid = ?", (fid, ))
        conn.commit()

    except Exception as e:
        flash(f"Error deleting file: {e}", "danger")

    finally:
        conn.close()

    return redirect(url_for('dashboard'))


@app.route('/processed/<int:fid>')
def processed(fid):
    """Process the uploaded file, convert it to CSV, and generate a profiling report."""
    conn = get_db_connection()
    cursor = conn.cursor()

    # Fetch file path and type from uploads table
    cursor.execute("SELECT fpath, ftype FROM uploads WHERE fid = ?", (fid, ))
    file_data = cursor.fetchone()

    if not file_data:
        return "File not found", 404

    # Extract file path and type
    file_path = file_data[0]
    file_type = file_data[1]
    filename = os.path.basename(file_path)

    # Handle URL or local file path
    if file_path.startswith("http://") or file_path.startswith("https://"):
        print("Downloading file from URL...")
        response = requests.get(file_path)
        if response.status_code != 200:
            return "Failed to download file", 500

        # Store the downloaded file in a temporary in-memory buffer
        excel_file = io.BytesIO(response.content)
    else:
        print("Using local file path...")
        if not os.path.exists(file_path):
            return f"Local file not found: {file_path}", 404

        # Open the local file
        excel_file = file_path

    # Convert Excel to CSV in-memory
    converter = importing.ExcelToCSVConverter(excel_file)
    csv_stream = converter.convert_to_csv()  # In-memory CSV stream

    try:
        print("Profiling...")
        csv_stream.seek(0)
        profiler = profiling.DataProfiler(csv_stream)
        profiling_logs = profiler.generate_profiling_report()

    except Exception as e:
        profiling_logs = f"Error during profiling: {str(e)}"

    # Reset stream position for cleaning
    csv_stream.seek(0)

    conn.close()
    return render_template('processed.html',
                           admin_name=session.get('admin_name'),
                           logs=profiling_logs,
                           file={'fid': fid})


@app.route('/view_con_file/<int:fid>')
def view_con_file(fid):
    """View the converted CSV file."""
    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute("SELECT cpath FROM converted_file WHERE f_tid = ?", (fid, ))
    file_data = cursor.fetchone()

    if not file_data:
        conn.close()
        return "File not found", 404

    # Extract the output folder and filename
    full_path = file_data['cpath']
    output_folder = os.path.dirname(
        full_path
    )  # Get '/home/runner/workspace/implementation/uploads/output'
    filename = os.path.basename(full_path)

    print("Serving File:", os.path.join(output_folder, filename))  # Debugging

    conn.close()
    return send_from_directory(output_folder, filename)


@app.route('/view_error_file/<int:fid>')
def view_error_file(fid):
    """Serve multiple error files as a ZIP archive."""
    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute("SELECT epath FROM error WHERE fid = ?", (fid, ))
    file_data = cursor.fetchall()

    if not file_data:
        conn.close()
        return "No error files found", 404

    # Create a list of full file paths
    file_paths = [row['epath'] for row in file_data]

    # Create ZIP archive
    zip_filename = f"errors_{fid}.zip"
    zip_filepath = os.path.join(UPLOAD_FOLDER, zip_filename)

    with zipfile.ZipFile(zip_filepath, 'w') as zipf:
        for file_path in file_paths:
            if os.path.exists(file_path):  # Ensure file exists
                zipf.write(file_path, os.path.basename(file_path))

    conn.close()
    return send_from_directory(UPLOAD_FOLDER, zip_filename, as_attachment=True)


@app.route('/logout')
def logout():
    session.clear()
    return redirect(url_for('login'))


@app.route('/download/<filename>')
def download_file(filename):
    return send_from_directory(UPLOAD_FOLDER, filename)


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080)
