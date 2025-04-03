import pandas as pd
import re
import sqlite3
import os

class DataCleaner:
    def __init__(self, input_csv, output_dir,fid,connection):
        self.input_csv = input_csv
        self.output_dir = output_dir
        self.fid=fid
        self.connection=connection
        self.df = pd.read_csv(input_csv)
        self.removed_rows = {}

        self.init_db()

    def init_db(self):
        """Initialize SQLite database and create tables if they don't exist."""
        cursor = self.connection.cursor()

        cursor.execute('''
        CREATE TABLE IF NOT EXISTS converted_file (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            f_tid TEXT,
            cname TEXT,
            cpath TEXT,
            ctype TEXT,
            generated_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        ''')

        cursor.execute('''
        CREATE TABLE IF NOT EXISTS error (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            fid TEXT,
            ename TEXT,
            epath TEXT,
            type TEXT,
            generated_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        ''')
        
        self.connection.commit()
        
    def remove_duplicates(self):
        self.df = self.df.drop_duplicates()

    def filter_import_type(self):
        self.removed_rows["import_type"] = self.df[self.df["Import Type"] != "campaignmember-so"]
        self.df = self.df[self.df["Import Type"] == "campaignmember-so"]
        print("Executed")

    
    def validate_status(self):
        state_validation_list = ["Attended", "Attended - On Demand", "Sales Follow-Up"]

        def is_valid_status(status):
            return status in state_validation_list  # Returns True/False

        # Identify invalid status rows
        invalid_status_rows = ~self.df["Import SFDC Campaign Status"].apply(is_valid_status)

        # Store invalid rows before removing them
        self.removed_rows["invalid_statuses"] = self.df[invalid_status_rows]

        # Keep only valid rows
        self.df = self.df[~invalid_status_rows]
    
    def validate_sfdc_campaign_id(self):
        pattern = r"^701(\d{12}|\d{15})$"
        self.removed_rows["sfdc_campaign"] = self.df[~self.df["Import SFDC Campaign Id"].astype(str).str.match(pattern, na=False)]
        self.df = self.df[self.df["Import SFDC Campaign Id"].astype(str).str.match(pattern, na=False)]

        
    def remove_restricted_emails(self):
        email_pattern = r".*@(splunk\.com|verticurl\.com|bdo\.[a-zA-Z]{0,})$"
        self.removed_rows["restricted_emails"] = self.df[self.df["Email Address"].astype(str).str.match(email_pattern, na=False)]
        self.df = self.df[~self.df["Email Address"].astype(str).str.match(email_pattern, na=False)]


    def remove_blank_rows(self):
        required_cols = ["Import SFDC Campaign Type", "Import SFDC Campaign Id", "Import SFDC Campaign Status", "Import Type", "Email Address"]
        self.removed_rows["missing_names_email"] = self.df[self.df[required_cols].isnull().any(axis=1)]
        self.df = self.df.dropna(subset=required_cols) 

    def save_to_db(self, file_path, table_name, error_type=None):
        cursor = self.connection.cursor()

        try:
            if table_name == "converted_file":
                cursor.execute('''
                    INSERT INTO converted_file (f_tid, cname, cpath, ctype) 
                    VALUES (?, ?, ?, ?)
                ''', (self.fid, f"converted_file_{self.fid}", file_path, "non-partner"))

            elif table_name == "error":
                cursor.execute('''
                    INSERT INTO error (fid, ename, epath, type)
                    VALUES (?, ?, ?, ?)
                ''', (self.fid, f"error_{error_type}_{self.fid}", file_path, "non-partner"))

            self.connection.commit()
        except sqlite3.Error as e:
            print(f"Error saving to database: {e}")
        finally:
            pass
    
    
    

    def process(self):
        self.remove_duplicates()
        self.filter_import_type()
        self.remove_blank_rows()
        self.validate_sfdc_campaign_id()
        self.remove_restricted_emails()
        self.validate_status()        
        cleaned_file_path = f"{self.output_dir}/cleaned_data_{self.fid}.csv"
        self.df.to_csv(cleaned_file_path, index=False)
        self.save_to_db(cleaned_file_path, "converted_file")

        for key, data in self.removed_rows.items():
            error_file_path = f"{self.output_dir}/removed_{key}_{self.fid}.csv"
            data.to_csv(error_file_path, index=False)
            self.save_to_db(error_file_path, "error", error_type=key)

        print("Data cleaning complete!")