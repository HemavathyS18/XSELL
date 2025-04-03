import pandas as pd
import re
import sqlite3
import os

class DataCleaner:
    def __init__(self, input_csv, picklist_xlsx, output_dir,fid,connection):
        self.input_csv = input_csv
        self.picklist_xlsx = picklist_xlsx
        self.output_dir = output_dir
        self.fid=fid
        self.connection = connection
        self.df = pd.read_csv(input_csv)
        self.removed_rows = {}

        self.state_validation_dict = {
            "United States": ["AL", "AK", "AZ", "AR","AS", "CA", "CO", "CT", "DC", "DE", "FL", "GA", "GU", "HI", "ID", "IL", "IN", "IA", "KS", "KY",
                "LA", "ME", "MD", "MA", "MI", "MN", "MS", "MO", "MP", "MT", "NE", "NV", "NH", "NJ", "NM", "NY", "NC", "ND", 
                "OH", "OK", "OR", "PA", "RI", "SC", "SD", "TN", "TX", "UT", "VT", "VI", "VA", "WA", "WV", "WI", "WY"],
            "Canada": ["AB", "BC", "MB", "NB", "NC", "NS", "NT", "NJ", "ON", "PE", "QC", "SK", "YT"],
            "Australia": ["ACT", "NSW", "NT", "QLD", "SA", "TAS", "VIC", "WA"]
        }

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
        self.removed_rows["import_type"] = self.df[self.df["Import Type"] != "partner"]
        self.df = self.df[self.df["Import Type"] == "partner"]
        print("Executed")

    def validate_picklist_values(self):
        picklist_df = pd.read_excel(self.picklist_xlsx)
        valid_countries = picklist_df["Country"].dropna().unique()
        valid_areas_of_interest = picklist_df["Area Of Interest"].dropna().unique()
        valid_sfdc_status = picklist_df["Import SFDC Campaign Status"].dropna().unique()
        
        invalid_rows = (
            ~self.df["Country"].isin(valid_countries) |
            (~self.df["Area Of Interest"].isna() & ~self.df["Area Of Interest"].isin(valid_areas_of_interest)) |
            ~self.df["Import SFDC Campaign Status"].isin(valid_sfdc_status)
        )
        
        self.removed_rows["invalid_picklist_values"] = self.df[invalid_rows]
        self.df = self.df[~invalid_rows]

    def validate_state(self):
        def is_valid_state(country, state):
            return state in self.state_validation_dict.get(country, ["Non-US/Canada"])

        invalid_state_rows = ~self.df.apply(lambda row: is_valid_state(row["Country"], row["State or Province"]), axis=1)
        self.removed_rows["invalid_states"] = self.df[invalid_state_rows]
        self.df = self.df[~invalid_state_rows]

    def validate_opt_in_flag(self):
        self.removed_rows["opt_in_flag_zero"] = self.df[self.df["Opt-In Flag"] == 0]
        self.df = self.df[self.df["Opt-In Flag"] != 0]

    def validate_sfdc_campaign_id(self):
        pattern = r"^701(\d{12}|\d{15})$"
        self.removed_rows["sfdc_campaign"] = self.df[~self.df["Import SFDC Campaign Id"].astype(str).str.match(pattern, na=False)]
        self.df = self.df[self.df["Import SFDC Campaign Id"].astype(str).str.match(pattern, na=False)]

    def remove_blank_rows(self):
        required_cols = ["Import SFDC Campaign Type", "Import SFDC Campaign Id", "Import Type", "Partner Profile ID","Import UTM Source","First Name", "Last Name", "Email Address", "Job Title", "Country", "State or Province", "Zip or Postal Code", "Phone"]
        self.removed_rows["missing_names_email"] = self.df[self.df[required_cols].isnull().any(axis=1)]
        self.df = self.df.dropna(subset=required_cols)

    def fill_missing_values(self):
        self.df["Opt-In Flag"].fillna("undecided", inplace=True)
        self.df["Company"].fillna("unknown", inplace=True)

    def validate_zip_codes(self):
        invalid_zip = (self.df["Country"] == "United States") & (~self.df["Zip or Postal Code"].astype(str).str.match(r"^\d{5}$", na=False))
        self.removed_rows["invalid_zip"] = self.df[invalid_zip]
        self.df = self.df[~invalid_zip]

    def fill_missing_subject(self):
        self.df.loc[self.df["Comments"].notna() & self.df["Subject"].isna(), "Subject"] = "Notes"

    def remove_restricted_emails(self):
        email_pattern = r".*@(splunk\.com|verticurl\.com|bdo\.[a-zA-Z]{0,})$"
        self.removed_rows["restricted_emails"] = self.df[self.df["Email Address"].astype(str).str.match(email_pattern, na=False)]
        self.df = self.df[~self.df["Email Address"].astype(str).str.match(email_pattern, na=False)]

    def remove_restricted_companies(self):
        restricted_companies = [
            "Default Customer Account - Do Not Edit or Request Ownership",
            "Individual Account - online purchases"
        ]
        self.removed_rows["restricted_companies"] = self.df[self.df["Company"].isin(restricted_companies)]
        self.df = self.df[~self.df["Company"].isin(restricted_companies)]

    def clean_text_columns(self):
        junk_pattern = r"[,/\"']"
        columns_to_clean = ["First Name", "Last Name", "Company", "Phone"]
        for col in columns_to_clean:
            self.df[col] = self.df[col].astype(str).str.replace(junk_pattern, "", regex=True)


    def remove_exclude_routing(self):
        """Removes rows where 'Exclude Routing' is 'TRUE' and 'Exclude Routing Reason' is 'Permanent'."""
        self.df["Exclude Routing"] = self.df["Exclude Routing"].replace({1: "TRUE", 0: "FALSE"})
        self.df["Exclude Routing"] = self.df["Exclude Routing"].fillna("FALSE")

        self.removed_rows["exclude_routing_permanent"] = self.df[
            (self.df["Exclude Routing"] == "TRUE") & (self.df["Exclude Routing Reason"].str.lower() == "permanent")
        ]

        self.df = self.df[~((self.df["Exclude Routing"] == "TRUE") & (self.df["Exclude Routing Reason"].str.lower() == "permanent"))]
    
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
        self.validate_picklist_values()
        self.validate_state()
        self.validate_opt_in_flag()
        self.validate_sfdc_campaign_id()
        self.remove_blank_rows()
        self.fill_missing_values()
        self.validate_zip_codes()
        self.fill_missing_subject()
        self.remove_restricted_emails()
        self.remove_restricted_companies()
        self.remove_exclude_routing()
        self.clean_text_columns()
        
        cleaned_file_path = f"{self.output_dir}/cleaned_data_{self.fid}.csv"
        self.df.to_csv(cleaned_file_path, index=False)
        self.save_to_db(cleaned_file_path, "converted_file")

        for key, data in self.removed_rows.items():
            error_file_path = f"{self.output_dir}/removed_{key}_{self.fid}.csv"
            data.to_csv(error_file_path, index=False)
            self.save_to_db(error_file_path, "error", error_type=key)

        print("Data cleaning complete!")