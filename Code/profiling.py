import pandas as pd

class DataProfiler:
    def __init__(self, csv_file):
        """
        Initialize the profiler with a CSV file.
        :param csv_file: Path to the CSV file.
        """
        self.csv_file = csv_file
        self.df = pd.read_csv(csv_file)  # Load CSV into a DataFrame

    def basic_statistics(self):
        """
        Get basic descriptive statistics of the dataset.
        """
        return self.df.describe()  # Return DataFrame instead of printing

    def check_missing_values(self):
        """
        Identify missing values in the dataset.
        """
        missing = self.df.isnull().sum()
        return missing[missing > 0]  # Return only columns with missing values

    def check_duplicates(self):
        """
        Identify duplicate records.
        """
        return self.df.duplicated().sum()  # Return number of duplicates

    def generate_profiling_report(self):
        """
        Generate a report with key data profiling insights.
        """
        profiling_report = []
        profiling_report.append("--- Data Profiling Report ---")

        # Basic statistics
        profiling_report.append("\nBasic Statistics:")
        profiling_report.append(self.basic_statistics().to_string())

        # Missing values
        missing_values = self.check_missing_values()
        profiling_report.append("\nMissing Values:")
        profiling_report.append(missing_values.to_string() if not missing_values.empty else "No missing values")

        # Duplicates
        profiling_report.append(f"\nNumber of Duplicate Rows: {self.check_duplicates()}")

        return "\n".join(profiling_report)  # Return full report as a string
