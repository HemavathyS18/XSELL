import pandas as pd
import io


class ExcelToCSVConverter:

    def __init__(self, excel_file):
        self.excel_file = excel_file

    def convert_to_csv(self):
        df = pd.read_excel(self.excel_file)

        # Store the CSV in memory
        csv_stream = io.StringIO()
        df.to_csv(csv_stream, index=False)

        # Reset stream position for subsequent operations
        csv_stream.seek(0)
        return csv_stream
