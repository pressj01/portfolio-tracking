import pyodbc

SERVER = "ALIENR16"
DATABASE = "Portfolio"
CONNECTION_STRING = (
    f"DRIVER={{ODBC Driver 17 for SQL Server}};"
    f"SERVER={SERVER};"
    f"DATABASE={DATABASE};"
    f"Trusted_Connection=yes;"
)

EXCEL_PATH = r"C:\Files\Trading\Dividend Tracking\Dividend_Tracking_082025.xlsm"
SHEET_NAME = "All Accounts"


def get_connection():
    return pyodbc.connect(CONNECTION_STRING)
