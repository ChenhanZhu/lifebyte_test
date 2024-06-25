import pandas as pd
import psycopg2
import numpy as np

# Database credentials
DB_USER = 'candidate'
DB_PASS = 'NW337AkNQH76veGc'
DB_HOST = 'technical-test-1.cncti7m4kr9f.ap-south-1.rds.amazonaws.com'
DB_PORT = '5432'
DB_NAME = 'technical_test'
# Create a connection to the PostgreSQL database
conn = psycopg2.connect(
    dbname=DB_NAME,
    user=DB_USER,
    password=DB_PASS,
    host=DB_HOST,
    port=DB_PORT
)

# Create a cursor object
cur = conn.cursor()

# Query to fetch all table names in the database
cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")

# Fetch all rows
table_names = cur.fetchall()


# Iterate over table names and perform tests
for table_name in table_names:
    table_name = table_name[0]  # Extract table name from tuple
    print(f"Testing table: {table_name}")

    query = f"SELECT * FROM {table_name}"
    # Load data into a DataFrame
    data = pd.read_sql_query(query, conn)

    # Close the connection
    # conn.close()
    print(data)
    # Display basic information about the dataset
    print("Dataset Information:")
    print(data.info())
    print("\n")

    # Check for missing values
    print("Missing Values:")
    missing_values = data.isnull().sum()
    print(missing_values[missing_values > 0])
    print("\n")

    # Check for duplicate rows
    print("Duplicate Rows:")
    duplicate_rows = data.duplicated().sum()
    print(f"Number of duplicate rows: {duplicate_rows}")
    print("\n")

    if 'open_time' in data.columns and 'close_time' in data.columns:
        print("Checking that open_time <= close_time:")
        open_close_violations = data[pd.to_datetime(data['open_time']) > pd.to_datetime(data['close_time'])]
        if not open_close_violations.empty:
            print("Found open_time > close_time:")
            print(open_close_violations)
        else:
            print("All open_time values are <= close_time values.")
        print("\n")

    # Check for data type mismatches
    print("Data Type Mismatches:")
    for col in data.columns:
        if data[col].dtype == 'object':
            unique_vals = data[col].unique()
            try:
                if all(val.isdigit() for val in unique_vals if pd.notnull(val)):
                    print(f"Column '{col}' has numeric values stored as strings.")
            except:
                pass
    print("\n")

    # Check for inconsistent categorical data
    print("Inconsistent Categorical Data:")
    categorical_cols = data.select_dtypes(include=['object']).columns
    for col in categorical_cols:
        unique_vals = data[col].unique()
        if len(unique_vals) < 10:  # Assuming fewer than 10 unique values indicates a categorical column
            print(f"Column '{col}' unique values: {unique_vals}")
    print("\n")

    # Check for unrealistic values
    # Check for unexpected strings
    print("Unexpected Strings:")
    for col in data.select_dtypes(include='object').columns:
        print(f"Column '{col}':")
        unexpected_strings = data[col].apply(lambda x: isinstance(x, str) and not x.isdigit())
        if unexpected_strings.any():
            print(data.loc[unexpected_strings, col])
        else:
            print("No unexpected strings found.")
        print("\n")

    # Check for unexpected numerical values
    print("Unexpected Numerical Values:")
    for col in data.select_dtypes(include=np.number).columns:
        print(f"Column '{col}':")
        unexpected_numerical = data[col].apply(lambda x: isinstance(x, (int, float)) and (x < 0 or x > 1000000))  # Adjust the range as needed
        if unexpected_numerical.any():
            print(data.loc[unexpected_numerical, col])
        else:
            print("No unexpected numerical values found.")
        print("\n")
    try:
        # Check for unexpected dates
        print("Unexpected Dates:")
        date_columns = ['open_time', 'close_time']  # Assuming these are the date columns
        for col in data.select_dtypes(include="np.datetime64").columns:
            print(f"Column '{col}':")
            unexpected_dates = pd.to_datetime(data[col], errors='coerce').isnull()
            if unexpected_dates.any():
                print(data.loc[unexpected_dates, col])
            else:
                print("No unexpected dates found.")
            print("\n")
    except:
        pass

    # Display summary statistics for further inspection
    print("Summary Statistics:")
    print(data.describe())

    # Check for hash length consistency (assuming hash columns end with '_hash')
    print("Checking hash length consistency:")
    hash_cols = [col for col in data.columns if col.endswith('_hash')]
    for col in hash_cols:
        hash_lengths = data[col].dropna().apply(len).unique()
        if len(hash_lengths) > 1:
            print(f"Column '{col}' has inconsistent hash lengths: {hash_lengths}")
        else:
            print(f"Column '{col}' has consistent hash lengths.")
    print("\n")


fd = open('tech_test_quesy.sql', 'r')
query = fd.read()
fd.close()

# Load data into a DataFrame
data = pd.read_sql_query(query, conn)

# Close cursor and connection
cur.close()
conn.close()

# Display basic information about the dataset
print("Dataset Information:")
print(data)
print("\n")
