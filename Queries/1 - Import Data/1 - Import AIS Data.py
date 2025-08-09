# This Python Script will take all the csv files from a folder and load those to a table
# Both the csv path and the PostgreSQL table should be difined on the relevant variables

import os
import pandas as pd
import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_values

# ************************************************************************************
# Define the PostgreSQL connection details, the csv location and the load table
# ************************************************************************************

# Database connection details
db_config = {
    'host': 'localhost',
    'dbname': 'thesis_v5',
    'user': 'postgres',
    'password': 'xxxxx'
}

# CSV folder location. Replace with your directory path
directory_path = 'J:/OneDrive/Documents/Personal Files/Academic/Demokritos/Thesis/Complex Event Recognition/Data/AIS/commercial_2022' 

# PostgreSQL tables where the data will be loaded
table_name = 'import.imported_ais_data_2022'           


#%%
# ************************************************************************************
# Define funtions
# ************************************************************************************

# Function to establish a PostgreSQL connection
def get_connection():
    conn = psycopg2.connect(
        host=db_config['host'],
        dbname=db_config['dbname'],
        user=db_config['user'],
        password=db_config['password']
    )
    return conn

# Function to determine the PostgreSQL column types based on pandas data types
def get_postgresql_type(pandas_type):
    if pd.api.types.is_integer_dtype(pandas_type):
        return 'INTEGER'
    elif pd.api.types.is_float_dtype(pandas_type):
        return 'FLOAT'
    elif pd.api.types.is_bool_dtype(pandas_type):
        return 'BOOLEAN'
    elif pd.api.types.is_datetime64_any_dtype(pandas_type):
        return 'TIMESTAMP'
    else:
        return 'TEXT'

# Function to read all CSV files in a directory, concatenate them into a single DataFrame, and add a 'filename' column
def read_directory_files_to_pandas(directory):
    all_dataframes = []

    # Walk through the directory and its subdirectories
    for root, dirs, files in os.walk(directory):
        for file in files:
            if file.endswith('.csv'):
                file_path = os.path.join(root, file)
                print(f'Reading {file_path}...')

                # Read the CSV file (ignore the header)
                df = pd.read_csv(file_path, header=0)

                if not df.empty:
                    # Add a 'filename' column with the current file name
                    df.insert(0, 'filename', file)
                    
                    all_dataframes.append(df)
                
    # Concatenate all dataframes into one
    combined_df = pd.concat(all_dataframes, ignore_index=True)
    return combined_df


# Function to generate CREATE TABLE SQL based on pandas DataFrame
def generate_create_table_sql(df, table_name):
    # Generate the CREATE TABLE SQL dynamically based on the dataframe's columns
    columns = []
    for col in df.columns:
        col_type = get_postgresql_type(df[col])
        columns.append(f"{col} {col_type}")

    create_table_query = f"CREATE TABLE IF NOT EXISTS {table_name} ({', '.join(columns)});"
    return create_table_query

# Main function to read CSVs, create a DataFrame, and generate SQL statement
def process_directory(directory, table_name):
    # Step 1: Read all files into a pandas DataFrame
    combined_df = read_directory_files_to_pandas(directory)
    
    # Step 2: Generate the CREATE TABLE SQL
    create_table_sql = generate_create_table_sql(combined_df, table_name)
    
    # Step 3: Return the DataFrame and SQL statement
    return combined_df, create_table_sql


# Function to execute the CREATE TABLE query in PostgreSQL
def create_table_in_postgresql(create_table_query):
    conn = get_connection()
    cursor = conn.cursor()
    
    try:
        # Execute the CREATE TABLE query
        cursor.execute(create_table_query)
        conn.commit()
        print("Table created successfully!")
    except Exception as e:
        print(f"Error creating table: {e}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()
        
        
# Function to insert the DataFrame data into the PostgreSQL table
def insert_dataframe_to_postgresql(df, table_name):
    conn = get_connection()
    cursor = conn.cursor()

    # Insert data into the table row by row
    try:
        #for _, row in df.iterrows():
        #    placeholders = ', '.join(['%s'] * len(row))
        #    insert_query = sql.SQL(f"INSERT INTO {table_name} VALUES ({placeholders})")
        #    cursor.execute(insert_query, tuple(row))
        records = df.to_records(index=False)
        #rows = list(records)
        rows = [tuple(row) for row in df.itertuples(index=False, name=None)]
        
        query = f"INSERT INTO {table_name} VALUES %s"
        execute_values(cursor, query, rows)
        
        conn.commit()
        print("Data inserted successfully!")
    except Exception as e:
        print(f"Error inserting data: {e}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()
        
                

 
#%% 
# ************************************************************************************
# Main Program
# ************************************************************************************

df, create_table_sql = process_directory(directory_path, table_name)

# Print the CREATE TABLE SQL for review
print('Generated CREATE TABLE SQL: ')
print(create_table_sql)

# Output DataFrame
print(df.head())  # Preview the combined data


#%%

# Create the table in PostgreSQL
create_table_in_postgresql(create_table_sql)

# Insert the DataFrame into the created table
insert_dataframe_to_postgresql(df, table_name)

