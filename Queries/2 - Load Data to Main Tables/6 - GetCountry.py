import pandas as pd
import psycopg2
import googlemaps

table_name = 'import.imported_fixtures_data_2019'

# Initialize the API client
gmaps = googlemaps.Client(key='xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx')

# Database connection details
db_config = {
    "dbname": "thesis_v5",
    "user": "postgres",
    "password": "xxxxxxx",
    "host": "localhost",
    "port": "5432"
}


# Retrieve data from the database
def fetch_locations(table_name):
    try:
        connection = psycopg2.connect(**db_config)
        cursor = connection.cursor()
        query = f"SELECT DISTINCT port_load FROM {table_name} WHERE port_load IS NOT NULL;"
        cursor.execute(query)
        rows = cursor.fetchall()
        return pd.DataFrame(rows, columns=['port_load'])
    except Exception as e:
        print(f"Error fetching data: {e}")
    finally:
        if connection:
            cursor.close()
            connection.close()
            
            
# Function to extract country
def get_country(location_name):
    try:
        geocode_result = gmaps.geocode(location_name)
        
        #if not geocode_result:
        #    return f"No results found for location: {location_name}"
        if geocode_result:
            for component in geocode_result[0]['address_components']:
                if 'country' in component['types']:
                    return component['long_name']
            return None
        return "Unknown"
    except Exception as e:
        print(f"Error fetching geocode for {location_name}: {e}")
        return "Unknown"

# Update the database with country information
def update_country(dataframe, table_name):
    try:
        connection = psycopg2.connect(**db_config)
        cursor = connection.cursor()
        
        # Dynamic update query
        for _, row in dataframe.iterrows():
            query = f"UPDATE {table_name} SET country = %s WHERE port_load = %s;"
            cursor.execute(query, (row['country'], row['port_load']))
        
        connection.commit()
        print(f"Database table '{table_name}' updated successfully.")
    except Exception as e:
        print(f"Error updating database: {e}")
    finally:
        if connection:
            cursor.close()
            connection.close()

def add_country_column_if_not_exists(table_name):
    try:
        connection = psycopg2.connect(**db_config)
        cursor = connection.cursor()
        query_check_column = f"""
        SELECT column_name 
        FROM information_schema.columns 
        WHERE table_name = %s AND column_name = 'country';
        """
        cursor.execute(query_check_column, (table_name,))
        column_exists = cursor.fetchone()

        # If the column doesn't exist, add it
        if not column_exists:
            query_add_column = f"ALTER TABLE {table_name} ADD COLUMN country VARCHAR(255);"
            cursor.execute(query_add_column)
            connection.commit()
            print(f"Column 'country' added to table '{table_name}'.")
        else:
            print(f"Column 'country' already exists in table '{table_name}'.")

    except Exception as e:
        print(f"Error modifying table: {e}")
    finally:
        if connection:
            cursor.close()
            connection.close()
        
# Main workflow
def process_table(table_name):
    locations_df = fetch_locations(table_name)
    
    # Add country column if not exist
    add_country_column_if_not_exists(table_name)
    
    # Add a new column for countries
    locations_df['country'] = locations_df['port_load'].apply(get_country)
    
    # Update USG to United States
    locations_df.loc[locations_df['port_load'] == 'USG', 'country'] = 'United States'
    
    # Update the database with country information
    update_country(locations_df, table_name)
    


process_table(table_name)

#get_country('SU TU DEN TERMINAL')
