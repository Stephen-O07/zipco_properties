import requests
import json
import pandas as pd
import csv
import psycopg2

url = "https://realty-mole-property-api.p.rapidapi.com/randomProperties"

querystring = {"limit":"100000"}

headers = {
	"x-rapidapi-key": "64114f9223mshf6ee7f915c727eep1a5d9djsnef3a9270cef8",
	"x-rapidapi-host": "realty-mole-property-api.p.rapidapi.com"
}

response = requests.get(url, headers=headers, params=querystring)

# print(response.json())

data = response.json()

# save data to json file
filename = 'PropertyRecords.json'
with open(filename, 'w') as file:
    json.dump(data, file, indent=4)

# Read into a DataFrame
propertyrecords_df = pd.read_json('PropertyRecords.json')


# replace NaN values with appropriate defaults or remove  row/columns as necessary
propertyrecords_df.fillna({
    'assessorID': 'Unknown',
    'legalDescription': 'Not available',
    'squareFootage': 0,
    'subdivision': 'Not available',
    'yearBuilt': 0,
    'bathrooms': 0,
    'lotSize': 0,
    'propertyType': 'Unknown',
    'lastSalePrice': 0,
    'lastSaleDate': 'Not available',
    'features': 'None',
    'taxAssessment': 'Not available',
    'owner': 'Unknown',
    'propertyTaxes': 'Not available',
    'bedrooms': 0,
    'ownerOccupied': 0,
    'zoning': 'Unknown',
    'addressLine2': 'Not available',
    'formattedAddres': 'Not available',
    'county': 'Not available',
}, inplace = True)

# Create location Dimension
location_dim = propertyrecords_df[['formattedAddress', 'city', 'state', 'zipCode', 'county', 'subdivision', 'longitude', 'latitude']].copy().drop_duplicates().reset_index(drop=True)
location_dim['location_id'] =range(1, len(location_dim) + 1)
location_dim = location_dim[['location_id', 'formattedAddress', 'city', 'state', 'zipCode', 'county', 'subdivision', 'longitude', 'latitude']]

# Create sales Dimension
sales_dim = propertyrecords_df[['lastSalePrice', 'lastSaleDate']].copy().drop_duplicates().reset_index(drop=True)
sales_dim['sales_id'] =range(1, len(sales_dim) + 1)
sales_dim = sales_dim[['sales_id', 'lastSalePrice', 'lastSaleDate']]

# Create location Dimension
property_type_dim = propertyrecords_df[['propertyType', 'zoning', 'bedrooms', 'bathrooms', 'squareFootage', 'lotSize', 'ownerOccupied']].copy().drop_duplicates().reset_index(drop=True)
property_type_dim['property_type_id'] =range(1, len(property_type_dim) + 1)
property_type_dim = property_type_dim[['property_type_id', 'propertyType', 'zoning', 'bedrooms', 'bathrooms', 'squareFootage', 'lotSize', 'ownerOccupied']]

# Create location Dimension
legal_description_dim = propertyrecords_df[['legalDescription', 'yearBuilt']].copy().drop_duplicates().reset_index(drop=True)
legal_description_dim['legal_description_id'] =range(1, len(legal_description_dim) + 1)
legal_description_dim = legal_description_dim[['legal_description_id', 'legalDescription', 'yearBuilt']]

# Merge operation to create the propertyrecords_df
propertyrecords_df = propertyrecords_df.merge(location_dim, on=['formattedAddress', 'city', 'state', 'zipCode', 'county', 'subdivision', 'longitude', 'latitude'], how='left') \
    .merge(property_type_dim, on=['propertyType', 'zoning', 'bedrooms', 'bathrooms', 'squareFootage', 'lotSize', 'ownerOccupied'], how='left') \
    .merge(sales_dim, on=['lastSalePrice', 'lastSaleDate'], how='left') \
    .merge(legal_description_dim, on=['legalDescription', 'yearBuilt'], how='left')

# Ensure 'location_id', 'property_type_id', and 'legal_description_id' exist in the merged DataFrame
propertyrecords_df = propertyrecords_df[['location_id', 'sales_id','property_type_id', 'legal_description_id','bathrooms', 'squareFootage', 'lotSize', 'ownerOccupied']]

fact_columns = ['location_id', 'sales_id','property_type_id', 'legal_description_id','bathrooms', 'squareFootage', 'lotSize', 'ownerOccupied']
fact_table = propertyrecords_df[fact_columns]

# Save created tables to a csv file

# saving the created fact and dimension table to csv file
location_dim.to_csv('location_dimension.csv', index = False)
property_type_dim.to_csv('property_type_dimension.csv', index = False)
sales_dim.to_csv('sales_dimension.csv', index = False)
legal_description_dim.to_csv('legalDescription_dimension.csv', index = False)
fact_table.to_csv('property_fact.csv', index = False)

# Loading Layer
# develop a function to connect to pgadmin

def get_db_connection():
    connection = psycopg2.connect(
        host = 'localhost',
        database = 'zipco_agency',
        user = 'postgres',
        password = 'Favour@8282'
    )
    return connection

conn = get_db_connection()


#create schema and tables
def create_tables():
    conn = get_db_connection()
    cursor = conn.cursor()
    create_table_query = '''
    CREATE SCHEMA IF NOT EXISTS zipco;

    DROP TABLE IF EXISTS zipco.location_dim CASCADE;
    DROP TABLE IF EXISTS zipco.property_type_dim CASCADE;
    DROP TABLE IF EXISTS zipco.legal_description_dim CASCADE;
    DROP TABLE IF EXISTS zipco.sales_dim CASCADE;
    DROP TABLE IF EXISTS zipco.fact_table CASCADE;
    
     
    CREATE TABLE IF NOT EXISTS zipco.location_dim (
        location_id SERIAL PRIMARY KEY,
        formattedAddress VARCHAR(255),
        city VARCHAR(100),
        state VARCHAR(100),
        zipcode INTEGER,
        county VARCHAR(100),
        subdivision VARCHAR(100),
        longitude FLOAT,
        latitude FLOAT
    );
    
     CREATE TABLE IF NOT EXISTS zipco.property_type_dim (
        property_type_id SERIAL PRIMARY KEY,
        propertyType VARCHAR(255),
        zoning VARCHAR(50),
        bedrooms FLOAT,
        bathrooms FLOAT,
        squareFootage FLOAT,
        lotSize FLOAT,
        ownerOccupied VARCHAR(50)
        
    );
    
    CREATE TABLE IF NOT EXISTS zipco.sales_dim (
        sales_id SERIAL PRIMARY KEY,
        lastSalePrice FLOAT,  
        lastSaleDate DATE
    );
    
    CREATE TABLE IF NOT EXISTS zipco.legal_description_dim (
        legal_description_id SERIAL PRIMARY KEY,
        legalDescription VARCHAR(255),
        yearBuilt FLOAT  
    );
    
    CREATE TABLE IF NOT EXISTS zipco.fact_table (
        location_id INTEGER,
        sales_id INTEGER,
        property_type_id INTEGER,
        legal_description_id INTEGER,
        bathrooms FLOAT,
        squareFootage FLOAT,
        lotSize FLOAT,
        ownerOccupied FLOAT,
        FOREIGN KEY (location_id) REFERENCES zipco.location_dim(location_id),
        FOREIGN KEY (sales_id) REFERENCES zipco.sales_dim(sales_id),
        FOREIGN KEY (property_type_id) REFERENCES zipco.property_type_dim(property_type_id),
        FOREIGN KEY (legal_description_id) REFERENCES zipco.legal_description_dim(legal_description_id)
        
    );
    '''
    
    cursor.execute(create_table_query)
    conn.commit() 
    cursor.close()
    conn.close()

    
create_tables() 

# create a function to load the csv data into the database

def load_data_from_csv_to_table(csv_path, table_name):
    conn = get_db_connection()
    cursor = conn.cursor()
    with open(csv_path, 'r', encoding = 'utf-8') as file:
        reader = csv.reader(file)
        next(reader) # Skip the header row
        for row in reader:
            placeholders = ', '.join(['%s'] * len(row))
            query = f'INSERT INTO {table_name} VALUES ({placeholders});'
            cursor.execute(query, row)
    conn.commit() 
    cursor.close()
    conn.close()  


# Add data from csv file to location dimension table
location_dim_csv_path = r'C:\Users\Acer\zipco_estate\location_dimension.csv'
load_data_from_csv_to_table(location_dim_csv_path, 'zipco.location_dim')

# Add data from csv file to Property Type dimension table
property_type_dim_csv_path = r'C:\Users\Acer\zipco_estate\property_type_dimension.csv'
load_data_from_csv_to_table(property_type_dim_csv_path, 'zipco.property_type_dim')

# Add data from csv file to legal description dimension table
legal_description_dim_csv_path = r'C:\Users\Acer\zipco_estate\legalDescription_dimension.csv'
load_data_from_csv_to_table(legal_description_dim_csv_path, 'zipco.legal_description_dim')

# Code to ignore the Not Available in the sales dimension table

# create a function to load the csv data into the database

def load_data_from_csv_to_sales_table(csv_path, table_name):
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # define the columns name in sales_dim table
    sale_dim_columns = ['sales_id', 'lastSalePrice', 'lastSaleDate']
    
    with open(csv_path, 'r', encoding = 'utf-8') as file:
        reader = csv.reader(file)
        next(reader) # Skip the header row
        
        for row in reader:
            # Convert empty strings (or 'Not Available' in the date to None(Null in SQL)
            # row = [None if (cell == '' or cell == 'Not available') and col_name == 'lastSaledate' else cell for cell, col_name in zip(row, sale_dim_columns)]
            row = [None if col_name == 'lastSaleDate' and (cell == '' or cell.lower() == 'not available') else cell for cell, col_name in zip(row, sale_dim_columns)]
            placeholders = ', '.join(['%s'] * len(row))
            query = f'INSERT INTO {table_name} VALUES ({placeholders});'
            cursor.execute(query, row)
    conn.commit() 
    cursor.close()
    conn.close()  
    


# sales dimension table

sales_dim_csv_path = r'C:\Users\Acer\zipco_estate\sales_dimension.csv'
load_data_from_csv_to_sales_table(sales_dim_csv_path, 'zipco.sales_dim')

# Add data from csv file to fact table table
fact_table_csv_path = r'C:\Users\Acer\zipco_estate\property_fact.csv'
load_data_from_csv_to_table(fact_table_csv_path, 'zipco.fact_table')

print('All data has been loaded successfully into the respective schema and tables')