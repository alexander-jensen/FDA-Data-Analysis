"""This file is meant to help clean up the csvtosql.ipynb notebook. It contains a good portion of the table definition, and it would be hard to soft-code variables for everything. Therefore, all table creations will be held in here. """
import os
import psycopg2
import pandas as pd
import psycopg2.extras as extras 
from config import config # This is a file to get params as a dict for the db login, make your own database.ini!
import numpy as np

def connect():
    """Connects to the database and returns an sql cursor. Run this to establish a connection to the server using the config file. """
    print('Attempting to connect to postgreSQL database...')
    connection = None
    crsr = None
    try:
        # Connect to db
        connection = psycopg2.connect(**config())
        print('Connected')
        # Get cursor and fetch version
        crsr = connection.cursor()
        crsr.execute('SELECT version()')
        db_version = crsr.fetchone() # Fetch first row?
        print('postgreSQL db version: {0}'.format(db_version))
    except(Exception, psycopg2.DatabaseError) as error:
        print(error)
    
    return connection, crsr

def insert_values(conn, query, df, cols): 
    # Data to insert
    tuples = [tuple(x) for x in df[cols].fillna('NULL').to_numpy()] 

    
    # SQL query to execute  
    cursor = conn.cursor() 
    try: 
        extras.execute_values(cursor, query, tuples)
    except (Exception, psycopg2.DatabaseError) as error: 
        print("Error: %s" % error) 
        conn.rollback() 
        cursor.close() 
        return 1
    print("the dataframe is inserted") 
    cursor.close() 

def run_command(conn, cmd, vars=None):
    try:
        with conn.cursor() as curs:
            curs.execute(cmd, vars)
    except(psycopg2.DatabaseError) as error:
        print(type(error), error)
        conn.rollback()

        
def create_insert_branded_foods(conn, clear_table=False):
    # Creates and populates the branded_foods table. This is meant to be ran only once.
    
    reduced_food = pd.read_csv(os.path.join('cleaned', 'branded_food_reduced.csv'))#.drop(columns='Unnamed: 0')

    branded_food_schema = """
        CREATE TABLE branded_foods (
            fdc_id INTEGER PRIMARY KEY NOT NULL,
            gtin_upc VARCHAR(32),
            serving_size DECIMAL,
            serving_size_unit VARCHAR(4),
            package_weight VARCHAR(32), 
            available_date TIMESTAMP NOT NULL,
            insig_iron BOOLEAN,
            insig_calcium BOOLEAN,
            insig_cholesterol BOOLEAN,
            insig_dietary_fiber BOOLEAN,
            insig_trans_fat BOOLEAN, 
            insig_satured_fat BOOLEAN,
            insig_vitamin_d BOOLEAN,
            insig_potassium BOOLEAN, 
            insig_vitamin_a BOOLEAN,
            insig_vitamin_c BOOLEAN,
            insig_added_sugars BOOLEAN,
            insig_total_sugars BOOLEAN,
            insig_calories_from_fat BOOLEAN, 
            insig_sugars BOOLEAN, 
            insig_fiber BOOLEAN,
            category_id INTEGER,
            brand_owner_id INTEGER,
            brand_name_id INTEGER
        )
    """
    # Create table if not already created
    run_command(conn, branded_food_schema)
    
    
    # Clear values if specified
    if clear_table:
        run_command(conn, 'DELETE FROM branded_foods')
    
    bool_cols = ['insig_iron', 
        'insig_calcium',
        'insig_cholesterol',
        'insig_dietary_fiber',
        'insig_trans_fat',
        'insig_satured_fat',
        'insig_vitamin_d', 
        'insig_potassium', 
        'insig_vitamin_a',
        'insig_vitamin_c',
        'insig_added_sugars',
        'insig_total_sugars',
        'insig_calories_from_fat',
        'insig_sugars', 
        'insig_fiber']

    # Replace with TRUEs and FALSEs
    #pd.set_option('future.no_silent_downcasting', True) # Warning silence
    reduced_food[bool_cols] = reduced_food[bool_cols].replace({0:'FALSE', 1:'TRUE'})
    
    cols = ['fdc_id', 
        'gtin_upc', 
        'serving_size', 
        'serving_size_unit',
        'package_weight', 
        'available_date', 
        'insig_iron', 
        'insig_calcium',
        'insig_cholesterol',
        'insig_dietary_fiber',
        'insig_trans_fat',
        'insig_satured_fat',
        'insig_vitamin_d', 
        'insig_potassium', 
        'insig_vitamin_a',
        'insig_vitamin_c',
        'insig_added_sugars',
        'insig_total_sugars',
        'insig_calories_from_fat',
        'insig_sugars', 
        'insig_fiber',
        'category_id',
        'brand_owner_id',
        'brand_name_id']
    
    query = "INSERT INTO %s(%s) VALUES (%s)" % ('branded_foods', ', '.join(cols), ', '.join(['%s'] * len(cols)))
    
    rows = [tuple(x) for x in reduced_food[cols].replace([np.nan], [None]).head().to_numpy()] 
    # Inserts values
    for row in rows:
        run_command(conn, query, row) 
        
    conn.commit()
    
    
def create_brand_names(conn, clear_table=True):
    brand_names = pd.read_csv(os.path.join('cleaned', 'brand_names.csv'))[['brand_name_id', 'brand_name']]
    
    # Create table
    brand_names_schema = """
        CREATE TABLE brand_names (
            brand_id INTEGER PRIMARY KEY NOT NULL,
            brand_name VARCHAR(64) NOT NULL
        )
    """

    run_command(conn, brand_names_schema)
    
    # Clear table if needed
    if clear_table:
        run_command(conn, 'DELETE FROM brand_names')
    
    # Insert values into table
    insert_command = "INSERT INTO brand_names VALUES (%s, %s)"
    
    for row in brand_names.to_numpy():
        run_command(conn, insert_command, row)
        
        
def create_brand_owners(conn, clear_table=True):
    brand_owners = pd.read_csv(os.path.join('cleaned', 'brand_owners.csv'))[['brand_owner_id', 'brand_owner']]
    brand_owners.head()

    # Create table
    brand_owners_schema = """
        CREATE TABLE brand_owners (
            brand_id INTEGER PRIMARY KEY NOT NULL,
            brand_name VARCHAR(128) NOT NULL
        )
    """

    run_command(conn, brand_owners_schema)

    # Clear table if needed
    if clear_table:
        run_command(conn, 'DELETE FROM brand_owners')

    # Insert values into table
    insert_command = "INSERT INTO brand_owners VALUES (%s, %s)"

    for row in brand_owners.to_numpy():
        run_command(conn, insert_command, row)
        
def create_food_categories(conn, clear_table=True):
    # Create food_categories schema
    food_cat = pd.read_csv(os.path.join('cleaned', 'branded_food_categories.csv'))[['category_id', 'category']]

    food_cat_schema = """
        CREATE TABLE food_categories (
            brand_id INTEGER PRIMARY KEY NOT NULL,
            category VARCHAR(64) NOT NULL
        )
    """

    run_command(conn, food_cat_schema)

    if clear_table:
        run_command(conn, "DELETE FROM food_categories")
        
        
    insert_command = "INSERT INTO food_categories VALUES (%s, %s)"
    for row in food_cat.to_numpy():
        run_command(conn, insert_command, row)
        
def create_ingredients(conn, clear_table=True):
    ingredients = pd.read_csv(os.path.join('cleaned', 'ingredients.csv'))[['ingredient_id', 'ingredient']]

    ingred_schema = """
        CREATE TABLE ingredients (
            ingredient_id INTEGER PRIMARY KEY NOT NULL,
            ingredient VARCHAR(2048) NOT NULL
        )
    """
    run_command(conn, ingred_schema)
    
    if clear_table:
        run_command(conn, "DELETE FROM ingredients")

    insert_command = "INSERT INTO ingredients VALUES (%s, %s)"
    for row in ingredients.to_numpy():
        run_command(conn, insert_command, row)
        
        
def create_food_to_ing(conn, clear_table=True):
    food_to_ingred = pd.read_csv(os.path.join('cleaned', 'food_to_id.csv'))[['fdc_id', 'ingredient_id']].astype(int)
    
    fti_schema = """
        CREATE TABLE food_to_ingredients (
            fdc_id INTEGER,
            ingredient_id INTEGER
        )
    """
    
    run_command(conn, fti_schema)
    
    if clear_table:
        run_command(conn, 'DELETE FROM food_to_ingredients')
        
    for row in food_to_ingred.to_numpy():
        run_command(conn, 'INSERT INTO food_to_ingredients VALUES (%s, %s)', row)
        
        
def create_nutrients(conn, clear_table=True):
    nutrients = pd.read_csv(os.path.join('cleaned', 'nutrient.csv'))[['id', 'name', 'unit_name']]
    nutrients_schema = """
        CREATE TABLE nutrients (
            nutrient_id INTEGER PRIMARY KEY NOT NULL,
            nutrient_name VARCHAR(128) NOT NULL,
            unit_name VARCHAR(8) NOT NULL
        )
    """

    run_command(conn, nutrients_schema)
    
    if clear_table:
        run_command(conn, 'DELETE FROM nutrients')

    for row in nutrients.to_numpy():
        run_command(conn, 'INSERT INTO nutrients VALUES (%s, %s, %s)', row)
        
        
def create_food_nutrients(conn, clear_table=True):
    # ids do not contain a valid id in the nutrients table
    invalid_ids = [20938736, 20938797, 20938853, 20941611, 20941655, 20941700,
                   20941745, 20941795, 22036319, 22036352, 22036385, 22036418]
    food_nutrients = pd.read_csv(os.path.join('cleaned', 'food_nutrient.csv'), 
                                 skiprows=invalid_ids, 
                                 usecols=['fdc_id', 'nutrient_id', 'amount']).dropna()
    
    nutrients_schema = """
        CREATE TABLE food_nutrients (
            fdc_id INTEGER,
            nutrient_id INTEGER REFERENCES nutrients(nutrient_id),
            amount DECIMAL NOT NULL
        )
    """
    
    run_command(conn, nutrients_schema)
    
    if clear_table:
        run_command(conn, 'DELETE FROM food_nutrients')
        
    for row in food_nutrients.to_numpy():
        run_command(conn, 'INSERT INTO food_nutrients VALUES (%s, %s, %s)', row)