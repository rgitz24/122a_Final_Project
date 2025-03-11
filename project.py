import mysql.connector
import sys
import csv
import os

'''
CONNECTING
'''

# Connection information
DB_CONFIG = {
    'user': 'test',
    'password': 'password',
    'database': 'cs122a',
}

def connect():
    try:
        connection = mysql.connector.connect(**DB_CONFIG)
        return connection
    except mysql.connector.Error as error:
        print(f"Error: {error}")
        sys.exit(1)

'''
FUNCTIONS
'''

def import_data(folder_path):
    try:
        connection = connect()          # Connects to local database using configs
        cursor = connection.cursor()    # MySQL object that can fetch and operate on each row

        # Delete old tables
        tables = ["Sessions", "Movies", "Series", "Videos", "Reviews", "Viewers", "Producers", "Producers", "Releases", "Users"]

        for table in tables:
            cursor.execute(f"DROP TABLE IF EXISTS {table}")     # Removes all tables defined under tables

        # Execute statements from load_data_instructions.txt
        with open(os.path.join(folder_path, "load_data_instructions.txt"), 'r') as database:
            statements = database.read().split(';')     # New statements are split via ;
            for statement in statements:
                if statement.strip():
                    cursor.execute(statement)

        # Import data from all the .csv files
        tables_csv = {  # Defines a (table_name : table_file) tuple for easier writing
            "Movies": "movies.csv",
            "Producers": "producers.csv",
            "Releases": "releases.csv",
            "Reviews": "reviews.csv",
            "Series": "series.csv",
            "Sessions": "sessions.csv",
            "Users": "users.csv",
            "Videos": "videos.csv",
            "Viewers": "viewers.csv"
        }
        
        for table, csv_file in tables_csv.items():
            file_path = os.path.join(folder_path, csv_file)

            # Open each .csv file
            if os.path.exists(file_path):
                with open(file_path, 'r') as file:
                    csv_read = csv.reader(file)
                    for row in csv_read:
                        # Input is NULL, treat as None type in Python 
                        edited_row = [None if item == '' else item for item in row]
                        
                        # Get each string separated by commas
                        values = ', '.join(['%s'] * len(edited_row))

                        # Insert
                        insert = f"INSERT INTO {table} VALUES ({values})"
                        cursor.execute(insert, edited_row)

        # Commits all edits
        connection.commit()         
        cursor.close()              
        connection.close()
        return True
    
    except Exception as error:
        print(f"Error in import_data as: {error}")
        connection.rollback()       # Wipes all edits
        cursor.close()              
        connection.close()          
        return False


def insert_viewer():
    pass

def add_genre():
    pass

def delete_viewer():
    pass

def insert_movie():
    pass

def insert_session():
    pass

def update_release():
    pass

def get_releases_reviewed():
    pass

def get_popular_releases():
    pass

def release_title():
    pass

def get_active_viewers():
    pass

def videos_reviewed_count():
    pass


def main():

    # Error with user syntax
    if len(sys.argv) < 2:
        print("Please use the syntax: python3 project.py <function> [param1] [param2] ...")
        return
    
    function_name = sys.argv[1]     # Collects the function to be executed
    params = sys.argv[2:]           # Everything after are the parameters

    # Available functions
    functions = {
        'import': lambda: import_data(params[0])
    }

    # Run functions
    if function_name in functions:
        result = functions[function_name]()
        if isinstance(result, bool):
            print("Success" if result else "Fail")
    else:
        print(f"Unknown function entered: {function_name}")

if __name__ == "__main__":
    main()