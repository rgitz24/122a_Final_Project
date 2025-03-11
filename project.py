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

def create_tables():
    try:
        connection = connect()
        cursor = connection.cursor()

        # Create tables
        tables = {
            "Users": 
                """
                CREATE TABLE IF NOT EXISTS Users (
                    uid INT,
                    email TEXT NOT NULL,
                    joined_date DATE NOT NULL,
                    nickname TEXT NOT NULL,
                    street TEXT,
                    city TEXT,
                    state TEXT,
                    zip TEXT,
                    genres TEXT,
                    PRIMARY KEY (uid)
                )
                """,
            "Producers":
                """
                CREATE TABLE IF NOT EXISTS Producers (
                    uid INT,
                    bio TEXT,
                    company TEXT,
                    PRIMARY KEY (uid),
                    FOREIGN KEY (uid) REFERENCES Users(uid) ON DELETE CASCADE
                ) 
                """,
            "Viewers":
                """
                CREATE TABLE IF NOT EXISTS Viewers (
                    uid INT,
                    subscription ENUM('free', 'monthly', 'yearly'),
                    first_name TEXT NOT NULL,
                    last_name TEXT NOT NULL,
                    PRIMARY KEY (uid),
                    FOREIGN KEY (uid) REFERENCES Users(uid) ON DELETE CASCADE
                )
                """,
            "Releases":
                """
                CREATE TABLE IF NOT EXISTS Releases (
                    rid INT,
                    producer_uid INT NOT NULL,
                    title TEXT NOT NULL,
                    genre TEXT NOT NULL,
                    release_date DATE NOT NULL,
                    PRIMARY KEY (rid),
                    FOREIGN KEY (producer_uid) REFERENCES Producers(uid) ON DELETE CASCADE
                )
                """,
            "Movies":
                """
                CREATE TABLE IF NOT EXISTS Movies (
                    rid INT,
                    website_url TEXT,
                    PRIMARY KEY (rid),
                    FOREIGN KEY (rid) REFERENCES Releases(rid) ON DELETE CASCADE
                );
                """,
            "Series":
                """
                CREATE TABLE IF NOT EXISTS Series (
                    rid INT,
                    introduction TEXT,
                    PRIMARY KEY (rid),
                    FOREIGN KEY (rid) REFERENCES Releases(rid) ON DELETE CASCADE
                )
                """,
            "Videos":
                """
                CREATE TABLE IF NOT EXISTS Videos (
                    rid INT,
                    ep_num INT NOT NULL,
                    title TEXT NOT NULL,
                    length INT NOT NULL,
                    PRIMARY KEY (rid, ep_num),
                    FOREIGN KEY (rid) REFERENCES Releases(rid) ON DELETE CASCADE
                );
                """,
            "Sessions":
                """
                CREATE TABLE IF NOT EXISTS Sessions (
                    sid INT,
                    uid INT NOT NULL,
                    rid INT NOT NULL,
                    ep_num INT NOT NULL,
                    initiate_at DATETIME NOT NULL,
                    leave_at DATETIME NOT NULL,
                    quality ENUM('480p', '720p', '1080p'),
                    device ENUM('mobile', 'desktop'),
                    PRIMARY KEY (sid),
                    FOREIGN KEY (uid) REFERENCES Viewers(uid) ON DELETE CASCADE,
                    FOREIGN KEY (rid, ep_num) REFERENCES Videos(rid, ep_num) ON DELETE CASCADE
                )
                """,
            "Reviews":
                """
                CREATE TABLE IF NOT EXISTS Reviews (
                    rvid INT,
                    uid INT NOT NULL,
                    rid INT NOT NULL,
                    rating DECIMAL(2, 1) NOT NULL CHECK (rating BETWEEN 0 AND 5),
                    body TEXT,
                    posted_at DATETIME NOT NULL,
                    PRIMARY KEY (rvid),
                    FOREIGN KEY (uid) REFERENCES Viewers(uid) ON DELETE CASCADE,
                    FOREIGN KEY (rid) REFERENCES Releases(rid) ON DELETE CASCADE
                );
                """
        }

        for table, command in tables.items():
            cursor.execute(command)
        
        # Commit all edits
        connection.commit()
        cursor.close()
        connection.close()
        print("Tables created successfully.")

    except Exception as error:
        print(f"Error creating tables: {error}")
        connection.rollback()       # Wipes all edits
        cursor.close()              
        connection.close()   


def import_data(folder_path):
    try:
        connection = connect()          # Connects to local database using configs
        cursor = connection.cursor()    # MySQL object that can fetch and operate on each row

        # Delete old tables
        tables = {"movies", "producers", "releases", "reviews", "series", "sessions", "users", "videos", "viewers"}

        for table in tables:
            cursor.execute(f"DROP TABLE IF EXISTS {table}")     # Removes all tables defined under tables

        # Import data from all the .csv files
        tables_csv = {
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
                with open(file_path, 'r') as f:
                    csv_reader = csv.reader(f)
                    for row in csv_reader:
                        # Replace empty strings with None
                        processed_row = [None if item == '' else item for item in row]
                        
                        # Create the placeholders for the INSERT statement
                        placeholders = ', '.join(['%s'] * len(processed_row))
                        
                        # Execute the INSERT statement
                        insert_query = f"INSERT INTO {table} VALUES ({placeholders})"
                        cursor.execute(insert_query, processed_row)

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

    create_tables()

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