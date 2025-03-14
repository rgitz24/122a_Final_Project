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
    # Creates all tables listed under 'tables' using the command associated with each table name

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
                    first TEXT NOT NULL,
                    last TEXT NOT NULL,
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

        # Creates all tables above by executing the commands above
        for table, command in tables.items():
            cursor.execute(command)
        
        # Commit all edits
        connection.commit()
        cursor.close()
        connection.close()

    except Exception as error:
        print(f"Error creating tables: {error}")
        connection.rollback()       
        cursor.close()              
        connection.close()   


def import_data(folder_path):
    # Given a path to .csv files, create tables in memory from data in those .csv files
    
    try:
        connection = connect()          # Connects to local database using configs
        cursor = connection.cursor()    # MySQL object that can fetch and operate on each row

        # Delete old tables
        tables = {"movies", "producers", "releases", "reviews", "series", "sessions", "users", "videos", "viewers"}

        for table in tables:
            cursor.execute(f"DROP TABLE IF EXISTS {table}")     # Removes all tables defined under tables

        # Creates tables first 
        create_tables()

        # Import data from all the .csv files
        tables_csv = {
            "Users": "users.csv",
            "Producers": "producers.csv",
            "Viewers": "viewers.csv",
            "Releases": "releases.csv",
            "Movies": "movies.csv",
            "Series": "series.csv",
            "Videos": "videos.csv",               
            "Reviews": "reviews.csv",
            "Sessions": "sessions.csv"
        }
        
        for table, csv_file in tables_csv.items():
            file_path = os.path.join(folder_path, csv_file)

            # Open each .csv file
            if os.path.exists(file_path):
                with open(file_path, 'r') as f:
                    csv_reader = csv.reader(f)
                    headers = next(csv_reader, None) # Skip headers

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


def insert_viewer(uid, email, nickname, street, city, state, zip, genres, joined_date, first, last, subscription):
    # Insert a new viewer
    # NOTE: create a User first then Viewer isA User

    try:
        connection = connect()
        cursor = connection.cursor()

        # Check for duplicates
        check = "SELECT uid FROM Users WHERE uid = %s"
        cursor.execute(check, (uid,))
        existing_user = cursor.fetchone()

        if existing_user:
            cursor.close()
            connection.close()
            return False
        

        # Insert user
        insert_user_command = """
            INSERT INTO Users (uid, email, nickname, street, city, state, zip, genres, joined_date)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

        user_params = (uid, email, nickname, street, city, state, zip, genres, joined_date)
        cursor.execute(insert_user_command, user_params)

        # Insert viewer
        insert_viewer_command = """
            INSERT INTO Viewers (uid, subscription, first, last)
            VALUES (%s, %s, %s, %s)
        """

        viewer_params = (uid, subscription, first, last)
        cursor.execute(insert_viewer_command, viewer_params)

        # Commit all edits
        connection.commit()         
        cursor.close()              
        connection.close()
        return True

    except Exception as error:
        print(f"Error in insert_viewer as: {error}")
        connection.rollback()       # Wipes all edits
        cursor.close()              
        connection.close()          
        return False
        

def add_genre():
    pass

def delete_viewer():
    pass

def insert_movie(rid, website_url):
    # Assumption: Corresponding release record already exists

    try:
        connection = connect()
        cursor = connection.cursor()

        check = """
                SELECT rid 
                FROM Movies 
                WHERE rid = %s
                """
        cursor.execute(check, (rid,))
        existing_movie = cursor.fetchone()

        if existing_movie:
            cursor.close()
            connection.close()
            return False

        insert_movie_q = """
                         INSERT INTO Movies(rid, website_url)
                         VALUES (%s, %s)
                         """

        cursor.execute(insert_movie_q,(rid,website_url))

        connection.commit()
        cursor.close()
        connection.close()
        return True

    except Exception as error:
        print(f"Error in insert_movie as: {error}")
        connection.rollback()
        cursor.close()
        connection.close()
        return False

def insert_session():
    pass

def update_release():
    pass

def get_releases_reviewed(uid):
    try:
        connection = connect()
        cursor = connection.cursor()

        # Get releases for a viewer
        get_releases_reviewed_command = """
            SELECT DISTINCT r.rid, r.genre, r.title
            FROM Releases r
            JOIN Reviews rv ON r.rid = rv.rid
            WHERE rv.uid = %s
            ORDER BY r.title ASC;
        """
        
        cursor.execute(get_releases_reviewed_command, (uid,))
        results = cursor.fetchall()

        cursor.close()
        connection.close()

        # Check if results exists and print
        if results:
            for row in results:
                print(f"{row[0]},{row[1]},{row[2]}")
        else:
            print("No reviews found.")
    
    except Exception as error:
        print(f"Error in get_releases_reviewed as: {error}")
        cursor.close()
        connection.close()

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
        'import': lambda: import_data(params[0]),
        'insertViewer': lambda: insert_viewer(params[0], params[1], params[2], params[3], params[4], params[5], params[6], params[7], params[8], params[9], params[10], params[11]),
        'insertMovie': lambda: insert_movie(params[0], params[1]),
        'listReleases': lambda: get_releases_reviewed(params[0])
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