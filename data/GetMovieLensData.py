import pandas as pd
import sqlite3
import uuid

def load_data(file_path, column_names, separator='::'):
    return pd.read_csv(file_path, sep=separator, engine='python', header=None, names=column_names, encoding='latin-1')

def load_data_from_sqlite(db_filename):
    conn = sqlite3.connect(db_filename)

    # Read data from the 'movies', 'users', and 'ratings' tables using pandas
    tables = ['movies', 'users', 'ratings']
    data = {}
    for table in tables:
        query = f'SELECT * FROM {table};'
        data[table] = pd.read_sql_query(query, conn)

    # Close the database connection
    conn.close()

    return data['movies'], data['users'], data['ratings']

def map_age_ranges(age):
    age_ranges_dict = {
        1: "Under 18",
        18: "18-24",
        25: "25-34",
        35: "35-44",
        45: "45-49",
        50: "50-55",
        56: "56+"
    }
    return age_ranges_dict.get(age, "Unknown")

def map_occupation(occupation):
    occupation_dict = {
        0: "other or not specified",
        1: "academic/educator",
        2: "artist",
        3: "clerical/admin",
        4: "college/grad student",
        5: "customer service",
        6: "doctor/health care",
        7: "executive/managerial",
        8: "farmer",
        9: "homemaker",
        10: "K-12 student",
        11: "lawyer",
        12: "programmer",
        13: "retired",
        14: "sales/marketing",
        15: "scientist",
        16: "self-employed",
        17: "technician/engineer",
        18: "tradesman/craftsman",
        19: "unemployed",
        20: "writer"
    }
    return occupation_dict.get(occupation, "Unknown")

def preprocess_data(data):
    data['age'] = data['age'].apply(map_age_ranges)
    data['occupation'] = data['occupation'].apply(map_occupation)
    data['user_id'] = data['user_id'].map({user_id: str(uuid.uuid4()) for user_id in data['user_id'].unique()})
    data['movie_id'] = data['movie_id'].map({movie_id: str(uuid.uuid4())[:8] for movie_id in data['movie_id'].unique()})
    return data

def main():
    data_path = 'movie-lens-1m/'
    db_filename = 'sql_database/movielens.db'

    # Load data from CSV files
    movies_columns = ['movie_id', 'title', 'genres']
    movies = load_data(data_path + 'movies.dat', column_names=movies_columns)

    users_columns = ['user_id', 'gender', 'age', 'occupation', 'zipcode']
    users = load_data(data_path + 'users.dat', column_names=users_columns)

    ratings_columns = ['user_id', 'movie_id', 'rating', 'timestamp']
    ratings = load_data(data_path + 'ratings.dat', column_names=ratings_columns)

    # Save DataFrames to SQL tables
    conn = sqlite3.connect(db_filename)
    movies.to_sql('movies', conn, if_exists='replace', index=False)
    users.to_sql('users', conn, if_exists='replace', index=False)
    ratings.to_sql('ratings', conn, if_exists='replace', index=False)
    conn.close()

    # Load data from SQLite
    movies, users, ratings = load_data_from_sqlite(db_filename)

    # Combine the data using SQL query
    combined_data_query = '''
    SELECT ratings.*, movies.title, movies.genres, users.gender, users.age, users.occupation, users.zipcode
    FROM ratings
    INNER JOIN movies ON ratings.movie_id = movies.movie_id
    INNER JOIN users ON ratings.user_id = users.user_id;
    '''

    conn = sqlite3.connect(db_filename)
    combined_data = pd.read_sql_query(combined_data_query, conn)
    conn.close()

    # Preprocess data
    combined_data = preprocess_data(combined_data)

    # New Movie Data
    movies = combined_data[['movie_id', 'title', 'genres']]
    movies = movies.drop_duplicates(subset=['movie_id', 'title', 'genres'], keep='first')


    # New User Data
    users = combined_data[['user_id', 'gender', 'age', 'occupation', 'zipcode']]
    users = users.drop_duplicates(subset=['user_id', 'gender', 'age', 'occupation', 'zipcode'], keep='first')

    # New Rating Data
    ratings = combined_data[['user_id', 'movie_id', 'rating', 'timestamp']]
    ratings = ratings.drop_duplicates(subset=['user_id', 'movie_id', 'rating', 'timestamp'], keep='first')

    # Save DataFrames to SQL tables
    conn = sqlite3.connect(db_filename)
    movies.to_sql('movies', conn, if_exists='replace', index=False)
    users.to_sql('users', conn, if_exists='replace', index=False)
    ratings.to_sql('ratings', conn, if_exists='replace', index=False)
    combined_data.to_sql('combined_data', conn, if_exists='replace', index=False)
    conn.close()

if __name__ == "__main__":
    main()
