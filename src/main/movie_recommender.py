from typing import List, Dict

# Sample movie data 
movies = {
    1: {"name": "Movie A", "genre": "Action"},
    2: {"name": "Movie B", "genre": "Comedy"},
    3: {"name": "Movie C", "genre": "Drama"},
    # Add more movies here
}

# Sample user preferences data 
user_preferences = {
    101: [1, 3],  # User 101 likes Movie A and Movie C
    102: [2, 3],  # User 102 likes Movie B and Movie C
}

def is_valid_user_id(user_id: int) -> bool:
    return user_id in user_preferences

def get_movie_recommendations(user_id: int) -> List[str]:
    if not is_valid_user_id(user_id):
        return []  
    pass

def get_popular_movies() -> List[Dict[str, str]]:
    pass

def get_up_next_movies(movie_sequence: List[str]) -> List[str]:
    pass
