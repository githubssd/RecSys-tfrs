from pydantic import BaseModel
from typing import List
from typing import Dict

# Define Pydantic models for request and response
class UserIDRequest(BaseModel):
    user_id: int

class GenreInput(BaseModel):
    genre_name: str

class MovieRecommendationResponse(BaseModel):
    user_id: int
    recommendations: List[str]

class PopularMoviesResponse(BaseModel):
    user_id: int
    popular_movies: List[str]

class UpNextMoviesResponse(BaseModel):
    user_id: int
    up_next_movies: List[str]

class TopMoviesByGenreResponse(BaseModel):
    top_movies_by_genre: Dict[str, str]
