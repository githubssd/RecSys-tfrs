import json
import redis
from datetime import timedelta
import sentry_sdk
from sentry_sdk.integrations.asgi import SentryAsgiMiddleware
from typing import List, Dict, Optional
from kafka import KafkaProducer
from prometheus_fastapi_instrumentator import Instrumentator
from prometheus_client import Counter, Histogram


from fastapi import (
    FastAPI, 
    HTTPException, 
    status, 
    Query)

from models import (
    UserIDRequest, GenreInput, 
    MovieRecommendationResponse,
    TopMoviesByGenreResponse,
    PopularMoviesResponse,
    UpNextMoviesResponse,
    )

from movie_recommender import (
    get_movie_recommendations,
    get_popular_movies,
    get_up_next_movies,
    is_valid_user_id,
    get_top_movies_by_genre_rating,
    get_popular_movies_by_genre
)

# Initialize Sentry
sentry_sdk.init(
    dsn="YOUR_SENTRY_DSN",
    integrations=[SentryAsgiMiddleware],
)

app = FastAPI()

# Initialize Kafka producer (assuming Kafka is already running)
producer = KafkaProducer(
    bootstrap_servers="Kafka broker address",  # Replace with your Kafka broker address
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

# Initialize Prometheus metrics
instrumentator = Instrumentator()
instrumentator.add(Counter("requests_total", "Total number of requests.", ["endpoint", "method", "status"]))
instrumentator.add(Histogram("request_duration_seconds", "Request duration.", ["endpoint", "method", "status"]))


@app.on_event("startup")
async def startup_event():
    # Enable Prometheus metrics middleware
    instrumentator.instrument(app)

redis_client = redis.Redis(host="your_redis_host", port=6379, db=0)


# Updated endpoint with caching for popular movies
def get_popular_movies_cached(genre_name):
    # Check if the response is already in the cache
    cached_response = redis_client.get(f"popular_{genre_name}")
    if cached_response:
        return json.loads(cached_response.decode("utf-8"))

    # Get popular movies based on the specified genre
    popular_movies = get_popular_movies_by_genre(genre_name)

    # Cache the response in Redis with a TTL of 1 hour (3600 seconds)
    redis_client.setex(f"popular_{genre_name}", timedelta(hours=1), json.dumps(popular_movies))

    return popular_movies

@app.get("/v1/popular", response_model=PopularMoviesResponse, status_code=200)
def get_popular_movie_v1(user_id: int, genre_name: Optional[str] = Query(None)):
    """
    Get popular movies.

    Args:
        user_id (int): The ID of the user.
        genre_name (Optional[str]): The name of the genre (optional).

    Returns:
        dict: A dictionary containing the user ID and popular movies.
    """
    request_data = UserIDRequest(user_id=user_id)
    if not is_valid_user_id(request_data.user_id):
        raise HTTPException(status_code=404, detail="User ID not found")

    if genre_name:
        popular_movies = get_popular_movies_cached(genre_name)
    else:
        popular_movies = get_popular_movies_cached("general")

    # Publish the response to Kafka topic
    topic = "fastapi_responses"
    response = {"user_id": request_data.user_id, "popular_movies": popular_movies}
    producer.send(topic, value=response)

    return {"user_id": request_data.user_id, "popular_movies": popular_movies}

# ... (remaining code)

# Add caching for the top-rated movies genre-based endpoint
def get_top_movies_by_genre_cached(genre_name):
    # Check if the response is already in the cache
    cached_response = redis_client.get(f"top_rated_{genre_name}")
    if cached_response:
        return json.loads(cached_response.decode("utf-8"))

    # Assuming you have a function called get_top_movies_by_genre_rating()
    top_movies_by_genre = get_top_movies_by_genre_rating(genre_name)

    if not top_movies_by_genre:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Genre '{genre_name}' not found in the database.",
        )

    # Cache the response in Redis with a TTL of 1 hour (3600 seconds)
    redis_client.setex(f"top_rated_{genre_name}", timedelta(hours=1), json.dumps(top_movies_by_genre))

    return top_movies_by_genre

@app.get("/v1/top_movies_by_genre", response_model=TopMoviesByGenreResponse, status_code=200)
def get_top_movies_by_genre(genre_input: GenreInput):
    """
    Get top movie recommendations based on average ratings for the specified genre.

    Args:
        genre_input (GenreInput): The input model containing the name of the genre.

    Returns:
        dict: A dictionary containing the top movies for the specified genre.
    """
    top_movies_by_genre = get_top_movies_by_genre_cached(genre_input.genre_name)

    return {"top_movies_by_genre": top_movies_by_genre}


@app.get("/v1/recommend/{user_id}", response_model=MovieRecommendationResponse, status_code=200)
def recommend_movies_v1(user_id: int):
    """
    Get movie recommendations for a user.

    Args:
        user_id (int): The ID of the user.

    Returns:
        dict: A dictionary containing the user ID and movie recommendations.
    """
    request_data = UserIDRequest(user_id=user_id)
    if not is_valid_user_id(request_data.user_id):
        raise HTTPException(status_code=404, detail="User ID not found")

    recommendations = get_movie_recommendations(request_data.user_id)

    # Publish the response to Kafka topic
    topic = "fastapi_responses"
    response = {"user_id": request_data.user_id, "recommendations": recommendations}
    producer.send(topic, value=response)

    return {"user_id": request_data.user_id, "recommendations": recommendations}


@app.get("/v1/popular", response_model=PopularMoviesResponse, status_code=200)
def get_popular_movie_v1(user_id: int, genre_name: Optional[str] = Query(None)):
    """
    Get popular movies.

    Args:
        user_id (int): The ID of the user.
        genre_name (Optional[str]): The name of the genre (optional).

    Returns:
        dict: A dictionary containing the user ID and popular movies.
    """
    request_data = UserIDRequest(user_id=user_id)
    if not is_valid_user_id(request_data.user_id):
        raise HTTPException(status_code=404, detail="User ID not found")

    if genre_name:
        # Get popular movies based on the specified genre
        popular_movies = get_popular_movies_by_genre(genre_name)
    else:
        # Get general popular movies
        popular_movies = get_popular_movies()

    # Publish the response to Kafka topic
    topic = "fastapi_responses"
    response = {"user_id": request_data.user_id, "popular_movies": popular_movies}
    producer.send(topic, value=response)

    return {"user_id": request_data.user_id, "popular_movies": popular_movies}


@app.get("/v1/upnext", response_model=UpNextMoviesResponse, status_code=200)
def get_up_next_movie_v1(user_id: int, movies: List[str]):
    """
    Get up next movies for a user.

    Args:
        user_id (int): The ID of the user.
        movies (List[str]): A list of movies.

    Returns:
        dict: A dictionary containing the user ID and up next movies.
    """
    request_data = UserIDRequest(user_id=user_id)
    if not is_valid_user_id(request_data.user_id):
        raise HTTPException(status_code=404, detail="User ID not found")

    up_next_movies = get_up_next_movies(movies)

    # Publish the response to Kafka topic
    topic = "fastapi_responses"
    response = {"user_id": request_data.user_id, "up_next_movies": up_next_movies}
    producer.send(topic, value=response)

    return {"user_id": request_data.user_id, "up_next_movies": up_next_movies}


@app.get("/v1/top_movies_by_genre", response_model=TopMoviesByGenreResponse, status_code=200)
def get_top_movies_by_genre(genre_input: GenreInput):
    """
    Get top movie recommendations based on average ratings for the specified genre.

    Args:
        genre_input (GenreInput): The input model containing the name of the genre.

    Returns:
        dict: A dictionary containing the top movies for the specified genre.
    """
    # Assuming you have a function called get_top_movies_by_genre_rating()
    top_movies_by_genre = get_top_movies_by_genre_rating(genre_input.genre_name)

    if not top_movies_by_genre:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Genre '{genre_input.genre_name}' not found in the database.",
        )

    return {"top_movies_by_genre": top_movies_by_genre}



@app.get("/health")
def check_health():
    """
    Check the health status of the API.

    Returns:
        dict: A dictionary indicating the health status.
    """
    return {"status": "healthy"}