import json
import pytest
from fastapi.testclient import TestClient
from app import app

# Initialize the TestClient with the FastAPI app
client = TestClient(app)

def test_health_check():
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "healthy"}

def test_recommend_movies_v1():
    response = client.get("/v1/recommend/123")
    assert response.status_code == 200
    data = response.json()
    assert "user_id" in data
    assert "recommendations" in data

def test_get_popular_movie_v1():
    response = client.get("/v1/popular?user_id=123")
    assert response.status_code == 200
    data = response.json()
    assert "user_id" in data
    assert "popular_movies" in data

def test_get_up_next_movie_v1():
    response = client.get("/v1/upnext?user_id=123&movies=movie1,movie2,movie3")
    assert response.status_code == 200
    data = response.json()
    assert "user_id" in data
    assert "up_next_movies" in data

def test_get_top_movies_by_genre():
    response = client.get("/v1/top_movies_by_genre?genre_name=Action")
    assert response.status_code == 200
    data = response.json()
    assert "top_movies_by_genre" in data
    assert isinstance(data["top_movies_by_genre"], dict)

# Add more test cases for edge cases and error scenarios as needed

if __name__ == "__main__":
    pytest.main()
