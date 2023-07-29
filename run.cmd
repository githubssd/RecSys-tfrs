docker build --build-arg GUNICORN_WORKERS=8 -t my_fastapi_app .
docker run -d -p 8000:8000 -e GUNICORN_WORKERS=8 my_fastapi_app