docker build --build-arg GUNICORN_WORKERS=8 -t RecSys_app .
docker run -d -p 8000:8000 -e GUNICORN_WORKERS=8 RecSys_app
