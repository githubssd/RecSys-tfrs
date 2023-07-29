# Use the official Python base image
FROM python:3.9-slim

# Set the working directory in the container
WORKDIR /app

# Copy the source code and requirements.txt to the working directory
COPY . /app/

# Install the required dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Expose the Gunicorn port (replace 8000 with the actual port used in the FastAPI app)
EXPOSE 8080

# Set the number of worker processes (adjust as needed)
ENV GUNICORN_WORKERS=4

# Start the FastAPI server with Gunicorn when the container runs
CMD ["gunicorn", "main:app", "-b", "0.0.0.0:8080", "--workers", "${GUNICORN_WORKERS}", "--access-logfile", "-"]
