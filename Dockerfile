# Use the official Python base image
FROM python:3.10-slim

WORKDIR /app

COPY ./server.py /app

# Expose the port that your app runs on
EXPOSE 8000

# Run your Python script
CMD ["python3", "server.py"]
