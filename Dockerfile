FROM python:3.10-slim

WORKDIR /app
RUN mkdir -p /app/dbt_docs
COPY ./server.py /app/server.py 
COPY ./dbt_docs/logo.png /app/dbt_docs/logo.png
COPY ./requirements.txt /app/requirements.txt
COPY ./config.ini /app/config.ini
RUN pip install -r requirements.txt

EXPOSE 8000

# Run Python server
CMD ["python3", "server.py"]
