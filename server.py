from http.server import SimpleHTTPRequestHandler, HTTPServer
import os
import sys
import logging
import re
from contextlib import contextmanager
from typing import Generator
import configparser
from google.cloud import storage
from pathlib import Path
config = configparser.ConfigParser()
config.read('config.ini') 
docs_path= config.get('Settings', 'docs_path')
gcs_bucket= config.get('Settings', 'gcs_bucket')
key_path = config.get('Settings', 'key_path')
host= config.get('Settings', 'host')
port= int(config.get('Settings', 'port'))

logging.basicConfig(
    stream=sys.stdout,  # Log to standard output
    level=logging.INFO, # Set the logging level to INFO
    format='%(asctime)s - %(levelname)s - %(message)s'
)

home_page_template = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Welcome to Our Projects Documentation</title>
    <style>
        body {{
            font-family: Arial, sans-serif;
            margin: 0;
            padding: 0;
            background-color: #f4f4f4;
        }}
        .container {{
            max-width: 800px;
            margin: 50px auto;
            padding: 20px;
            background-color: #fff;
            border-radius: 5px;
            box-shadow: 0 2px 5px rgba(0,0,0,0.1);
        }}
        header {{
            text-align: center;
            padding: 20px 0;
        }}
        header img {{
            max-width: 100px;
            height: auto;
        }}
        h1 {{
            text-align: center;
            margin-bottom: 30px;
        }}
        .card {{
            background-color: #fff;
            border-radius: 5px;
            box-shadow: 0 2px 5px rgba(0,0,0,0.1);
            padding: 20px;
            margin-bottom: 20px;
        }}
        .card h2 {{
            margin-top: 0;
        }}
        .card a {{
            display: block;
            padding: 10px 20px;
            background-color: #007bff;
            color: #fff;
            text-decoration: none;
            text-align: center;
            border-radius: 5px;
            transition: background-color 0.3s ease;
        }}
        .card a:hover {{
            background-color: #0056b3;
        }}
    </style>
</head>
<body>
    <div class="container">
        <header>
            <img src="logo.png" alt="Logo">
        </header>
        <h1>Welcome to Our Projects Documentation</h1>
        {}
    </div>
</body>
</html>
"""

@contextmanager
def get_gcs_connection() -> Generator[storage.Client, None, None]:

    gcsclient = storage.Client(project="pricing-338819")
    try:
        yield gcsclient
    finally:
        ...

def download_gcs_file(gcs_bucket):
    with get_gcs_connection() as gcsclient:
        bucket = gcsclient.get_bucket(gcs_bucket)
        blobs = bucket.list_blobs(prefix='dbt_docs')
        for blob in blobs:
            filename = Path(blob.name)
            filename.parent.mkdir(parents=True, exist_ok=True)
            blob.download_to_filename(filename)

def generate_project_cards(projects):
    cards = ""
    for project in projects:
        cards += f"""
        <div class="card">
            <h2>{project.split('.')[0].replace('_', ' ').title()}</h2>
            <a href="{project}">View Documentation</a>
        </div>
        """
    return cards


def inject_html_into_index(docs_path):
    projects = [p for p in os.listdir(docs_path) if os.path.isfile(os.path.join(docs_path, p)) and 'html' in p]
    for project in projects:
        index_file_path = os.path.join(docs_path, project)
        logging.info(f"Inject sylndr logo, {index_file_path}")
        html_line_to_inject = '<a href="/"><img style="width: 62; height: 25px; float:right; margin-left:50px;" class="logo" src="logo.png" alt="Sylndr Logo"></a>'
        with open(index_file_path, 'r') as f:
            content = f.read()
        if html_line_to_inject not in content:
            content = re.sub(r'(placeholder="Search for models\.\.\."\s*\/?>)', r'\1\\n' + html_line_to_inject + ' ', content)
            with open(index_file_path, 'w') as f:
                f.write(content)
        else:
            print(f"Logo found in {index_file_path}")
    return projects

class MyHTTPRequestHandler(SimpleHTTPRequestHandler):
    # Override the do_GET method to handle directory requests
    def do_GET(self):
        if self.path == '/':
            # Serve the home page
            self.send_response(200)
            self.send_header('Content-type', 'text/html')
            self.end_headers()
            self.wfile.write(home_page_template.format(generate_project_cards(projects)).encode())
        else:
            try:
                return SimpleHTTPRequestHandler.do_GET(self)
            except FileNotFoundError:
                self.send_error(404, f"File not found")

try:
    logging.info(f"Start downloading docs")
    download_gcs_file("sylndr-dbt-docs")
    try:
        projects = inject_html_into_index(docs_path)
        os.chdir(docs_path)
        httpd = HTTPServer((host, port), MyHTTPRequestHandler)
        logging.info(f"Server running at http://{host}:{port}")
        httpd.serve_forever()
    except KeyboardInterrupt:
        httpd.server_close()
        logging.info("Server stopped by the user")
    except Exception as e:
        logging.info(f"An error occurred {e}")

except Exception as e:
    logging.error(f"Error while downloading docs: {e}")

