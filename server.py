from http.server import SimpleHTTPRequestHandler, HTTPServer
import os
import sys
import threading
import logging
import re


docs_path = "./dbt_docs"
host = '0.0.0.0'
port = 8000

# Set up logging
logging.basicConfig(
    stream=sys.stdout,  # Log to standard output
    level=logging.INFO, # Set the logging level to INFO
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# HTML template for the home page
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


# Get list of directories in the path
projects = [d.replace('_', ' ').title() for d in os.listdir(docs_path) if os.path.isdir(os.path.join(docs_path, d))]

def generate_project_cards(projects):
    cards = ""
    for project in projects:
        cards += f"""
        <div class="card">
            <h2>{project}</h2>
            <a href="{project.replace(' ', '_')}/">View Documentation</a>
        </div>
        """
    return cards


def inject_html_into_index(index_file_path, html_line):
    logging.info(f"Inject sylndr logo, {index_file_path}")
    with open(index_file_path, 'r') as f:
        content = f.read()
    if html_line not in content:
        content = re.sub(r'(placeholder="Search for models\.\.\."\s*\/?>)', r'\1\\n' + html_line + ' ', content)
        with open(index_file_path, 'w') as f:
            f.write(content)
    else:
        print(f"Logo found in {index_file_path}")

for project in projects:
    index_file_path = os.path.join(docs_path, project.replace(' ', '_'), "index.html")
    html_line_to_inject = '<a href="/"><img style="width: 62; height: 25px; float:right; margin-left:50px;" class="logo" src="../logo.png" alt="Sylndr Logo"></a>'
    inject_html_into_index(index_file_path, html_line_to_inject)

# HTTP request handler
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
            # Serve requested file or directory
            try:
                # Call the superclass method to handle requests
                return SimpleHTTPRequestHandler.do_GET(self)
            except FileNotFoundError:
                # File or directory not found
                self.send_error(404, "File not found")



os.chdir(docs_path)

def restart_server():
    print(f"Server restsrting at http://{host}:{port}")
    logging.info(f"Server restsrting at http://{host}:{port}")
    os.chdir('../')
    httpd.shutdown()
    httpd.server_close()
    os.execv(sys.executable, [sys.executable] + sys.argv)

httpd = HTTPServer((host, port), MyHTTPRequestHandler)

# Start a thread to restart the server every 5 minutes
try:
    threading.Timer(600.0, restart_server).start()
except Exception as e:
    logging.error(f"Error while starting restart thread: {e}")

try:
    print(f"Server running at http://{host}:{port}")
    logging.info(f"Server running at http://{host}:{port}")
    httpd.serve_forever()
except KeyboardInterrupt:
    httpd.server_close()
    logging.info("Server stopped by the user")
    print("Server stopped")
