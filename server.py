from http.server import SimpleHTTPRequestHandler, HTTPServer
import os

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
        ul {{
            list-style-type: none;
            padding: 0;
            text-align: center;
        }}
        li {{
            margin-bottom: 10px;
        }}
        li a {{
            display: inline-block;
            padding: 10px 20px;
            background-color: #007bff;
            color: #fff;
            text-decoration: none;
            border-radius: 5px;
            transition: background-color 0.3s ease;
        }}
        li a:hover {{
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
        <ul>
            {}
        </ul>
    </div>
</body>
</html>
"""

# Directory containing directories
dir_path = "./html_docs"

# Get list of directories in the path
projects = [d for d in os.listdir(dir_path) if os.path.isdir(os.path.join(dir_path, d))]

# Function to generate list items for projects
def generate_list_items(projects):
    items = ""
    for project in projects:
        items += f'<li><a href="{project}/">{project.capitalize()}</a></li>'
    return items

# HTTP request handler
class MyHTTPRequestHandler(SimpleHTTPRequestHandler):
    # Override the do_GET method to handle directory requests
    def do_GET(self):
        if self.path == '/':
            # Serve the home page
            self.send_response(200)
            self.send_header('Content-type', 'text/html')
            self.end_headers()
            self.wfile.write(home_page_template.format(generate_list_items(projects)).encode())
        else:
            # Serve requested file or directory
            try:
                # Call the superclass method to handle requests
                return SimpleHTTPRequestHandler.do_GET(self)
            except FileNotFoundError:
                # File or directory not found
                self.send_error(404, "File not found")

# Server configuration
host = 'localhost'
port = 8000

# Change the current working directory to the directory containing project directories
os.chdir(dir_path)

# Create HTTP server
httpd = HTTPServer((host, port), MyHTTPRequestHandler)

# Server initialization message
print(f"Server running at http://{host}:{port}")

# Start server
httpd.serve_forever()
