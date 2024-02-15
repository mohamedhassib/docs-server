# Simple Python Web Server

This is a simple Python web server that serves static dbt projects docs.

## Features

- Serves projects static files and directories over HTTP.
- Handles directory requests by serving an index file if present.
- Handles 404 errors for files or directories not found.

## Usage

1. Generate dbt docs using `dbt docs generate`.
2. Move the project docs directory inside `dbt_docs` dir.
3. Run the server using the following command: `python3 server.py`
4. Access the server at `http://localhost:8000` in your web browser.

## With Docker

1. Build app image `docker build -t docs-server . `
2. Run a container `docker run -d -p 8000:8000 -v ./dbt_docs:/app/dbt_docs --name docs-server docs-server`

## Contributing

Contributions are welcome! If you find any issues or have suggestions for improvements, please feel free to open an issue or submit a pull request.