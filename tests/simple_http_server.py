import os
import json
from http.server import SimpleHTTPRequestHandler, HTTPServer


class JSONDirectoryHandler(SimpleHTTPRequestHandler):
    def list_directory(self, path):
        try:
            encoded = json.dumps(os.listdir(path)).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(encoded)))
            self.end_headers()
            self.wfile.write(encoded)
        except os.error:
            self.send_error(404, "No permission to list directory")


if __name__ == "__main__":
    port = 8000
    server_address = ("0.0.0.0", port)
    httpd = HTTPServer(server_address, JSONDirectoryHandler)
    print(f"Serving on port {port}")
    httpd.serve_forever()
