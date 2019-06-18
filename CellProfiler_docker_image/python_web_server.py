# A simple http server to handle remote httep request for CellProfiler analysis tasks

# *** NOTE: A more stable approach is to use AWS SQS to queue the requests ***
# *** The worker nodes get task from the queue when they are available ***

# Usage::
#     ./python_web_server [<port>]
# Send a GET request::
#     curl http://localhost:port?param=value

# Send a POST request with json body::
#   curl --header "Content-Type: application/json" \
#       --request POST \
#       --data '{"username":"xyz","password":"xyz"}' \
#       http://localhost:8000


# The BaseHTTPServer module has been merged into http.server in Python 3
from http.server import BaseHTTPRequestHandler, HTTPServer
import socketserver
import json
from urllib import parse
import subprocess
import time


# This class will handle any incoming request from
# a browser
class S(BaseHTTPRequestHandler):
    def _set_headers(self):
        self.send_response(200)
        self.send_header('Content-type', 'application/json')
        self.end_headers()

# allow users to use get method to check job status
    def do_GET(self):
        print("in get method")
        self._set_headers()
        parsed_path = parse.urlparse(self.path)
        print(parsed_path.query)
        self.wfile.write(bytes(parsed_path.query, "utf-8"))

        # response = subprocess.check_output(["python", request_id])
        # self.wfile.write(json.dumps(response))

# post method to submit analysis task
    def do_POST(self):
        print("in post method")
        self._set_headers()

        # get the requset body in json format
        self.data_string = self.rfile.read(int(self.headers['Content-Length']))
        data = json.loads(self.data_string)
        print(data)
        self.wfile.write(self.data_string)

        # request_id = parsed_path.path
        # response = subprocess.check_output(["python", request_id])
        # self.wfile.write(json.dumps(response))

    def do_HEAD(self):
        self._set_headers()


def run(server_class=HTTPServer, handler_class=S, port=8000):
    server_address = ('', port)
    myServer = server_class(server_address, handler_class)
    print(time.asctime(), "Server Starts - %s:%s" % server_address)

    try:
        myServer.serve_forever()
    except KeyboardInterrupt:
        pass

    myServer.serve_forever()


if __name__ == "__main__":
    from sys import argv

    if len(argv) == 2:
        run(port=int(argv[1]))
    else:
        run()
