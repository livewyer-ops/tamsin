"""Serve HTTP fixtures on an ephemeral loopback port and publish readiness."""

import functools
import http.server
import pathlib
import sys

handler = functools.partial(http.server.SimpleHTTPRequestHandler, directory=sys.argv[1])
with http.server.ThreadingHTTPServer(("127.0.0.1", 0), handler) as server:
    pathlib.Path(sys.argv[2]).write_text(str(server.server_port), encoding="utf-8")
    server.serve_forever()
