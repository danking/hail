import os
import threading
import time
import requests
from werkzeug.serving import make_server
from flask import Response

from hailtop import httpx


class ServerThread(threading.Thread):
    def __init__(self, app):
        super().__init__()

        @app.route('/ping', methods=['GET'])
        def ping():
            return Response(status=200)

        self.host = os.environ['HAIL_BATCH_WORKER_IP']
        self.port = os.environ['HAIL_BATCH_WORKER_PORT']
        self.app = app
        self.server = make_server('0.0.0.0', 5000, app)
        self.context = app.app_context()
        self.context.push()

    def url_for(self, uri):
        return f'http://{self.host}:{self.port}{uri}'

    def ping(self):
        ping_url = 'http://{}:{}/ping'.format(self.host, self.port)

        up = False
        with httpx.blocking_client_session() as session:
            while not up:
                try:
                    with session.get(ping_url) as resp:
                        resp.text()
                    up = True
                except requests.exceptions.ConnectionError:
                    time.sleep(0.01)

    def start(self):
        super().start()
        self.ping()

    def run(self):
        self.server.serve_forever()

    def shutdown(self):
        self.server.shutdown()
