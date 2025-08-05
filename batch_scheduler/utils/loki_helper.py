from config import LOKI


class LokiClient:
    def __init__(self, logger):
        self.logger = logger

    def get_url(self):
        return f"http://{LOKI.HOST}:{LOKI.PORT}/loki/api/v1/push"
