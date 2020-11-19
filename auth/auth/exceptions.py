from aiohttp import web


class AuthUserError(Exception):
    def __init__(self, message, severity):
        super().__init__(message)
        self.message = message
        self.ui_error_type = severity

    def http_response(self):
        return web.HTTPForbidden(reason=self.message)
