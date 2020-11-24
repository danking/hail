from aiohttp import web
from hailtop.utils import HailHTTPUserError


class NonExistentBillingProjectError(HailHTTPUserError):
    def __init__(self, billing_project):
        super().__init__(f'Billing project {billing_project} does not exist.', 'error')

    def http_response(self):
        return web.HTTPNotFound(reason=self.message)


class NonExistentUserError(HailHTTPUserError):
    def __init__(self, user, billing_project):
        super().__init__(f'User {user} is not in billing project {billing_project}.', 'error')

    def http_response(self):
        return web.HTTPNotFound(reason=self.message)


class ClosedBillingProjectError(HailHTTPUserError):
    def __init__(self, billing_project):
        super().__init__(f'Billing project {billing_project} is closed and cannot be modified.', 'error')


class InvalidBillingLimitError(HailHTTPUserError):
    def __init__(self, billing_limit):
        super().__init__(f'Invalid billing_limit {billing_limit}.', 'error')

    def http_response(self):
        return web.HTTPBadRequest(reason=self.message)


class NonExistentBatchError(HailHTTPUserError):
    def __init__(self, batch_id):
        super().__init__(f'Batch {batch_id} does not exist.', 'error')


class OpenBatchError(HailHTTPUserError):
    def __init__(self, batch_id):
        super().__init__(f'Batch {batch_id} is open.', 'error')
