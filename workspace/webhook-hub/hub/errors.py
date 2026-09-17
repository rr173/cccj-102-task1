"""Domain errors shared by store / facade / API layers."""


class HubError(Exception):
    status = 500
    code = "internal"

    def __init__(self, message="internal error"):
        super().__init__(message)
        self.message = message


class NotFound(HubError):
    status = 404
    code = "not_found"


class Conflict(HubError):
    status = 409
    code = "conflict"


class BadRequest(HubError):
    status = 400
    code = "bad_request"
