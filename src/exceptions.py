# src/exceptions.py

class QueryError(Exception):
    pass


# Raised by service.ActorProxy when ray.get() times out.
class QueryTimeoutError(Exception):
    pass


class S3Error(Exception):
    pass