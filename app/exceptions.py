class DataNotFoundError(Exception):
    """Raised when expected data is not found in the database."""

    def __init__(self, message: str):
        super().__init__(message)
        self.message = message

    def __str__(self):
        return self.message
