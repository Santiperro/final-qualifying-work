class InvalidDateFormatException(Exception):
    def __init__(self, message="Invalid date format"):
        self.message = message
        super().__init__(self.message)