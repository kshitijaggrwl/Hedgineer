import logging
import os


class Logger:
    signal = None
    request = None
    response = None

    @classmethod
    def setup_logger(cls, base_path: str):
        """
        Sets up three different loggers as class attributes:
        - signal: Logs signal-related events, e.g., system signals or lifecycle events.
        - request: Logs incoming HTTP requests.
        - response: Logs outgoing HTTP responses.

        Logs are written to different files for each logger:
        - signal.log
        - request.log
        - response.log

        Args:
            base_path (str): The directory where log files will be stored.
        """
        # Ensure the base path exists, create it if necessary
        if not os.path.exists(base_path):
            os.makedirs(base_path)

        # Setup each logger with a different file handler
        cls.signal = cls._setup_logger("signal", base_path, "signal.log")
        cls.request = cls._setup_logger("request", base_path, "request.log")
        cls.response = cls._setup_logger("response", base_path, "response.log")

        # Optionally, log setup completion
        cls.signal.info("Logger setup completed")

    @staticmethod
    def _setup_logger(name: str, base_path: str, log_file: str) -> logging.Logger:
        """
        Helper function to set up the logger with the provided name and log file.

        Args:
            name (str): The name of the logger (e.g., "signal", "request", "response").
            base_path (str): The base directory where the log file should be saved.
            log_file (str): The file to write the logs to.

        Returns:
            logging.Logger: The configured logger.
        """
        logger = logging.getLogger(name)
        logger.setLevel(logging.INFO)

        # Create a file handler for each logger with its own log file
        file_path = os.path.join(base_path, log_file)
        file_handler = logging.FileHandler(file_path)
        file_handler.setLevel(logging.INFO)

        # Set log format
        formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )
        file_handler.setFormatter(formatter)

        # Add file handler to the logger
        logger.addHandler(file_handler)

        return logger
