import logging
from logging.handlers import RotatingFileHandler

class Logger:
    def __init__(self, log_file="./log/app.log", log_level=logging.INFO):
        self.logger_instance = logging.getLogger(__name__)
        self.logger_instance.setLevel(log_level)

        # Create a file handler and set the level to log to a file
        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(log_level)

        # Create a console handler and set the level to log to the console
        console_handler = logging.StreamHandler()
        console_handler.setLevel(log_level)

        # Create a formatter and set the format for the handlers
        formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
        file_handler.setFormatter(formatter)
        console_handler.setFormatter(formatter)

        # Add the handlers to the logger
        self.logger_instance.addHandler(file_handler)
        self.logger_instance.addHandler(console_handler)

    def info(self, message):
        self.logger_instance.info(message)

    def warning(self, message):
        self.logger_instance.warning(message)

    def error(self, message):
        self.logger_instance.error(message)

    def critical(self, message):
        self.logger_instance.critical(message)

    def debug(self, message):
        self.logger_instance.debug(message)


logger = Logger("./logs/app.log", logging.INFO)
