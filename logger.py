# logger.py
import logging
import os

def setup_logger(name: str, log_file: str = "etl_log.txt", level: int = logging.INFO) -> logging.Logger:
    """Sets up a logger with the specified name and log file."""
    logger = logging.getLogger(name)
    logger.setLevel(level)

    # Prevent duplicate handlers if logger already exists
    if not logger.hasHandlers():
        # Create a console handler
        ch = logging.StreamHandler()
        ch.setLevel(level)

        # Create a formatter and add it to the handler
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        ch.setFormatter(formatter)

        # Add the console handler to the logger
        logger.addHandler(ch)

        # If a log file is specified, also log to the file
        if log_file:
            # Specify the directory where the log file will be saved
            log_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "logs")
            log_file_path = os.path.join(log_dir, log_file)

            # Ensure the directory exists
            try:
                os.makedirs(log_dir, exist_ok=True)
            except Exception as e:
                logger.error(f"Error creating log directory: {e}")

            try:
                fh = logging.FileHandler(log_file_path)
                fh.setLevel(level)
                fh.setFormatter(formatter)
                logger.addHandler(fh)
                logger.info(f"Logging to file: {log_file_path}")
            except Exception as e:
                logger.error(f"Error setting up file logging: {e}")

    return logger
