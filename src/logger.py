import logging
import os

logger_instance = None

def setup_logger(logFile):
    """Configura o logger para registrar logs no arquivo e console."""
    global logger_instance

    if not logFile:
        raise ValueError("Log file is not defined. Please provide a valid log file path.")

    if logger_instance is None:
        logDir = os.path.dirname(logFile)
        # Ensure the log directory exists
        if logDir and not os.path.exists(logDir):
            os.makedirs(logDir)

        # Create the logger instance
        logger_instance = logging.getLogger("mongosyncdb")
        logger_instance.setLevel(logging.INFO)

        # Avoid adding duplicate handlers
        if not logger_instance.hasHandlers():
            # Create a handler for the log file
            file_handler = logging.FileHandler(logFile, mode='w')
            file_handler.setLevel(logging.INFO)
            file_handler.setFormatter(logging.Formatter('%(asctime)s - %(message)s'))

            # Add the handler to the logger
            logger_instance.addHandler(file_handler)

            # Add output to the console
            console_handler = logging.StreamHandler()
            console_handler.setLevel(logging.INFO)
            console_handler.setFormatter(logging.Formatter('%(asctime)s - %(message)s'))
            logger_instance.addHandler(console_handler)

def log(message, config=None):
    """Log a message, ensuring it includes the database name if available."""
    
    # Handle cases where config is None or database key is missing
    db_name = config['database'] if config and 'database' in config else "default"

    logFile = f"./log/{db_name}.log"
    setup_logger(logFile)  # Pass the correct log file explicitly

    # Include the database name in the log message
    message = f"{message}"

    logger_instance.info(message)
