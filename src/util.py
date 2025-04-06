import yaml
import os
import pymongo
from src import logger  # Ensure logger is correctly imported

def getParameters(config_file, config):
    """Loads all configurations from the YAML file and stores them in the `config` dictionary."""
    try:
        # Check if the configuration file exists
        if not os.path.exists(config_file):
            logger.log(f"Error: Configuration file '{config_file}' does not exist!",config)
            return False

        with open(config_file, 'r') as file:
            file_content = yaml.safe_load(file)

        # If the file is empty or invalid, return an error
        if not file_content:
            logger.log("Error: The configuration file is empty or invalid!",config)
            return False

        # Store all content from YAML into the `config` dictionary
        config.update(file_content)

        # Now it's safe to log (since config['database'] is available)
        logger.log(f"Configuration file '{config_file}' loaded successfully.", config)


        # Establish connections to MongoDB
        logger.log(f"Connecting to source MongoDB: {config['source']['hostname']}:{config['source']['port']}",config)
        config["clientSource"] = pymongo.MongoClient(
            f"mongodb://{config['source']['username']}:{config['source']['password']}@"
            f"{config['source']['hostname']}:{config['source']['port']}/?authSource=admin"
        )
        config["dbSource"] = config["clientSource"][config["database"]]

        logger.log(f"Connecting to target MongoDB: {config['target']['hostname']}:{config['target']['port']}",config)
        config["clientTarget"] = pymongo.MongoClient(
            f"mongodb://{config['target']['username']}:{config['target']['password']}@"
            f"{config['target']['hostname']}:{config['target']['port']}/?authSource=admin"
        )
        config["dbTarget"] = config["clientTarget"][config["database"]]

        return True

    except yaml.YAMLError as e:
        logger.log(f"Error parsing YAML file: {e}",config)
        return False
    except Exception as e:
        logger.log(f"Unexpected error while loading configuration file: {e}",config)
        return False