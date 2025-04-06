from src import logger
from src.util import getParameters
from src import initial_load
from src import change_stream
import argparse
import signal
import sys

# Global dictionary to store configuration
config = {}

def checkDatabaseOnTarget():
    """Checks if the database already exists on the target server."""
    dbs = config["clientTarget"].list_database_names()
    
    if config["database"] in dbs:
        logger.log(f"Database '{config['database']}' already exists on the target server, initial load canceled.",config)
        sys.exit()
    
    # Check if there's a saved checkpoint for this database
    checkpointCollection = config["clientTarget"]["mongosyncdb"]["checkpoints"]
    checkpoint = checkpointCollection.find_one({"database": config["database"], "parameter": "resumeTimestamp"})
    
    if checkpoint:
        logger.log(f"Error: A checkpoint exists for database '{config['database']}', but a full reload was requested!",config)
        logger.log(f"To continue, either use `resume=True` or manually remove the checkpoint.",config)
        sys.exit()

    logger.log(f"No checkpoint found for database '{config['database']}', proceeding with initial load.",config)

def startSync():
    """Starts synchronization using the loaded configuration."""
    logger.log(f"Starting synchronization for database: {config['database']}",config)

    if not config["changeStream"]["resume"]:
        # Check if database already exists on target
        checkDatabaseOnTarget()

        # Capture the last oplog timestamp to ensure data consistency
        initial_load.getLastTimestampFromOplog(config)
    
        # Load collections
        initial_load.loadCollections(config)
        
        # Create views
        initial_load.createViews(config)

        # Save the oplog timestamp in the checkpoint collection BEFORE starting Change Stream
        logger.log("Saving the initial oplog timestamp as a checkpoint...",config)
        change_stream.saveResumeTimeStamp(config["lastTimestampFromOplog"], config)
        
    # Start change stream process
    change_stream.start(config)

def graceful_shutdown(signum, frame):
    """Closes MongoDB connections before shutting down."""
    logger.log("Shutting down gracefully...",config)
    config["clientSource"].close()
    config["clientTarget"].close()
    sys.exit(0)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="MongoDB synchronization script.")
    parser.add_argument('--config-file', type=str, required=True, help="Path to the YAML configuration file.")

    args = parser.parse_args()

    if getParameters(args.config_file, config):
        
        # Capture termination signals (Ctrl+C or SIGTERM)
        signal.signal(signal.SIGINT, graceful_shutdown)
        signal.signal(signal.SIGTERM, graceful_shutdown)

        startSync()
    else:
        logger.log("Failed to load configuration file.",config)