from src import logger
import pymongo
from pymongo import MongoClient
from pymongo.errors import OperationFailure
from pymongo.write_concern import WriteConcern
import sys
from bson.timestamp import Timestamp
import time
from dateutil import tz
from datetime import datetime

applyCount = 0
last_batch_time = time.time()

def convert_timestamp_in_datetime(timestamp):
    """Converts a BSON timestamp to a local datetime."""
    to_zone = tz.tzlocal()
    dt_naive_utc = datetime.fromtimestamp(timestamp)
    return dt_naive_utc.astimezone(to_zone)

def getResumeTimeStamp(config): 
    """Retrieves the last resume timestamp from the checkpoint collection."""
    doc = config["colCheckpoint"].find_one(
        { "database": config["database"], "parameter": "resumeTimestamp" },
        { "_id": 0, "ts": 1 }
    )

    if doc:
        timestamp = doc['ts']
        timestamp_ts = timestamp.time
        timestamp_inc = timestamp.inc
        config["resumeTimeStamp"] = Timestamp(timestamp_ts, timestamp_inc + 1)

def saveResumeTimeStamp(timestamp, config):
    """Saves the current timestamp to the checkpoint collection to allow resuming later."""
    try:
        checkpointCollection = config["clientTarget"]["mongosyncdb"]["checkpoints"]
        checkpointCollection.with_options(write_concern=WriteConcern(w=config["changeStream"]["writeConcern"])).update_one(
            {"database": config["database"], "parameter": "resumeTimestamp"},
            {"$set": {"ts": timestamp}},
            upsert=True
        )
        config["resumeTimeStamp"] = timestamp
        logger.log(f"Checkpoint saved: {timestamp}",config)

    except Exception as e:
        logger.log(f"Error saving checkpoint for database '{config['database']}': {str(e)}",config)
        sys.exit(1)  # Stop execution if checkpoint can't be saved

def start(config):
    """Starts the MongoDB Change Stream and applies changes to the target database."""
    config["dbCheckpoint"] = config["clientTarget"]["mongosyncdb"]
    config["colCheckpoint"] = config["dbCheckpoint"]["checkpoints"]

    if not config["changeStream"]["resume"]:
        start_timestamp = config["lastTimestampFromOplog"]
        start_position = convert_timestamp_in_datetime(start_timestamp.time)
        logger.log(f"ChangeStream started for database '{config['database']}' from '{start_timestamp}' - ({start_position})",config)
    else:
        getResumeTimeStamp(config)
        if "resumeTimeStamp" not in config:
            logger.log(f"Error: No checkpoint found for database '{config['database']}', but resume=True was used!",config)
            logger.log("Solution: Either start with `resume=False` to perform an initial load OR manually add a checkpoint.",config)
            sys.exit(1)  # Abort execution safely
    
        start_timestamp = config["resumeTimeStamp"]
        start_position = convert_timestamp_in_datetime(start_timestamp.time)
        logger.log(f"ChangeStream resumed for database '{config['database']}' from '{start_timestamp}' - ({start_position})",config)
    
    # Start Change Stream
    with config["dbSource"].watch(start_at_operation_time=start_timestamp, full_document="updateLookup") as stream:
        for change in stream:
            apply_change_to_target(change, config)

def apply_change_to_target(change, config):
    """Applies the changes received from the Change Stream to the target database."""
    try:
        global applyCount
        global last_batch_time

        clusterTime = change["clusterTime"]
        operation = change["operationType"]
        collectionName = change["ns"]["coll"]
        coll = config["dbTarget"][collectionName]

        try:  # Make sure all database writes are inside this try block
            if operation == "insert":
                document = change["fullDocument"]
                coll.with_options(write_concern=WriteConcern(w=config["changeStream"]["writeConcern"])).replace_one(
                    {"_id": document["_id"]}, document, upsert=True
                )
            
            elif operation == "update":
                documentId = change["documentKey"]["_id"]
                updateFields = change["updateDescription"]["updatedFields"]
                removeFields = change["updateDescription"]["removedFields"]

                coll.with_options(write_concern=WriteConcern(w=config["changeStream"]["writeConcern"])).update_one(
                    { "_id": documentId }, { "$set": updateFields }
                )

                for field in removeFields:
                    coll.with_options(write_concern=WriteConcern(w=config["changeStream"]["writeConcern"])).update_one(
                        { "_id": documentId }, { "$unset": { field: 1 } }
                    )
            
            elif operation == "replace":
                document = change["fullDocument"]
                coll.with_options(write_concern=WriteConcern(w=config["changeStream"]["writeConcern"])).replace_one(
                    {"_id": document["_id"]}, document
                )
            
            elif operation == "delete":
                documentId = change["documentKey"]["_id"]
                coll.with_options(write_concern=WriteConcern(w=config["changeStream"]["writeConcern"])).delete_one(
                    {"_id": documentId}
                )
            
            elif operation == "rename":
                collectionNameTo = change["to"]["coll"]
                coll.with_options(write_concern=WriteConcern(w=config["changeStream"]["writeConcern"])).rename(collectionNameTo)
            
            elif operation in ["dropDatabase", "drop"]:
                logger.log(f"ChangeStream operation '{operation}' received, synchronization aborted!",config)
                sys.exit(1)  # 🔴 Stop execution immediately

            else:
                logger.log(f"Unsupported ChangeStream operation '{operation}' detected. Synchronization failed!",config)
                sys.exit(1)  # 🔴 Stop execution immediately

        except pymongo.errors.PyMongoError as e:
            logger.log(f"Error writing to target database: {str(e)}",config)
            logger.log("Sync aborted due to write failure.",config)
            sys.exit(1)  # Stop execution if any write operation fails
        
        applyCount += 1
        
        # Checkpoint when reaching checkpointBatchSize or checkpointTimeInterval
        if applyCount == config["changeStream"]["checkpointBatchSize"] or time.time() - last_batch_time >= config["changeStream"]["checkpointTimeInterval"]:
            saveResumeTimeStamp(clusterTime, config)
            checkpointPosition = convert_timestamp_in_datetime(clusterTime.time)
            logger.log(f"Checkpoint completed, position '{checkpointPosition}' - {clusterTime}",config)
            applyCount = 0
            last_batch_time = time.time()

    except Exception as e:
        logger.log("ChangeStream processing failed!",config)
        logger.log(str(e),config)
        sys.exit(1)  # Stop execution on failure