from concurrent.futures import ThreadPoolExecutor, as_completed
from src import logger
import sys
import pymongo
import bson
import math
from pymongo.write_concern import WriteConcern
from bson.codec_options import CodecOptions, DatetimeConversion

def insertMany(coll, doc, countSource, config):
    """Inserts multiple documents into the target collection and logs progress."""
    targetCollection = config["dbTarget"][coll]
    targetCollection.with_options(write_concern=WriteConcern(w=config["changeStream"]["writeConcern"])).insert_many(doc)
    
    countTarget = targetCollection.estimated_document_count()
    if countTarget <= countSource:
        percent = math.ceil((countTarget / countSource) * 100)
        logger.log(f"Loading collection '{coll}' - {percent}% completed",config)

def loadCollection(coll, config):
    """Loads data from a source collection and inserts it into the target database."""
    logger.log(f"Fetching data from collection '{coll}'",config)
    
    sourceCollection = config["dbSource"][coll]
    targetCollection = config["dbTarget"][coll]
    
    countSource = sourceCollection.estimated_document_count()
    
    if countSource > 0:
        logger.log(f"Collection '{coll}' has {countSource} documents",config)
        documents = sourceCollection.find_raw_batches({}, batch_size=config["initialLoad"]["batchSize"])
        
        for doc in documents:
            codec_auto = CodecOptions(datetime_conversion=DatetimeConversion.DATETIME_AUTO)
            decoded_doc = bson.decode_all(doc, codec_options=codec_auto)
            insertMany(coll, decoded_doc, countSource, config)
        
        logger.log(f"Collection '{coll}' imported successfully",config)
    else:
        logger.log(f"Collection '{coll}' is empty",config)
        targetCollection.insert_one({})
        targetCollection.delete_one({})

    createIndexes(sourceCollection, targetCollection, coll, config)

def loadCollections(config):
    """Loads all collections in parallel using ThreadPoolExecutor."""
    try:
        logger.log("Starting parallel collection import",config)

        collections = config["dbSource"].list_collection_names(
            filter={"type": "collection", "name": {"$nin": ["system.profile", "system.views"]}}
        )

        # Read max_workers from config.yaml
        max_workers = config["initialLoad"].get("maxWorkers")
        max_workers = min(len(collections), max_workers)  # Ensure we don’t create more threads than needed
        logger.log(f"Using {max_workers} parallel workers for collection import",config)

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_collection = {executor.submit(loadCollection, coll, config): coll for coll in collections}

            for future in as_completed(future_to_collection):
                coll = future_to_collection[future]
                try:
                    future.result()  # Ensure any exceptions are caught
                    logger.log(f"Finished loading collection: {coll}",config)
                except Exception as e:
                    logger.log(f"Collection '{coll}' failed: {str(e)}",config)

        logger.log("All collections loaded successfully!")

    except Exception as e:
        logger.log("Failed to import collections!",config)
        logger.log(str(e),config)
        sys.exit()

def createIndexes(sourceCollection, targetCollection, coll, config):
    """Creates indexes in the target database based on the source collection."""
    indexes = sourceCollection.index_information()
    
    for index_name, index_info in indexes.items():
        if index_name == "_id_":  # Ignore the default _id index
            continue

        try:
            index_keys = [(k, int(v)) for k, v in index_info["key"]]  
            index_options = {k: v for k, v in index_info.items() if k not in ["key", "ns"]}

            targetCollection.create_index(index_keys, name=index_name, **index_options)
            logger.log(f"Index '{index_name}' created in collection '{coll}'",config)

        except pymongo.errors.OperationFailure as e:
            logger.log(f"Failed to create index '{index_name}' in collection '{coll}': {str(e)}",config)

def createViews(config):
    """Creates views in the target database based on source views."""
    try:
        logger.log("Creating database views",config)
        collections_info = config["dbSource"].command("listCollections", filter={"type": "view"})

        for view_info in collections_info["cursor"]["firstBatch"]:
            view_name = view_info["name"]
            source = view_info["options"]["viewOn"]
            pipeline = view_info["options"]["pipeline"]

            logger.log(f"Creating view '{view_name}' on source '{source}' with pipeline: {pipeline}",config)

            if view_name in config["dbTarget"].list_collection_names():
                config["dbTarget"][view_name].drop()

            config["dbTarget"].command(
                "create",
                view_name,
                viewOn=source,
                pipeline=pipeline
            )
            logger.log(f"View '{view_name}' created successfully",config)

    except Exception as e:
        logger.log("Failed to create views!",config)
        logger.log(str(e),config)
        sys.exit()

def getLastTimestampFromOplog(config):
    """Fetches the latest timestamp from the oplog and logs it."""
    dbSource = config["clientSource"]["local"]
    coll = dbSource["oplog.rs"]
    doc = coll.find_one({}, sort=[('$natural', -1)])

    if doc:
        config["lastTimestampFromOplog"] = doc["ts"]
        logger.log(f"Oplog timestamp collected: {config['lastTimestampFromOplog']}",config)
    else:
        logger.log("No oplog entries found! Change Stream may not start correctly.",config)
        sys.exit(1)  # Stop execution if no oplog entry is found
