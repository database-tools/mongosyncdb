# MongoSyncDB
MongoSyncDB is a tool to copy a mongodb database from one replicaset to another, keeping source and target synchronized through changestream.

### Install
```
python3 -m venv env
source env/bin/activate
pip3 install -r requirements.txt
```

### Run
```
python3 main.py --config-file mydb.yaml 
```
\* Use the config-template.yaml file as a template to create your configuration file.

### Config File Parameters
| Parameter                                     |  Description                                                                                                  | Sample Value 
|:----------------------------------------------|:--------------------------------------------------------------------------------------------------------------|:-----------------
| `database`                                    |  Name of database that will be syncronized                                                                    | dba
| `source.hostname`                             |  Hostname of source replicaset                                                                                | host1
| `source.port`                                 |  Port of source replicaset                                                                                    | 27017
| `source.username`                             |  Username of source replicaset                                                                                | mongosyncdb
| `source.password`                             |  Password of source replicaset                                                                                | mypassword
| `target.hostname`                             |  Hostname of target replicaset                                                                                | host2
| `target.port`                                 |  Port of target replicaset                                                                                    | 27017
| `target.username`                             |  Username of target replicaset                                                                                | mongosyncdb
| `target.password`                             |  Password of target replicaset                                                                                | mypassword
| `initialLoad.batchSize`                       |  Quantity of documents by fetch                                                                               | 10000
| `initialLoad.maxWorkers`                      |  Number of collections to load in parallel                                                                    | 2
| `changeStream.resume`                         |  When `False` it does an initial load + cdc and when `True` it resumes cdc from the last checkpoint position  | `True` or `False`
| `changeStream.checkpointBatchSize`            |  Number of transactions to make a checkpoint                                                                  | 100
| `changeStream.checkpointTimeInterval`         |  Number of seconds to make a checkpoint                                                                       | 60
| `writeConcern`                                |  Write concern to write data on target replicaset                                                             | 1

### User Privileges
#### source
```
use admin
db.createUser( { user: "mongosyncdb", 
pwd: "mypassword", 
roles: [
  { "role" : "read", "db" : "local" },
  { "role" : "readAnyDatabase", "db" : "admin" },
  { "role" : "hostManager", "db" : "admin" }
]})
```

#### target
```
use admin
db.createUser( { user: "mongosyncdb", 
pwd: "mypassword", 
roles: [
   { "role" : "readWriteAnyDatabase", "db" : "admin" }
]})
```
### Supported operations by changestream process
- insert  - Occurs when an operation adds documents to a collection.
- update  - Occurs when an operation updates a document in a collection.
- delete  - Occurs when a document is removed from the collection.
- drop    - Occurs when a collection is dropped from a database.
- rename  - Occurs when a collection is renamed.
- replace - Occurs when an update operation removes a document from a collection and replaces it with a new document.

### Important Notes
\* This utility was developed and tested with mongodb 5.0.

\* Operations not supported will not be replicated on target database (createIndex, dropIndex, create, etc.).<br>
For more information check the https://www.mongodb.com/docs/v5.0/reference/change-events/.

\* To more information about mongodb change streams check the mongodb oficial documentation. <br>
https://www.mongodb.com/docs/manual/changeStreams/ <br>
https://www.mongodb.com/developer/languages/python/python-change-streams/ <br>
https://www.mongodb.com/docs/manual/administration/change-streams-production-recommendations <br>
https://www.mongodb.com/docs/manual/reference/change-events/ <br>
