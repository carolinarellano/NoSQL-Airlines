# NoSQL-Airlines
This is a project for the NoSQL class, we are using three databases: Cassandra, MongoDb and Neo4j
# CassandraCQL
## Install project python requirements
```
python3 -m pip install -r requirements.txt
```
## Launch cassandra container
### To start a new container
```
docker run --name node01 -p 9042:9042 -d cassandra
```
### If container already exists just start it
```
docker start node01
```
## IF FIRST TIME USING THE API
### Run the app.py so the table can be created and exit the app.py
```
python3 app.py
```
### Generate data
```
python3 flight_data.py  -> data will we writen on flight_passengers.csv
```
### Extract data
```
python3 extraccion.py  -> data will be formated into .cql to be inserted
```
### Copy data to container
##In terminal
```
docker cp tools/data.cql node01:/root/data.cql
```
```
docker exec -it node01 bash -c "cqlsh -u cassandra -p cassandra"
```
##In cqlsh:
```
USE investments;
```
```
SOURCE '/root/data.cql'

```
## IF HAS ALREADY BEEN USED
### Run the app.py    
```
python3 app.py
```
# MongoDB
A place to share mongodb app code
### Setup a python virtual env with python cassandra installed
```
# If pip is not present in you system
sudo apt update
sudo apt install python3-pip

# Install and activate virtual env
python3 -m pip install virtualenv
virtualenv -p python3 ./venv
source ./venv/bin/activate

# Install project python requirements
python3 -m pip install -r requirements.txt
```
### To run the API service
```
python3 -m uvicorn main:app --reload
```
### To load data
Ensure you have a running mongodb instance
i.e.:
```
docker run --name mongodb_arlyn -d -p 27017:27017 mongo
```
Once your API service is running (see step above), run the populate script
```
cd data/
python3 populate.py
```
# Neo4j
A place to share neo4j app code
### Setup a python virtual env with python neo4j installed
```
# If pip is not present in you system
sudo apt update
sudo apt install python3-pip

# Install and activate virtual env
python3 -m pip install virtualenv
virtualenv -p python3 ./venv
source ./venv/bin/activate

# Install project python requirements
python3 -m pip install -r requirements.txt
```
### To load data
Ensure you have a running neo4j instance
i.e.:
```
docker run -d --name=neo4j --publish=7474:7474 --publish=7687:7687 neo4j
```
Run main.py
i.e.:
```
python3 main.py
```
## GDSL Workaround

```
docker cp gds/neo4j.conf neo4j:/var/lib/neo4j/conf/
docker cp gds/neo4j-graph-data-science-2.2.2.jar neo4j:/var/lib/neo4j/plugins/
docker restart neo4j
```

