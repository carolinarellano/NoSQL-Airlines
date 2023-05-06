### Install project python requirements
python3 -m pip install -r requirements.txt
```
### Launch cassandra container
```
# To start a new container
docker run --name node01 -p 9042:9042 -d cassandra

# If container already exists just start it
docker start node01
```
### IF FIRST TIME USING IT 
```
# Run the app.py so the table can be created and exit the app.py
python3 app.py

# Generate data
python3 flight_data.py  -> data will we writen on flight_passengers.csv

# Extract data
python3 extraccion.py  -> data will be formated into .cql to be inserted

# Copy data to container
#In terminal
docker cp tools/data.cql node01:/root/data.cql
docker exec -it node01 bash -c "cqlsh -u cassandra -p cassandra"
#In cqlsh:
USE investments;
SOURCE '/root/data.cql'

```

### IF HAS ALREADY BEEN USED
```
# Run the app.py    
python3 app.py
```



