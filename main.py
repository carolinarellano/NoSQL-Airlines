import csv
import os
import datetime as date

from neo4j import GraphDatabase
from neo4j.exceptions import ConstraintError

class AirlinesApp(object):

    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))
        self._create_constraints()


    def close(self):
        self.driver.close()
  

    def _create_constraints(self):
        with self.driver.session() as session:
            #Define the CONSTRAINTS for the airlines data
            session.run("CREATE CONSTRAINT unique_flight IF NOT EXISTS FOR (f:Flight) REQUIRE f.flight_ID IS UNIQUE")
            session.run("CREATE CONSTRAINT unique_airport IF NOT EXISTS FOR (a:Airport) REQUIRE a.airport_name IS UNIQUE")
            session.run("CREATE CONSTRAINT unique_date IF NOT EXISTS FOR (d:Date) REQUIRE (d.day, d.month, d.year, d.hour) IS UNIQUE")
            session.run("CREATE CONSTRAINT unique_passenger IF NOT EXISTS FOR (p:Passenger) REQUIRE p.passenger_ID IS UNIQUE")
            
    """
    Flight -> airline, flight_ID
    Airport -> airport_name
    Date -> day, month, year, hour
    Passenger -> age, gender, reason, stay, transit, passenger_ID
    
    """  

    def _create_flight_node(self, flight_ID, airline):
        with self.driver.session() as session:
            try:
                session.run("CREATE (f:Flight {flight_ID: $flight_ID, airline: $airline})", 
                            flight_ID=flight_ID, airline=airline)
            except ConstraintError:
                pass


    def _create_airport_node(self, airport_name):
        with self.driver.session() as session:
            try:
                session.run("CREATE (a:Airport {airport_name: $airport_name})", 
                            airport_name=airport_name)
            except ConstraintError:
                pass


    def _create_date_node(self, year, month, day, hours, minutes, seconds):
        hour_formatter = date.datetime(year, month, day, hours, minutes, seconds)
        with self.driver.session() as session:
            try:
                session.run("CREATE (d:Date {year: $year, month: $month, day: $day, hour: $hour})", 
                            year=year, month=month, day=day, hour=hour_formatter)
            except ConstraintError:
                pass


    def _create_passenger_node(self, passenger_ID, age, gender, reason, stay, transit):
        with self.driver.session() as session:
            try:
                session.run("CREATE (p:Passenger {passenger_ID: $passenger_ID, age: $age, gender: $gender, reason: $reason, stay:$stay, transit:$transit})", 
                            passenger_ID=passenger_ID, age=age, gender=gender, reason=reason, stay=stay, transit=transit)
            except ConstraintError:
                pass


    def _create_FROM_relationship(self, flight, airport):
        with self.driver.session() as session:
            session.run("""
                MATCH (f:Flight {flight_ID: $flight_ID}), (a:Airport {airport_name: $airport_name})
                MERGE (f)-[r:FROM]->(a)
                RETURN type(r)""", fligh_ID=flight, airport_name=airport)


    def _create_TO_relationship(self, flight, airport):
        with self.driver.session() as session:
            session.run("""
                MATCH (f:Flight {flight_ID: $flight_ID}), (a:Airport {airport_name: $airport_name})
                MERGE (f)-[r:TO]->(a)
                RETURN type(r)""", fligh_ID=flight, airport_name=airport)
            

    def _create_user_to_artist_relationship(self, username, artist):
        with self.driver.session() as session:
            session.run("""
                MATCH (u:User {username: $username}), (a:Artist {artist_name: $artist_name})
                MERGE (u)-[:FOLLOWS_ARTIST]->(a)
                RETURN u, a""", username=username, artist_name=artist)


    def _create_song_to_album_relationship(self, album):
        with self.driver.session() as session:
            session.run("""
                MATCH (s:Song), (al:Album)
                WHERE s.album_name=$album_name AND al.album_name=$album_name
                MERGE (s)-[r:FROM_ALBUM]->(al)
                RETURN type(r)""", album_name=album)
            

    def init(self, source):
            with open(source, newline='') as csv_file:
                reader = csv.DictReader(csv_file,  delimiter='|')
                for r in reader:
                    pass
                        
                        

if __name__ == "__main__":
    data = 'data/airlines.csv'
    # Read connection env variables
    neo4j_uri = os.getenv('NEO4J_URI', 'bolt://localhost:7687')
    neo4j_user = os.getenv('NEO4J_USER', 'neo4j')
    neo4j_password = os.getenv('NEO4J_PASSWORD', '')

    airlines = AirlinesApp(neo4j_uri, neo4j_user, neo4j_password)
    airlines.init(data)

    airlines.close()

