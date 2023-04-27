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
                session.run("MERGE (f:Flight {flight_ID: $flight_ID, airline: $airline})", 
                            flight_ID=flight_ID, airline=airline)
            except ConstraintError:
                pass


    def _create_airport_node(self, airport_name):
        with self.driver.session() as session:
            try:
                session.run("MERGE (a:Airport {airport_name: $airport_name})", 
                            airport_name=airport_name)
            except ConstraintError:
                pass


    def _create_date_node(self, year, month, day):
        with self.driver.session() as session:
            try:
                session.run("MERGE (d:Date {year: $year, month: $month, day: $day})", 
                            year=year, month=month, day=day)
            except ConstraintError:
                pass


    def _create_passenger_node(self, passenger_ID, age, gender, reason, stay, transit):
        with self.driver.session() as session:
            try:
                session.run("MERGE (p:Passenger {passenger_ID: $passenger_ID, age: $age, gender: $gender, reason: $reason, stay:$stay, transit:$transit})", 
                            passenger_ID=passenger_ID, age=age, gender=gender, reason=reason, stay=stay, transit=transit)
            except ConstraintError:
                pass


    def _create_FROM_relationship(self, flight, airport):
        with self.driver.session() as session:
            session.run("""
                MATCH (f:Flight {flight_ID: $flight_ID}), (a:Airport {airport_name: $airport_name})
                MERGE (f)-[r:FROM]->(a)
                RETURN type(r)""", flight_ID=flight, airport_name=airport)


    def _create_TO_relationship(self, flight, airport):
        with self.driver.session() as session:
            session.run("""
                MATCH (f:Flight {flight_ID: $flight_ID}), (a:Airport {airport_name: $airport_name})
                MERGE (f)-[r:TO]->(a)
                RETURN type(r)""", flight_ID=flight, airport_name=airport)
            

    def _create_ON_DATE_relationship(self, flight, year, month, day):
        with self.driver.session() as session:
            session.run("""
                MATCH (f:Flight {flight_ID: $flight_ID}), (d:Date {year: $year, month: $month, day: $day})
                MERGE (f)-[r:ON_DATE]->(d)
                RETURN type(r)""", flight_ID=flight, year=year, month=month, day=day)


    def _create_BOARDED_relationship(self, passenger, flight):
        with self.driver.session() as session:
            session.run("""
                MATCH (p:Passenger{passenger_ID:$passenger_ID}), (f:Flight{flight_ID:$flight_ID})
                MERGE (p)-[r:BOARDED]->(f)
                RETURN type(r)""", passenger_ID=passenger, flight_ID=flight)
            

    def init(self, source):
            with open(source, newline='') as csv_file:
                reader = csv.DictReader(csv_file,  delimiter=',')
                for r in reader:
                    self._create_airport_node(r["from"])
                    self._create_flight_node(r["id"], r["airline"])
                    self._create_date_node(r["year"], r["month"], r["day"])
                    self._create_passenger_node(r["passenger_id"], r["age"], r["gender"], r["reason"], r["stay"], r["transit"])
                    self._create_FROM_relationship(r["id"], r["from"])
                    self._create_TO_relationship(r["id"], r["to"])
                    self._create_ON_DATE_relationship(r["id"], r["year"], r["month"], r["day"])
                    self._create_BOARDED_relationship(r["passenger_id"], r["id"]) 

if __name__ == "__main__":
    data = "data/flight_passengers.csv"
    # Read connection env variables
    neo4j_uri = os.getenv('NEO4J_URI', 'bolt://localhost:7687')
    neo4j_user = os.getenv('NEO4J_USER', 'neo4j')
    neo4j_password = os.getenv('NEO4J_PASSWORD', 'formal-pizza-trident-october-donor-9379')

    airlines = AirlinesApp(neo4j_uri, neo4j_user, neo4j_password)
    airlines.init(data)

    airlines.close()

