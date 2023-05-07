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
            session.run("CREATE CONSTRAINT unique_flight IF NOT EXISTS FOR (f:Flight) REQUIRE (f.airline, f.year, f.month, f.day) IS UNIQUE")
            session.run("CREATE CONSTRAINT unique_airport IF NOT EXISTS FOR (a:Airport) REQUIRE a.airport_name IS UNIQUE")
            session.run("CREATE CONSTRAINT unique_date IF NOT EXISTS FOR (d:Date) REQUIRE (d.day, d.month, d.year, d.hour) IS UNIQUE")
            session.run("CREATE CONSTRAINT unique_passenger IF NOT EXISTS FOR (p:Passenger) REQUIRE (p.age, p.gender, p.reason, p.stay, p.transit) IS UNIQUE")
            session.run("CREATE CONSTRAINT unique_airline IF NOT EXISTS FOR (ai:Airline) REQUIRE ai.airline_name IS UNIQUE")
            
    """
    Flight -> airline
    Airport -> airport_name
    Date -> day, month, year, hour
    Passenger -> age, gender, reason, stay, transit, wait
    
    """  
    def _create_airline_node(self, airline_name):
        with self.driver.session() as session:
            try:
                session.run("MERGE (ai:Airline {airline_name: $airline_name})", 
                            airline_name=airline_name)
            except ConstraintError:
                pass


    def _create_airport_node(self, airport_name):
        with self.driver.session() as session:
            try:
                session.run("MERGE (a:Airport {airport_name: $airport_name})", 
                            airport_name=airport_name)
            except ConstraintError:
                pass


    def _create_flight_node(self, airline, year, month, day):
        with self.driver.session() as session:
            try:
                session.run("MERGE (f:Flight {airline: $airline, date: date($year + '-' + $month + '-' + $day)})",
                            airline=airline, year=year, month=month, day=day)
            except ConstraintError:
                pass


    def _create_date_node(self, year, month, day):
        with self.driver.session() as session:
            try:
                session.run("MERGE (d:Date {year: $year, month: $month, day: $day})", 
                            year=year, month=month, day=day)
            except ConstraintError:
                pass


    def _create_passenger_node(self, age, gender, reason, stay, transit, wait):
        with self.driver.session() as session:
            try:
                session.run("MERGE (p:Passenger {age: $age, gender: $gender, reason: $reason, stay:$stay, transit:$transit, wait:$wait})", 
                            age=age, gender=gender, reason=reason, stay=stay, transit=transit, wait=wait)
            except ConstraintError:
                pass


    def _create_FROM_relationship(self, year, month, day, airport, airline):
        with self.driver.session() as session:
            session.run("""
                MATCH (f:Flight {date:date($year + '-' + $month + '-' + $day), airline:$airline})
                MATCH (a:Airport {airport_name:$airport_name})
                MERGE (f)-[r:FROM]->(a)
                RETURN type(r)""", year=year, month=month, day=day, airport_name=airport, airline=airline)


    def _create_TO_relationship(self, year, month, day, airport, airline):
        with self.driver.session() as session:
            session.run("""
                MATCH (f:Flight {date:date($year + '-' + $month + '-' + $day), airline:$airline})
                MATCH (a:Airport {airport_name:$airport_name})
                MERGE (f)-[r:TO]->(a)
                RETURN type(r)""", year=year, month=month, day=day, airport_name=airport, airline=airline)
            

    def _create_ON_DATE_relationship(self, year, month, day):
        with self.driver.session() as session:
            session.run("""
                MATCH (f:Flight {date:date($year + '-' + $month + '-' + $day)})
                MERGE (d:Date {year:$year, month:$month, day:$day})
                MERGE (f)-[r:ON_DATE]->(d)
                RETURN type(r)""", year=year, month=month, day=day)


    def _create_BOARDED_relationship(self, age, gender, reason, stay, transit, year, month, day):
        with self.driver.session() as session:
            session.run("""
                MATCH (f:Flight {date:date($year + '-' + $month + '-' + $day)})
                MERGE (p:Passenger {age:$age, gender:$gender, reason:$reason, stay:$stay, transit:$transit})
                MERGE (f)-[r:BOARDED]->(p)
                RETURN type(r)""", age=age, gender=gender, reason=reason, stay=stay, transit=transit, year=year, month=month, day=day)
            

    def _create_OPERATE_AT_relationship(self, airline, airport):
        with self.driver.session() as session:
            session.run("""
                MATCH (a:Airline {airline_name:$airline_name})
                MATCH (ap:Airport {airport_name:$airport_name})
                MERGE (a)-[r:OPERATES_AT]->(ap)
                RETURN type(r)
            """, airline_name=airline, airport_name=airport)


    def _wait_count(self, age, wait, airport, airports):
        with self.driver.session() as session:
            result = session.run(
                """
                MATCH (p:Passenger)
                WHERE p.age=$age AND p.wait = $wait
                RETURN p.age, p.wait, COUNT(p) AS count
                ORDER BY p.age, p.wait
                """, age=age, wait=wait
            )
            for record in result:
                if int(record['p.wait']) > 0:
                    print(f"{record['p.age']}  {record['p.wait']} {record['count']} {airport}")
                    if airport in airports:
                        airports[airport] += int(record['count'])
                    else:
                        airports[airport] = int(record['count'])
            
                

    def init(self, source):
            airports = {}
            with open(source, newline='') as csv_file:
                reader = csv.DictReader(csv_file,  delimiter=',')
                for r in reader:
                    self._create_airline_node(r["airline"])
                    self._create_airport_node(r["from"])
                    self._create_flight_node(r["airline"], r["year"], r["month"], r["day"])
                    self._create_date_node(r["year"], r["month"], r["day"])
                    self._create_passenger_node(r["age"], r["gender"], r["reason"], r["stay"], r["transit"], r["wait"])
                    self._create_OPERATE_AT_relationship(r["airline"], r["from"])
                    self._create_FROM_relationship(r["year"], r["month"], r["day"], r["from"], r["airline"])
                    self._create_TO_relationship(r["year"], r["month"], r["day"], r["to"], r["airline"])
                    self._create_ON_DATE_relationship(r["year"], r["month"], r["day"])
                    self._create_BOARDED_relationship(r["age"], r["gender"], r["reason"], r["stay"], r["transit"], r["year"], r["month"], r["day"])
                    self._create_OPERATE_AT_relationship(r["airline"], r["to"])
                    self._wait_count(r["age"], r["wait"], r["from"], airports)
            print(f"Airport frequency: {airports}")



if __name__ == "__main__":
    data = "data/flight_passengers.csv"
    # Read connection env variables
    neo4j_uri = os.getenv('NEO4J_URI', 'bolt://localhost:7687')
    neo4j_user = os.getenv('NEO4J_USER', 'neo4j')
    neo4j_password = os.getenv('NEO4J_PASSWORD', 'formal-pizza-trident-october-donor-9379')

    airlines = AirlinesApp(neo4j_uri, neo4j_user, neo4j_password)
    airlines.init(data)

    airlines.close()

