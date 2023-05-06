#!/usr/bin/env python3
import logging
from tabulate import tabulate
from colorama import init, Fore, Style
init()

# Set logger
log = logging.getLogger()


CREATE_KEYSPACE = """
        CREATE KEYSPACE IF NOT EXISTS {}
        WITH replication = {{ 'class': 'SimpleStrategy', 'replication_factor': {} }}
"""

CREATE_PRINCIPAL_TABLE = '''
CREATE TABLE IF NOT EXISTS airport_wait_time (
  airline text,
  de text,
  hacia text,
  day int,
  month int,
  year int,
  age int,
  gender text,
  reason text,
  stay text,
  transit text,
  connection text,
  wait int,
  PRIMARY KEY ((airline, de, hacia, year, month, day), age, gender, reason, stay, transit, connection)
) WITH CLUSTERING ORDER BY (age DESC, gender DESC, reason DESC, stay DESC, transit DESC, connection DESC);
'''
CREATE_INDEXES = '''
    CREATE INDEX IF NOT EXISTS airport_wait_time_wait_idx ON airport_wait_time (wait);
    '''

CREATE_INDEXES2 = '''
    CREATE INDEX IF NOT EXISTS year_index ON airport_wait_time (year);
    '''



QUANTITY_ALL = '''
    SELECT COUNT(*) AS total_count FROM airport_wait_time;
    '''

SELECT_ALL = '''
SELECT * FROM airport_wait_time;
'''



SELECT_BY_AIRLINE = '''
    SELECT * FROM airport_wait_time WHERE airline = ? ALLOW FILTERING;
    '''

SELECT_BY_AIRLINE_WAIT = '''
    SELECT * FROM airport_wait_time WHERE airline = ? AND wait = ? ALLOW FILTERING;
    '''

SELECT_BY_WAIT_LESS_0 = '''
    SELECT * FROM airport_wait_time WHERE wait <= ? ALLOW FILTERING;
    '''
SELECT_BY_WAIT_MORE_0 = '''
    SELECT * FROM airport_wait_time WHERE wait >= ? ALLOW FILTERING;
    '''

SELECT_BY_MONTH_YEAR = '''
    SELECT * FROM airport_wait_time WHERE month = ? AND year = ? ALLOW FILTERING;
    '''

SELECT_BY_FROM_TO = '''
    SELECT * FROM airport_wait_time WHERE de = ? AND hacia = ?  ALLOW FILTERING;
    '''

SELECT_BY_FROM_TO_WAIT = '''
    SELECT * FROM airport_wait_time WHERE de = ? AND hacia = ? AND wait = ? ALLOW FILTERING;
    '''

SELECT_BY_STAY_CONNECTION = '''
    SELECT * FROM airport_wait_time WHERE stay = ? AND connection = ? ALLOW FILTERING;
    '''


SELECT_BY_AIRLINE_FROM = '''
    SELECT * FROM airport_wait_time WHERE airline = ? AND de = ? ALLOW FILTERING;
    '''

SELECT_BY_TRANSIT_WAIT = '''
    SELECT * FROM airport_wait_time WHERE transit = ? AND wait >= ? ALLOW FILTERING;
    '''

SELECT_BY_FROM_TO_MONTH = '''
    SELECT * FROM airport_wait_time WHERE de = ? AND hacia = ? AND month = ? ALLOW FILTERING;   
    '''

#selects especiales----------------------------------

#UN DIA MES AÑO ESPECIFICO
SELECT_BY_PERCENTAJE_DATE = '''
    SELECT COUNT(*) AS filtered_count FROM airport_wait_time WHERE day = ? AND month = ? AND year = ? ALLOW FILTERING;
'''

#VUELOS EN UN DIA ESPECIFICO
SELECT_BY_PERCENTAJE_DAY = '''
    SELECT COUNT(*) AS filtered_count FROM airport_wait_time WHERE day = ? ALLOW FILTERING;
    '''

#VUELOS EN DETERMINADO TIEMPO (LEES AND MORE)
SELECT_BY_PERCENTAJE__LESS_WAIT = '''
    SELECT COUNT(*) AS filtered_count FROM airport_wait_time WHERE wait <= ? ALLOW FILTERING;
    '''
SELECT_BY_PERCENTAJE__MORE_WAIT = '''
    SELECT COUNT(*) AS filtered_count FROM airport_wait_time WHERE wait >= ? ALLOW FILTERING;
    '''

#VUELOS QUE VAN A UN AEROPUERTO Y UN (DIA, MES, AÑO) ESPECIFICO
SELECT_BY_PERCENTAJE_TO_DAY = '''
    SELECT COUNT(*) AS filtered_count FROM airport_wait_time WHERE de = ? AND day = ? ALLOW FILTERING;
    '''

SELECT_BY_PERCENTAJE_TO_MONTH = '''
    SELECT COUNT(*) AS filtered_count FROM airport_wait_time WHERE de = ? AND month = ? ALLOW FILTERING;
    '''

SELECT_BY_PERCENTAJE_TO_YEAR = '''
    SELECT COUNT(*) AS filtered_count FROM airport_wait_time WHERE de = ? AND year = ? ALLOW FILTERING;
    '''

# VUELOS POR CONEXION
SELECT_BY_PERCENTAJE_CONNECTION_TRUE = '''
    SELECT COUNT(*) AS filtered_count FROM airport_wait_time WHERE connection = 'True' ALLOW FILTERING;
    '''

SELECT_BY_PERCENTAJE_CONNECTION_FALSE = '''
    SELECT COUNT(*) AS filtered_count FROM airport_wait_time WHERE connection = 'False' ALLOW FILTERING;
    '''

# VUELOS DE UNA AEROLINEA EN UNA FECHA ESPECIFICA
SELECT_BY_PERCENTAJE__AIRLINE_DATE = '''
    SELECT COUNT(*) AS filtered_count FROM airport_wait_time WHERE airline = ? AND day = ? AND month = ? AND year = ? ALLOW FILTERING;
    '''


def create_keyspace(session, keyspace, replication_factor):
    log.info(f"Creating keyspace: {keyspace} with replication factor {replication_factor}")
    session.execute(CREATE_KEYSPACE.format(keyspace, replication_factor))


def create_schema(session):
    log.info("Creating model schema")
    session.execute(CREATE_PRINCIPAL_TABLE)
    session.execute(CREATE_INDEXES)


def select_all(session):
    log.info("Retrieving all airport wait times")
    stmt = session.prepare(SELECT_ALL)
    rows = session.execute(stmt)
    print(f"=== All airport flight")
    print("\n")

    data = []
    for row in rows:
        airline = f"{Style.BRIGHT}{Fore.GREEN}{row.airline}{Style.RESET_ALL}"
        day = f"{Style.BRIGHT}{Fore.BLUE}{row.day}{Style.RESET_ALL}"
        month = f"{Style.BRIGHT}{Fore.BLUE}{row.month}{Style.RESET_ALL}"
        year = f"{Style.BRIGHT}{Fore.BLUE}{row.year}{Style.RESET_ALL}"
        wait = f"{Style.BRIGHT}{Fore.RED}{row.wait}{Style.RESET_ALL}"

        data.append([airline, row.de, row.hacia, day, month, year, row.age, row.reason, wait])

    headers = ["Airline", "From", "To", "Day", "Month", "Year", "Age", "Reason", "Wait"]
    print(tabulate(data, headers=headers, tablefmt="fancy_grid"))

def select_by_airline(session, airline):
    log.info(f"Retrieving airport wait times for airline {airline}")
    stmt = session.prepare(SELECT_BY_AIRLINE)
    rows = session.execute(stmt, [airline])
    print(f"=== Airport wait times for airline {airline}")
    print("\n")
    data = []
    for row in rows:
        airline = f"{Style.BRIGHT}{Fore.GREEN}{row.airline}{Style.RESET_ALL}"
        day = f"{Style.BRIGHT}{Fore.BLUE}{row.day}{Style.RESET_ALL}"
        month = f"{Style.BRIGHT}{Fore.BLUE}{row.month}{Style.RESET_ALL}"
        year = f"{Style.BRIGHT}{Fore.BLUE}{row.year}{Style.RESET_ALL}"
        wait = f"{Style.BRIGHT}{Fore.RED}{row.wait}{Style.RESET_ALL}"

        data.append([airline, row.de, row.hacia, day, month, year, row.age, row.reason, wait])

    headers = ["Airline", "From", "To", "Day", "Month", "Year", "Age", "Reason", "Wait"]
    print(tabulate(data, headers=headers, tablefmt="fancy_grid"))

def select_by_airline_wait(session, airline, wait_time):
    log.info(f"Retrieving airport wait times for airline {airline} and wait time {wait_time}")
    stmt = session.prepare(SELECT_BY_AIRLINE_WAIT)
    rows = session.execute(stmt, [airline, wait_time])
    print(f"=== Airport wait times for airline {airline} and wait time {wait_time}")
    print("\n")
    data = []
    for row in rows:
        airline = f"{Style.BRIGHT}{Fore.GREEN}{row.airline}{Style.RESET_ALL}"
        day = f"{Style.BRIGHT}{Fore.BLUE}{row.day}{Style.RESET_ALL}"
        month = f"{Style.BRIGHT}{Fore.BLUE}{row.month}{Style.RESET_ALL}"
        year = f"{Style.BRIGHT}{Fore.BLUE}{row.year}{Style.RESET_ALL}"
        wait = f"{Style.BRIGHT}{Fore.RED}{row.wait}{Style.RESET_ALL}"

        data.append([airline, row.de, row.hacia, day, month, year, row.age, row.reason, wait])

    headers = ["Airline", "From", "To", "Day", "Month", "Year", "Age", "Reason", "Wait"]
    print(tabulate(data, headers=headers, tablefmt="fancy_grid"))

def select_by_wait_less_0(session, wait_time):
    log.info(f"Retrieving airport wait times with wait time less than or equal to {wait_time}")
    stmt = session.prepare(SELECT_BY_WAIT_LESS_0)
    rows = session.execute(stmt, [wait_time])
    print(f"=== Airport wait times with wait time less than or equal to {wait_time}")
    print("\n")
    data = []
    for row in rows:
        airline = f"{Style.BRIGHT}{Fore.GREEN}{row.airline}{Style.RESET_ALL}"
        day = f"{Style.BRIGHT}{Fore.BLUE}{row.day}{Style.RESET_ALL}"
        month = f"{Style.BRIGHT}{Fore.BLUE}{row.month}{Style.RESET_ALL}"
        year = f"{Style.BRIGHT}{Fore.BLUE}{row.year}{Style.RESET_ALL}"
        wait = f"{Style.BRIGHT}{Fore.RED}{row.wait}{Style.RESET_ALL}"

        data.append([airline, row.de, row.hacia, day, month, year, row.age, row.reason, wait])

    headers = ["Airline", "From", "To", "Day", "Month", "Year", "Age", "Reason", "Wait"]
    print(tabulate(data, headers=headers, tablefmt="fancy_grid"))

def select_by_wait_more_0(session, wait_time):
    log.info(f"Retrieving airport wait times with wait time greater than or equal to {wait_time}")
    stmt = session.prepare(SELECT_BY_WAIT_MORE_0)
    rows = session.execute(stmt, [wait_time])
    print(f"=== Airport wait times with wait time greater than or equal to {wait_time}")
    print("\n")
    data = []
    for row in rows:
        airline = f"{Style.BRIGHT}{Fore.GREEN}{row.airline}{Style.RESET_ALL}"
        day = f"{Style.BRIGHT}{Fore.BLUE}{row.day}{Style.RESET_ALL}"
        month = f"{Style.BRIGHT}{Fore.BLUE}{row.month}{Style.RESET_ALL}"
        year = f"{Style.BRIGHT}{Fore.BLUE}{row.year}{Style.RESET_ALL}"
        wait = f"{Style.BRIGHT}{Fore.RED}{row.wait}{Style.RESET_ALL}"

        data.append([airline, row.de, row.hacia, day, month, year, row.age, row.reason, wait])

    headers = ["Airline", "From", "To", "Day", "Month", "Year", "Age", "Reason", "Wait"]
    print(tabulate(data, headers=headers, tablefmt="fancy_grid"))

def select_by_month_year(session, month, year):
    log.info(f"Retrieving airport wait times for month {month} and year {year}")
    stmt = session.prepare(SELECT_BY_MONTH_YEAR)
    rows = session.execute(stmt, [month, year])
    print(f"=== Airport wait times for month {month} and year {year}")
    print("\n")
    data = []
    for row in rows:
        airline = f"{Style.BRIGHT}{Fore.GREEN}{row.airline}{Style.RESET_ALL}"
        day = f"{Style.BRIGHT}{Fore.BLUE}{row.day}{Style.RESET_ALL}"
        month = f"{Style.BRIGHT}{Fore.BLUE}{row.month}{Style.RESET_ALL}"
        year = f"{Style.BRIGHT}{Fore.BLUE}{row.year}{Style.RESET_ALL}"
        wait = f"{Style.BRIGHT}{Fore.RED}{row.wait}{Style.RESET_ALL}"

        data.append([airline, row.de, row.hacia, day, month, year, row.age, row.reason, wait])

    headers = ["Airline", "From", "To", "Day", "Month", "Year", "Age", "Reason", "Wait"]
    print(tabulate(data, headers=headers, tablefmt="fancy_grid"))

def select_by_from_to(session, origin, destination):
    log.info(f"Retrieving airport wait times for origin {origin} and destination {destination}")
    stmt = session.prepare(SELECT_BY_FROM_TO)
    rows = session.execute(stmt, [origin, destination])
    print(f"=== Airport wait times for origin {origin} and destination {destination}")
    print("\n")
    data = []
    for row in rows:
        airline = f"{Style.BRIGHT}{Fore.GREEN}{row.airline}{Style.RESET_ALL}"
        day = f"{Style.BRIGHT}{Fore.BLUE}{row.day}{Style.RESET_ALL}"
        month = f"{Style.BRIGHT}{Fore.BLUE}{row.month}{Style.RESET_ALL}"
        year = f"{Style.BRIGHT}{Fore.BLUE}{row.year}{Style.RESET_ALL}"
        wait = f"{Style.BRIGHT}{Fore.RED}{row.wait}{Style.RESET_ALL}"

        data.append([airline, row.de, row.hacia, day, month, year, row.age, row.reason, wait])

    headers = ["Airline", "From", "To", "Day", "Month", "Year", "Age", "Reason", "Wait"]
    print(tabulate(data, headers=headers, tablefmt="fancy_grid"))

def select_by_from_to_wait(session, origin, destination, wait_time):
    log.info(f"Retrieving airport wait times for origin {origin}, destination {destination} and wait time {wait_time}")
    stmt = session.prepare(SELECT_BY_FROM_TO_WAIT)
    rows = session.execute(stmt, [origin, destination, wait_time])
    print(f"=== Airport wait times for origin {origin}, destination {destination} and wait time {wait_time}")
    print("\n")
    data = []
    for row in rows:
        airline = f"{Style.BRIGHT}{Fore.GREEN}{row.airline}{Style.RESET_ALL}"
        day = f"{Style.BRIGHT}{Fore.BLUE}{row.day}{Style.RESET_ALL}"
        month = f"{Style.BRIGHT}{Fore.BLUE}{row.month}{Style.RESET_ALL}"
        year = f"{Style.BRIGHT}{Fore.BLUE}{row.year}{Style.RESET_ALL}"
        wait = f"{Style.BRIGHT}{Fore.RED}{row.wait}{Style.RESET_ALL}"

        data.append([airline, row.de, row.hacia, day, month, year, row.age, row.reason, wait])

    headers = ["Airline", "From", "To", "Day", "Month", "Year", "Age", "Reason", "Wait"]
    print(tabulate(data, headers=headers, tablefmt="fancy_grid"))

def select_by_stay_connection(session, stay, connection):
    log.info(f"Retrieving airport wait times for stay {stay} and connection {connection}")
    stmt = session.prepare(SELECT_BY_STAY_CONNECTION)
    rows = session.execute(stmt, [stay, connection])
    print(f"=== Airport wait times for stay {stay} and connection {connection}")
    print("\n")
    data = []
    for row in rows:
        airline = f"{Style.BRIGHT}{Fore.GREEN}{row.airline}{Style.RESET_ALL}"
        day = f"{Style.BRIGHT}{Fore.BLUE}{row.day}{Style.RESET_ALL}"
        month = f"{Style.BRIGHT}{Fore.BLUE}{row.month}{Style.RESET_ALL}"
        year = f"{Style.BRIGHT}{Fore.BLUE}{row.year}{Style.RESET_ALL}"
        wait = f"{Style.BRIGHT}{Fore.RED}{row.wait}{Style.RESET_ALL}"

        data.append([airline, row.de, row.hacia, day, month, year, row.age, row.reason, wait])

    headers = ["Airline", "From", "To", "Day", "Month", "Year", "Age", "Reason", "Wait"]
    print(tabulate(data, headers=headers, tablefmt="fancy_grid"))

def select_by_airline_from(session, airline, origin):
    log.info(f"Retrieving airport wait times for airline {airline} and origin {origin}")
    stmt = session.prepare(SELECT_BY_AIRLINE_FROM)
    rows = session.execute(stmt, [airline, origin])
    print(f"=== Airport wait times for airline {airline} and origin {origin}")
    print("\n")
    data = []
    for row in rows:
        airline = f"{Style.BRIGHT}{Fore.GREEN}{row.airline}{Style.RESET_ALL}"
        day = f"{Style.BRIGHT}{Fore.BLUE}{row.day}{Style.RESET_ALL}"
        month = f"{Style.BRIGHT}{Fore.BLUE}{row.month}{Style.RESET_ALL}"
        year = f"{Style.BRIGHT}{Fore.BLUE}{row.year}{Style.RESET_ALL}"
        wait = f"{Style.BRIGHT}{Fore.RED}{row.wait}{Style.RESET_ALL}"

        data.append([airline, row.de, row.hacia, day, month, year, row.age, row.reason, wait])

    headers = ["Airline", "From", "To", "Day", "Month", "Year", "Age", "Reason", "Wait"]
    print(tabulate(data, headers=headers, tablefmt="fancy_grid"))

def select_by_transit_wait(session, transit, wait_time):
    log.info(f"Retrieving airport wait times for transit {transit} and wait time greater than {wait_time}")
    stmt = session.prepare(SELECT_BY_TRANSIT_WAIT)
    rows = session.execute(stmt, [transit, wait_time])
    print(f"=== Airport wait times for transit {transit} and wait time greater than {wait_time}")
    print("\n")
    data = []
    for row in rows:
        airline = f"{Style.BRIGHT}{Fore.GREEN}{row.airline}{Style.RESET_ALL}"
        day = f"{Style.BRIGHT}{Fore.BLUE}{row.day}{Style.RESET_ALL}"
        month = f"{Style.BRIGHT}{Fore.BLUE}{row.month}{Style.RESET_ALL}"
        year = f"{Style.BRIGHT}{Fore.BLUE}{row.year}{Style.RESET_ALL}"
        wait = f"{Style.BRIGHT}{Fore.RED}{row.wait}{Style.RESET_ALL}"

        data.append([airline, row.de, row.hacia, day, month, year, row.age, row.reason, wait])

    headers = ["Airline", "From", "To", "Day", "Month", "Year", "Age", "Reason", "Wait"]
    print(tabulate(data, headers=headers, tablefmt="fancy_grid"))

def select_by_from_to_month(session, origin, destination, month):
    log.info(f"Retrieving airport wait time for flights from {origin} to {destination} in {month}")
    stmt = session.prepare(SELECT_BY_FROM_TO_MONTH)
    rows = session.execute(stmt, [origin, destination, month])
    print(f"=== Airport wait time for flights from {origin} to {destination} in {month}")
    print("\n")
    data = []
    for row in rows:
        airline = f"{Style.BRIGHT}{Fore.GREEN}{row.airline}{Style.RESET_ALL}"
        day = f"{Style.BRIGHT}{Fore.BLUE}{row.day}{Style.RESET_ALL}"
        month = f"{Style.BRIGHT}{Fore.BLUE}{row.month}{Style.RESET_ALL}"
        year = f"{Style.BRIGHT}{Fore.BLUE}{row.year}{Style.RESET_ALL}"
        wait = f"{Style.BRIGHT}{Fore.RED}{row.wait}{Style.RESET_ALL}"

        data.append([airline, row.de, row.hacia, day, month, year, row.age, row.reason, wait])

    headers = ["Airline", "From", "To", "Day", "Month", "Year", "Age", "Reason", "Wait"]
    print(tabulate(data, headers=headers, tablefmt="fancy_grid"))

#Funciones especiales
flights_formatted = f"{Style.BRIGHT}{Fore.YELLOW}Flights{Style.RESET_ALL}"
percentage_formatted = f"{Style.BRIGHT}{Fore.YELLOW}Percentage{Style.RESET_ALL}"

def select_by_percentaje_date(session, day, month, year):
    log.info(f"Retrieving percentage by date: {day}/{month}/{year}")
    total_f = session.prepare(QUANTITY_ALL)
    stmt = session.prepare(SELECT_BY_PERCENTAJE_DATE)
    total_r = session.execute(total_f)
    rows = session.execute(stmt, [day, month, year])
    filtered_count = rows.one().filtered_count
    total_count = total_r.one().total_count
    percentage = (filtered_count / total_count) * 100
    print(f"Percentage by date: {day}/{month}/{year}")
    print("\n")
    filtered_count_formatted = f"{Fore.WHITE}{Style.BRIGHT}{filtered_count}{Style.RESET_ALL}"
    percentage_value_formatted = f"{Fore.WHITE}{Style.BRIGHT}{percentage:.2f}%{Style.RESET_ALL}"
    print(tabulate([[flights_formatted, filtered_count_formatted], [percentage_formatted, percentage_value_formatted]],tablefmt='fancy_grid'))

def select_by_percentaje_day(session, day):
    log.info(f"Retrieving percentage by day: {day}")
    total_f = session.prepare(QUANTITY_ALL)
    stmt = session.prepare(SELECT_BY_PERCENTAJE_DAY)
    total_r = session.execute(total_f)
    rows = session.execute(stmt, [day])
    filtered_count = rows.one().filtered_count
    total_count = total_r.one().total_count
    percentage = (filtered_count / total_count) * 100
    print(f"Percentage by day: {day}")
    print("\n")
    filtered_count_formatted = f"{Fore.WHITE}{Style.BRIGHT}{filtered_count}{Style.RESET_ALL}"
    percentage_value_formatted = f"{Fore.WHITE}{Style.BRIGHT}{percentage:.2f}%{Style.RESET_ALL}"
    print(tabulate([[flights_formatted, filtered_count_formatted], [percentage_formatted, percentage_value_formatted]],tablefmt='fancy_grid'))

def select_by_percentaje_less_wait(session, wait):
    log.info(f"Retrieving percentage by wait time less or equal to {wait} minutes")
    total_f = session.prepare(QUANTITY_ALL)
    stmt = session.prepare(SELECT_BY_PERCENTAJE__LESS_WAIT)
    rows = session.execute(stmt, [wait])
    total_r = session.execute(total_f)
    filtered_count = rows.one().filtered_count
    total_count = total_r.one().total_count
    percentage = (filtered_count / total_count) * 100
    print(f"Percentage by wait time less or equal to {wait} minutes")
    print("\n")
    filtered_count_formatted = f"{Fore.WHITE}{Style.BRIGHT}{filtered_count}{Style.RESET_ALL}"
    percentage_value_formatted = f"{Fore.WHITE}{Style.BRIGHT}{percentage:.2f}%{Style.RESET_ALL}"
    print(tabulate([[flights_formatted, filtered_count_formatted], [percentage_formatted, percentage_value_formatted]],tablefmt='fancy_grid'))

def select_by_percentaje_more_wait(session, wait):
    log.info(f"Retrieving percentage by wait time more or equal to {wait} minutes")
    total_f = session.prepare(QUANTITY_ALL)
    stmt = session.prepare(SELECT_BY_PERCENTAJE__MORE_WAIT)
    rows = session.execute(stmt, [wait])
    total_r = session.execute(total_f)
    filtered_count = rows.one().filtered_count
    total_count = total_r.one().total_count
    percentage = (filtered_count / total_count) * 100
    print(f"Percentage by wait time more or equal to {wait} minutes")
    print("\n")
    filtered_count_formatted = f"{Fore.WHITE}{Style.BRIGHT}{filtered_count}{Style.RESET_ALL}"
    percentage_value_formatted = f"{Fore.WHITE}{Style.BRIGHT}{percentage:.2f}%{Style.RESET_ALL}"
    print(tabulate([[flights_formatted, filtered_count_formatted], [percentage_formatted, percentage_value_formatted]],tablefmt='fancy_grid'))

def select_by_percentaje_to_day(session, day, airport):
    log.info(f"Retrieving percentage by airport {airport} on day {day}")
    stmt = session.prepare(SELECT_BY_PERCENTAJE_TO_DAY)
    rows = session.execute(stmt, [airport, day])
    total_f = session.prepare(QUANTITY_ALL)
    total_r = session.execute(total_f)
    filtered_count = rows.one().filtered_count
    total_count = total_r.one().total_count
    percentage = (filtered_count / total_count) * 100
    print(f"Percentage by airport {airport} on day {day}")
    print("\n")
    filtered_count_formatted = f"{Fore.WHITE}{Style.BRIGHT}{filtered_count}{Style.RESET_ALL}"
    percentage_value_formatted = f"{Fore.WHITE}{Style.BRIGHT}{percentage:.2f}%{Style.RESET_ALL}"
    print(tabulate([[flights_formatted, filtered_count_formatted], [percentage_formatted, percentage_value_formatted]],tablefmt='fancy_grid'))


def select_by_percentaje_to_month(session, month, airport):
    log.info(f"Retrieving percentage by airport {airport} on month {month}")
    stmt = session.prepare(SELECT_BY_PERCENTAJE_TO_MONTH)
    rows = session.execute(stmt, [airport, month])
    total_f = session.prepare(QUANTITY_ALL)
    total_r = session.execute(total_f)
    filtered_count = rows.one().filtered_count
    total_count = total_r.one().total_count
    percentage = (filtered_count / total_count) * 100
    print(f"Percentage by airport {airport} on month {month}")
    print("\n")
    filtered_count_formatted = f"{Fore.WHITE}{Style.BRIGHT}{filtered_count}{Style.RESET_ALL}"
    percentage_value_formatted = f"{Fore.WHITE}{Style.BRIGHT}{percentage:.2f}%{Style.RESET_ALL}"
    print(tabulate([[flights_formatted, filtered_count_formatted], [percentage_formatted, percentage_value_formatted]],tablefmt='fancy_grid'))


def select_by_percentaje_to_year(session, year, airport):
    log.info(f"Retrieving percentage by airport {airport} on year {year}")
    stmt = session.prepare(SELECT_BY_PERCENTAJE_TO_YEAR)
    rows = session.execute(stmt, [airport, year])
    total_f = session.prepare(QUANTITY_ALL)
    total_r = session.execute(total_f)
    filtered_count = rows.one().filtered_count
    total_count = total_r.one().total_count
    percentage = (filtered_count / total_count) * 100
    print(f"Percentage by airport {airport} on year {year}")
    print("\n")
    filtered_count_formatted = f"{Fore.WHITE}{Style.BRIGHT}{filtered_count}{Style.RESET_ALL}"
    percentage_value_formatted = f"{Fore.WHITE}{Style.BRIGHT}{percentage:.2f}%{Style.RESET_ALL}"
    print(tabulate([[flights_formatted, filtered_count_formatted], [percentage_formatted, percentage_value_formatted]],tablefmt='fancy_grid'))


def select_by_percentaje_connection_true(session):
    log.info("Retrieving percentage of flights with connections")
    stmt = session.prepare(SELECT_BY_PERCENTAJE_CONNECTION_TRUE)
    rows = session.execute(stmt)
    total_f = session.prepare(QUANTITY_ALL)
    total_r = session.execute(total_f)
    filtered_count = rows.one().filtered_count
    total_count = total_r.one().total_count
    percentage = (filtered_count / total_count) * 100
    print(f"Percentage of flights with connections")
    print("\n")
    filtered_count_formatted = f"{Fore.WHITE}{Style.BRIGHT}{filtered_count}{Style.RESET_ALL}"
    percentage_value_formatted = f"{Fore.WHITE}{Style.BRIGHT}{percentage:.2f}%{Style.RESET_ALL}"
    print(tabulate([[flights_formatted, filtered_count_formatted], [percentage_formatted, percentage_value_formatted]],tablefmt='fancy_grid'))


def select_by_percentaje_connection_false(session):
    log.info("Retrieving percentage of flights without connections")
    stmt = session.prepare(SELECT_BY_PERCENTAJE_CONNECTION_FALSE)
    rows = session.execute(stmt)
    total_f = session.prepare(QUANTITY_ALL)
    total_r = session.execute(total_f)
    filtered_count = rows.one().filtered_count
    total_count = total_r.one().total_count
    percentage = (filtered_count / total_count) * 100
    print(f"Percentage of flights without connections")
    print("\n")
    filtered_count_formatted = f"{Fore.WHITE}{Style.BRIGHT}{filtered_count}{Style.RESET_ALL}"
    percentage_value_formatted = f"{Fore.WHITE}{Style.BRIGHT}{percentage:.2f}%{Style.RESET_ALL}"
    print(tabulate([[flights_formatted, filtered_count_formatted], [percentage_formatted, percentage_value_formatted]],tablefmt='fancy_grid'))

def select_by_percentaje_airline_date(session, airline, day, month, year):
    log.info(f"Retrieving percentage of {airline} flights on {day}/{month}/{year}")
    stmt = session.prepare(SELECT_BY_PERCENTAJE__AIRLINE_DATE)
    rows = session.execute(stmt, [airline, day, month, year])
    total_f = session.prepare(QUANTITY_ALL)
    total_r = session.execute(total_f)
    filtered_count = rows.one().filtered_count
    total_count = total_r.one().total_count
    percentage = (filtered_count / total_count) * 100
    print(f"Percentage of {airline} flights on {day}/{month}/{year}")
    print("\n")
    filtered_count_formatted = f"{Fore.WHITE}{Style.BRIGHT}{filtered_count}{Style.RESET_ALL}"
    percentage_value_formatted = f"{Fore.WHITE}{Style.BRIGHT}{percentage:.2f}%{Style.RESET_ALL}"
    print(tabulate([[flights_formatted, filtered_count_formatted], [percentage_formatted, percentage_value_formatted]],tablefmt='fancy_grid'))
