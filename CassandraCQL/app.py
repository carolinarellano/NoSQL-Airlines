#!/usr/bin/env python3
import logging
import os
import random
import options


from cassandra.cluster import Cluster

import model

# Set logger
log = logging.getLogger()
log.setLevel('INFO')
handler = logging.FileHandler('investments.log')
handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s"))
log.addHandler(handler)

# Read env vars releated to Cassandra App
CLUSTER_IPS = os.getenv('CASSANDRA_CLUSTER_IPS', 'localhost')
KEYSPACE = os.getenv('CASSANDRA_KEYSPACE', 'investments')
REPLICATION_FACTOR = os.getenv('CASSANDRA_REPLICATION_FACTOR', '1')



def print_menu():
    print('\n')
    mm_options = {
        1: "Show all flights",
        2: "Show flights by airline",
        3: "Show flights by airline and wait time",
        4: "Show flights by wait",# dos opciones disponibles
        5: "Show flights by month and year",
        6: "Show flights by origin and destination",
        7: "Show flights by origin, destination and wait time",
        8: "Show flights by stay and connection ",
        9: "Show flights by airline and origin",
        10: "Show flights by transit and wait time",
        11: "Show flights by origin, destination and month",
        12: "Show flights by percentage for a specific date",
        13: "Show flights by percentage for a specific day",
        14: "Show flights by percentage for wait time less than or equal to",
        15: "Show flights by percentage for wait time greater than or equal to",
        16: "Show flights by percentage for a specific airport (Origin) and day",
        17: "Show flights by percentage for a specific airport (Origin) and month",
        18: "Show flights by percentage for a specific airport (Origin) and year",
        19: "Show flights by percentage for connecting flights",
        20: "Show flights by percentage for non-connecting flights",
        21: "Show flights by percentage for a specific airline and date",
        22: "Exit",
    }
    for key in mm_options.keys():
        print(key, '--', mm_options[key])



def get_instrument_value(instrument):
    instr_mock_sum = sum(bytearray(instrument, encoding='utf-8'))
    return random.uniform(1.0, instr_mock_sum)


def main():
    log.info("Connecting to Cluster")
    cluster = Cluster(CLUSTER_IPS.split(','))
    session = cluster.connect()

    model.create_keyspace(session, KEYSPACE, REPLICATION_FACTOR)
    session.set_keyspace(KEYSPACE)

    model.create_schema(session)

    

    
    
    while(True):
        print_menu()
        print("\nCopy and paste when there are given options\n")
        option = int(input('Enter your choice: '))
        if option == 1:
            model.select_all(session)

        elif option == 2:
            options.print_airlines()
            airline_name = input('Enter airline name: ')
            model.select_by_airline(session,airline_name)

        elif option == 3:
            options.print_airlines()
            airline_name = input('Enter airline name: ')
            wait_time = int(input('Enter wait time: '))
            model.select_by_airline_wait(session,airline_name, wait_time)

        elif option == 4:
            options.print_wait()
            wait_option = int(input('Enter your wait choice: '))
            if wait_option == 1:
                wait_time = int(input('Enter wait time: '))
                model.select_by_wait_less_0(session,wait_time)
            elif wait_option == 2:
                wait_time = int(input('Enter wait time: '))
                model.select_by_wait_more_0(session,wait_time)

        elif option == 5:
            month = int(input('Enter month: '))
            year = int(input('Enter year: '))
            model.select_by_month_year(session, month, year)

        elif option == 6:
            options.print_airports()
            origin = input('Enter origin airport: ')
            destination = input('Enter destination airport: ')
            model.select_by_from_to(session,origin, destination)

        elif option == 7:
            options.print_airports()
            origin = input('Enter origin airport: ')
            destination = input('Enter destination airport: ')
            wait_time = int(input('Enter wait time: '))
            model.select_by_from_to_wait(session,origin, destination, wait_time)
            
        elif option == 8:
            options.print_stays()
            stay_option = input('Enter stay option:')
            options.print_yes_no()
            connection = input('Enter connection option: ')
            model.select_by_stay_connection(session, stay_option,connection)

        elif option == 9:
            options.print_airlines()
            airline_name = input('Enter airline name: ')
            options.print_airports()
            origin = input('Enter origin airport: ')
            model.select_by_airline_from(session,airline_name, origin)

        elif option == 10:
            wait_time = int(input('Enter wait time: '))
            options.print_transits()
            transit_option = input('Enter transit option: ')
            model.select_by_transit_wait(session, transit_option, wait_time)

        elif option == 11:
            options.print_airports()
            origin = input('Enter origin airport: ')
            destination = input('Enter destination airport: ')
            month = int(input('Enter month: '))
            model.select_by_from_to_month(session, origin, destination, month)

        elif option == 12:
            date = input('Enter date (dd-mm-yyyy): ')
            day, month, year = date.split("-")
            day = int(day)
            month = int(month)
            year = int(year)
            model.select_by_percentaje_date(session,day,month,year)

        elif option == 13:
            day = int(input('Enter day of the month: '))
            model.select_by_percentaje_day(session, day)

        elif option == 14:
            wait_time = int(input('Enter wait time: '))
            model.select_by_percentaje_less_wait(session,wait_time)

        elif option == 15:
            wait_time = int(input('Enter wait time: '))
            model.select_by_percentaje_more_wait(session, wait_time)

        elif option == 16:
            options.print_airports()
            airport_code = input('Enter airport name (Origin): ')
            day = int(input('Enter day of the month: '))
            model.select_by_percentaje_to_day(session,day,airport_code)

        elif option == 17:
            options.print_airports()
            airport_code = input('Enter airport name (Origin): ')
            month = int(input('Enter month mm: '))
            model.select_by_percentaje_to_month(session, month,airport_code)
        
        elif option == 18:
            options.print_airports()
            airport_code = input('Enter airport name (Origin): ')
            year = int(input('Enter year yyyy: '))
            model.select_by_percentaje_to_year(session,year,airport_code)
        
        elif option == 19:
            model.select_by_percentaje_connection_true(session)
        
        elif option == 20:
            model.select_by_percentaje_connection_false(session)

        elif option == 21:
            options.print_airlines()
            airline = input('Enter Airline: ')
            date = input('Enter date (dd-mm-yyyy): ')
            day, month, year = date.split("-")
            day = int(day)
            month = int(month)
            year = int(year)
            model.select_by_percentaje_airline_date(session,airline,day,month,year)
        
        elif option == 22:
            print("Exiting...")
            break;


if __name__ == '__main__':
    main()
