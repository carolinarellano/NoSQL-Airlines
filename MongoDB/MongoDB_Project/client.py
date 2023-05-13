#!/usr/bin/env python3
import argparse
import logging
import os
import requests


# Set logger
log = logging.getLogger()
log.setLevel('INFO')
handler = logging.FileHandler('flights.log')
handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s"))
log.addHandler(handler)

# Read env vars related to API connection
FLIGHTS_API_URL = os.getenv("FLIGHTS_API_URL", "http://localhost:8000")


def print_flights(flight):
    for k in flight.keys():
        print(f"{k}: {flight[k]}")
    print("="*50)

def list_flight():
    option = input("Statistics for airline or airport(airline/airport): ")
    option_chosen = input("Write the specific airline/airport: ")
    suffix = "/flight"
    endpoint = FLIGHTS_API_URL + suffix
    params = {
        "option": option,
        "option_chosen": option_chosen,
    }
    
    response = requests.get(endpoint, params=params)
    if response.ok:
        json_resp = response.json()
        for flight in json_resp:
            print_flights(flight)
    else:
        print(f"Error: {response}")



def main():
    log.info(f"Welcome to books catalog. App requests to: {FLIGHTS_API_URL}")

    parser = argparse.ArgumentParser()

    list_of_actions = ["search", "get", "update", "delete"]
    parser.add_argument("action", choices=list_of_actions,
            help="Action to be user for the books library")
    parser.add_argument("-i", "--id",
            help="Provide a book ID which related to the book action", default=None)
    parser.add_argument("-r", "--rating",
            help="Search parameter to look for books with average rating equal or above the param (0 to 5)", default=0)
    parser.add_argument("-p", "--num_pages",
            help="Search parameter to look for books with num_pages equal or above the param", default=0)
    parser.add_argument("-rc", "--ratings_count",
            help="Search parameter to look for books with ratings_count equal or above the param", default=0)
    parser.add_argument("-l", "--language",
            help="Search parameter to look for books with a specific author", default=None)
    

    args = parser.parse_args()

    if args.id and not args.action in ["get", "update", "delete"]:
        log.error(f"Can't use arg id with action {args.action}")
        exit(1)

    if args.rating and args.action != "search":
        log.error(f"Rating arg can only be used with search action")
        exit(1)

    if args.action == "search":
        list_flight()

if __name__ == "__main__":
    main()