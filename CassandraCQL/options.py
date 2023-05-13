meses_dict = {
    1: "Enero",
    2: "Febrero",
    3: "Marzo",
    4: "Abril",
    5: "Mayo",
    6: "Junio",
    7: "Julio",
    8: "Agosto",
    9: "Septiembre",
    10: "Octubre",
    11: "Noviembre",
    12: "Diciembre"
}


def print_airlines():
    airlines = ["American Airlines", "Delta Airlines", "Alaska", "Aeromexico", "Volaris"]
    for i in range(len(airlines)):
        if i == len(airlines) - 1:
            print(airlines[i])
        else:
            print(airlines[i] + ",", end=" ")

def print_airports():
    airports = ["PDX", "GDL", "SJC", "LAX", "JFK"]
    for i in range(len(airports)):
        if i == len(airports) - 1:
            print(airports[i])
        else:
            print(airports[i] + ",", end=" ")

def print_genders():
    genders = ["male", "female", "unspecified", "undisclosed"]
    for i in range(len(genders)):
        if i == len(genders) - 1:
            print(genders[i])
        else:
            print(genders[i] + ",", end=" ")

def print_reasons():
    reasons = ["On vacation/Pleasure", "Business/Work", "Back Home"]
    for i in range(len(reasons)):
        if i == len(reasons) - 1:
            print(reasons[i])
        else:
            print(reasons[i] + ",", end=" ")

def print_stays():
    stays = ["Hotel", "Short-term homestay", "Home", "Friend/Family"]
    for i in range(len(stays)):
        if i == len(stays) - 1:
            print(stays[i])
        else:
            print(stays[i] + ",", end=" ")

def print_transits():
    transits = ["Airport cab", "Car rental", "Mobility as a service", "Public Transportation", "Pickup", "Own car"]
    for i in range(len(transits)):
        if i == len(transits) - 1:
            print(transits[i])
        else:
            print(transits[i] + ",", end=" ")

def print_yes_no():
    options = ['True', 'False']
    for i in range(len(options)):
        print(f"{i+1}- {options[i]}", end="")
        if i < len(options) - 1:
            print(",", end=" ")
    print()

def print_wait():
    options = ['LESS THAN','MORE THAN']
    for i in range(len(options)):
        print(f"{i+1}- {options[i]}", end="")
        if i < len(options) - 1:
            print(",", end=" ")
    print()



def print_all():
    print("\033[1m\033[4mAirline options:\033[0m", end=" ")
    print_airlines()
    print("\n\033[1m\033[4mAirport options:\033[0m", end=" ")
    print_airports()
    print("\n\033[1m\033[4mGender options:\033[0m", end=" ")
    print_genders()
    print("\n\033[1m\033[4mReason options:\033[0m", end=" ")
    print_reasons()
    print("\n\033[1m\033[4mStay options:\033[0m", end=" ")
    print_stays()
    print("\n\033[1m\033[4mTransit options:\033[0m", end=" ")
    print_transits()
    print('\n')








def main():
    print('\n')
    print_all()

    print_yes_no()
    print_wait()
    return 0

if __name__ == '__main__':
    main()
