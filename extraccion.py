import csv

# Nombre del archivo CSV
file_name = 'flight_passengers.csv'

# Lista donde se almacenarán los datos
data_list = []

# Abrir archivo CSV y leer datos
with open(file_name, newline='') as csvfile:
    reader = csv.DictReader(csvfile)
    for row in reader:
        data_list.append(row)

# Guardar los datos en un archivo de texto
with open('tools/data.cql', 'w') as f:
    for item in data_list:
        print("INSERT INTO airport_wait_time (airline, de, hacia, day, month, year, age, gender, reason, stay, transit, connection, wait) VALUES ('{0}', '{1}', '{2}', {3}, {4}, {5}, {6}, '{7}', '{8}', '{9}', '{10}', '{11}', {12});".format(
        item["airline"], item["de"], item["hacia"], int(item["day"]), int(item["month"]), int(item["year"]), int(item["age"]), item["gender"], item["reason"], item["stay"], item["transit"], item["connection"], int(item["wait"])
    ), file=f)
