import urllib.request
import json

from connect import connect

def loadSingleData(item, region):
    ##
    #   Avoid eventual "None" values: they mean 0, so they are changed into 0s
    #

    for field in item:
        if item[field] == None:
            item[field] = 0

    ##
    #   Get all the data of the previous day: it will be used to calculate the variation
    #

    query = f"SELECT Ricoverati_con_sintomi, Terapia_intensiva, Ospedalizzati, Isolamento_domiciliare, Positivi, " \
            f"Nuovi_positivi, Dimessi_guariti, Deceduti, Tamponi, Casi_testati FROM IERI WHERE REGIONE = '{region}'"
    db_action.execute(query)

    yesterday = {}
    for (Ricoverati_con_sintomi, Terapia_intensiva, Ospedalizzati, Isolamento_domiciliare, Positivi, Nuovi_positivi,
         Dimessi_guariti, Deceduti, Tamponi, Casi_testati) in db_action:
        yesterday["ricoverati_con_sintomi"] = Ricoverati_con_sintomi
        yesterday["terapia_intensiva"] = Terapia_intensiva
        yesterday["totale_ospedalizzati"] = Ospedalizzati
        yesterday["isolamento_domiciliare"] = Isolamento_domiciliare
        yesterday["totale_positivi"] = Positivi
        yesterday["nuovi_positivi"] = Nuovi_positivi
        yesterday["dimessi_guariti"] = Dimessi_guariti
        yesterday["deceduti"] = Deceduti
        yesterday["tamponi"] = Tamponi
        yesterday["casi_testati"] = Casi_testati

    ##
    #   Get the sum of some values: these fields are saved in the origin database as the sum of the value for the whole
    #   pandemic. We want our database to have jsut daily values, so we subtract the previous total from what is written
    #   in the origin database
    #

    query = f"SELECT SUM(Dimessi_guariti), SUM(Deceduti), SUM(Tamponi), SUM(Casi_testati) FROM STORICO WHERE " \
            f"REGIONE = '{region}'"
    db_action.execute(query)

    total = {}
    for (Dimessi_guariti, Deceduti, Tamponi, Casi_testati) in db_action:
        total["dimessi_guariti"] = Dimessi_guariti
        total["deceduti"] = Deceduti
        total["tamponi"] = Tamponi
        total["casi_testati"] = Casi_testati

    #print(total)

    ##
    #   Initialize the fields, values and variation variables. They will be used as follows for the query:
    #   INSERT INTO 'STORICO' ([fields]) VALUES([values])
    #   INSERT INTO 'VARIAZIONE' ([fields]) VALUES([variation])
    #   UPDATE IERI SET [update] WHERE Region = '[region]'
    #

    fields = "Data, Regione"
    values = f"'{item['data']}', '{region}'"
    variation = f"'{item['data']}', '{region}'"
    update = ""

    ##
    #   Insert into the fields the values that are saved in the origin database as single-day values
    #

    dbNames = ["Ricoverati_con_sintomi", "Terapia_intensiva", "Ospedalizzati", "Isolamento_domiciliare", "Positivi", "Nuovi_positivi"]
    jsonNames = ["ricoverati_con_sintomi", "terapia_intensiva", "totale_ospedalizzati", "isolamento_domiciliare", "totale_positivi", "nuovi_positivi"]
    for i in range(0, len(dbNames)):
        fields = fields + f", {dbNames[i]}"
        values = values + f", {item[jsonNames[i]]}"
        variation = f"{variation}, {int(item[jsonNames[i]]) - int(yesterday[jsonNames[i]])}"
        update = f"{update}, {dbNames[i]} = {item[jsonNames[i]]}"

    ##
    #   Insert into the fields the values that are saved in the origin database total values
    #

    dbNames = ["Dimessi_guariti", "Deceduti",  "Tamponi", "Casi_testati"]
    jsonNames = ["dimessi_guariti", "deceduti", "tamponi", "casi_testati"]
    for i in range(0, len(dbNames)):
        if total[jsonNames[i]] == None:
            total[jsonNames[i]] = 0
        fields = fields + f", {dbNames[i]}"
        values = values + f", {int(int(item[jsonNames[i]]) - total[jsonNames[i]])}"
        variation = f"{variation}, {int(int(item[jsonNames[i]]) - total[jsonNames[i]]) - int(yesterday[jsonNames[i]])}"
        update = f"{update}, {dbNames[i]} = {int(int(item[jsonNames[i]]) - total[jsonNames[i]])}"
        #print(f"\t{item[jsonNames[i]]} - {total[jsonNames[i]]} = {int(int(item[jsonNames[i]]) - total[jsonNames[i]])}")

    update = update[2:]

    # print(fields)
    # print(values)
    # print(variation)
    # print(update)
    db_action.execute(f"UPDATE IERI SET {update} WHERE Regione = '{region}'")
    db_action.execute(f"INSERT INTO STORICO ({fields}) VALUES({values})")
    db_action.execute(f"INSERT INTO VARIAZIONE ({fields}) VALUES({variation})")
    # print(f"UPDATE IERI SET {update} WHERE Regione = '{region}'")
    # print(f"INSERT INTO 'STORICO' ({fields}) VALUES({values})")
    # print(f"INSERT INTO 'VARIAZIONE' ({fields}) VALUES({variation})")
    # print()


##
#   Get data from the origin database
#

urlIta = "https://raw.githubusercontent.com/pcm-dpc/COVID-19/master/dati-json/dpc-covid19-ita-andamento-nazionale-latest.json"
ita = urllib.request.urlopen(urlIta)
jsonIta = json.loads(ita.read().decode("utf-8"))

urlRegioni = "https://raw.githubusercontent.com/pcm-dpc/COVID-19/master/dati-json/dpc-covid19-ita-regioni-latest.json"
regioni = urllib.request.urlopen(urlRegioni)
jsonRegioni = json.loads(regioni.read().decode("utf-8"))

##
#   Establish connection to the database
#

try:
    conn = connect()
    db_action = conn.cursor()
except Exception:
    quit()

##
#   Process the data of Italy
#

for item in jsonIta:
    loadSingleData(item, "Italia")

##
#   Process the data of the regions
#

foundPA = False
bolzano = {}

for item in jsonRegioni:

    item["denominazione_regione"] = item["denominazione_regione"].replace("P.A. ", "").replace(" ", "-").replace("'", "_")

    if item["denominazione_regione"] == "Bolzano" or item["denominazione_regione"] == "Trento":
        if foundPA:
            for index in ["ricoverati_con_sintomi", "terapia_intensiva", "totale_ospedalizzati",
                          "isolamento_domiciliare",
                          "totale_positivi", "nuovi_positivi", "dimessi_guariti", "deceduti", "totale_casi", "tamponi",
                          "casi_testati"]:
                if item[index] == None:
                    item[index] = 0
                if bolzano[index] == None:
                    bolzano[index] = 0
                item[index] = int(item[index]) + int(bolzano[index])
            loadSingleData(item, "Trentino-Alto-Adige")
            foundPA = False
            bolzano = {}

        else:
            for index in ["ricoverati_con_sintomi", "terapia_intensiva", "totale_ospedalizzati", "isolamento_domiciliare",
                         "totale_positivi", "nuovi_positivi", "dimessi_guariti", "deceduti", "totale_casi", "tamponi",
                         "casi_testati"]:
                bolzano[index] = item[index]
            foundPA = True

    else:
        loadSingleData(item, item["denominazione_regione"])


