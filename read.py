import os
import json
import mysql.connector
import getpass
import gzip


def safeGet(map, key, fallback):
	if(key in map):
		return map[key]

	return fallback




def readSecDataFolder(folderName):
	#get 10-k filing's asch id's
	subfile = open( folderName + "/sub.txt")

	myline = subfile.readline()
	values = myline.split("\t")


	idsForAnnualReports = {}
	while myline:
		
		values = myline.split("\t")
		
		if("10-K" in values[25]):
			idsForAnnualReports[values[0]] = values[1]

		myline = subfile.readline()

	subfile.close()



	#Read importan columne
	importantColumnMap = {}

	importantColumnList = [
	"Assets",
	"Liabilities",
	"Goodwill",
	"FiniteLivedIntangibleAssetsNet",
	"EntityCommonStockSharesOutstanding"
	]

	for item in importantColumnList:
		importantColumnMap[item] = ""



	numfile = open(folderName + "/num.txt")

	myline = numfile.readline()

	analysisdb = {}

	while myline:
		values = myline.split("\t")
		if(values[0] in idsForAnnualReports):
			#print(myline)
			if(values[1] in importantColumnMap):
				cik = idsForAnnualReports[values[0]]
				idToPrint =  cik
				if(cik in cikToTickerMap):
					ticker = cikToTickerMap[cik]
					
					if(not ticker in analysisdb):
						analysisdb[ticker] = {}

					period = values[4][0:4] #trim for just the year
					valueName = values[1]

					if(not period in analysisdb[ticker]):
						analysisdb[ticker][period] = {}
				
					analysisdb[ticker][period][valueName] = values[7]
					#print(myline)




		myline = numfile.readline()

	numfile.close()


	for ticker in analysisdb:
		tickerYears = analysisdb[ticker]
		for year in tickerYears:
			dataMap = tickerYears[year]

			assets = safeGet(dataMap, "Assets", 0.0)
			liabilities = safeGet(dataMap, "Liabilities", 0.0)
			goodwill = safeGet(dataMap, "Goodwill", 0.0)
			intangibles = safeGet(dataMap, "FiniteLivedIntangibleAssetsNet", 0.0)
			shares = safeGet(dataMap, "EntityCommonStockSharesOutstanding", 0.0)
			netIncome = safeGet(dataMap, "NetIncomeLoss", 0.0)

			sqlQuery = f"INSERT INTO annual_reports (ticker, year, assets, liabilities, goodwill, intangibles, net_income, shares)"
			sqlQuery += f" VALUES ('{ticker}','{year}', '{assets}', '{liabilities}', '{goodwill}', '{intangibles}', '{netIncome}', '{shares}')"

			try:
				cursor.execute(sqlQuery)
			except mysql.connector.Error as err:
				print(err.msg)
				print(sqlQuery)
				#print(sqlQuery)

	cnx.commit()
	print("Done reading folder: " + folderName)




##---MAIN ENTRY----

#map cik to tickers
cikToTickerMap = {}
cikMapFile = open("cik_to_ticker.txt")

myline = cikMapFile.readline()
while myline:
	
	values = myline.split("\t")
	cikStripped = values[1].replace('\n','')
	cikToTickerMap[cikStripped] = values[0]
	myline = cikMapFile.readline()

cikMapFile.close()


#setup table in the db
username = input("mysql uname: ")
pwd = getpass.getpass("mysql pwd: ")

cnx = mysql.connector.connect(user=username, password=pwd,
                              host='127.0.0.1',
                              database='financials')

cursor = cnx.cursor()

TABLES = {}

TABLES['annual_reports'] = (
    "CREATE TABLE `annual_reports` "
    "(ticker varchar(32),"
    "year int, "
    "cik int, "
    "asch varchar(32), "
    "currency varchar(32), "
    "assets double, "
    "liabilities double, "
    "goodwill double, "
    "intangibles double, "
    "net_income double,"
    "shares double, "
    "PRIMARY KEY(ticker, year)"
    ") ENGINE=InnoDB"
    )

for table_name in TABLES:
    table_description = TABLES[table_name]
    try:
        print("Creating table {}: ".format(table_name), end='')
        cursor.execute(table_description)
    except mysql.connector.Error as err:
            print(err.msg)
    else:
        print("OK")


cnx.commit()


for subdir, dirs, files in os.walk("SourceData"):
	for dir in dirs:
		dirPath = "SourceData/" + dir
		print("Read Folder: " + dirPath)
		readSecDataFolder(dirPath)

print("DONE!")