
''' This module downloads the data from el Congreso Nacional de Argentina '''

import json, urllib.request, sqlite3, time	

source = 'https://datos.hcdn.gob.ar:443/api/3/action/datastore_search?resource_id=a88b42c3-d375-4072-8542-92b11db1d711'
sourceName = 'Leyes'
queryTally = int()

while source is not False:
	
	# =================
	# File Acquisition
	# =================

	try:
		print ('Retrieving: ', source)
		urlHandler = urllib.request.urlopen(source)
		urlContents = urlHandler.read().decode()
		data = json.loads(urlContents)
	except:
		print ('Problem obtaining or parsing the data from: ', source)
		quit()

	# Catch failures on their backend
	if data['success'] is not True: print ('Data pull was not successful on the backend:', source)

	# Count total number of records in dataset and in this particular resultset
	queryResults = len(data['result']['records'])
	queryTotal = data['result']['total']
	print ('There are', queryResults,'in this page, and a total of',queryTotal,'for this dataset')

	# Obtain the next page to retrieve, if there are records left to analyze
	if queryResults > 0: 
		source = source[:source.find('/api')] + data['result']['_links']['next']
	else:
		source = False

	# =====================
	# Write on the database
	# =====================

	# Retrieve the schema and helper strings
	fullSchema = str() # needed because I'm building on schema
	schema = str()
	fieldTypes = dict() # this dictionary stores each field and the type, which we'll use when doing the WHERE clause
	for field in data['result']['fields']:
		fullSchema = fullSchema + ', ' + field['id'] + ' ' + field['type']
		schema = schema + ', ' + field['id']
		fieldTypes[field['id']] = field['type']
	fullSchema = fullSchema[1:]
	schema = schema [1:]

	def conditioner(record):
		""" this complicated function tries to assemble the list of conditions for the WHERE clause, since I can't use qmark given I want to dynamically generate both the field names and their values """
		conditions = str()
		for fieldName in record.keys(): 
			if fieldTypes[fieldName].lower() == 'text':
				conditions = conditions + ' AND ' + fieldName + ' = "' + record[fieldName] + '"'
			else:
				conditions = conditions + ' AND ' + fieldName + ' = ' + str(record[fieldName]) + ''
		return conditions[5:]

	# Create table with the schema
	database = sqlite3.connect('congreso.sqlite')
	cursor = database.cursor()
	sql = 'CREATE TABLE IF NOT EXISTS ' + sourceName + ' (' + fullSchema + ')'
	cursor.execute(sql)

	# Loop through records to insert into the database
	for record in data['result']['records']:
		# helper values
		fields = (str(list(record.keys()))[1:-1])
		values = (str(list(record.values()))[1:-1])
		conditions = conditioner(record)

		# See if the record is present, and if so, skip
		sql = 'SELECT ROWID FROM ' + sourceName + ' WHERE ' + conditions
		
		try:
			cursor.execute(sql)
		except:
			print ('Problem running query:', sql)

		if cursor.fetchone() is None: 
			sql = 'INSERT INTO ' + sourceName + ' (' + fields + ') VALUES (' + values + ')'
			cursor.execute(sql)
			print ('Added 1 record')
		else:
			print ('Skipped one record')
	
	database.commit()

	# Establish progress
	queryTally = queryTally + queryResults
	print ('We are ' + str(round((queryTally/queryTotal)*100)) + '% of the way there!')
	print ('='*30)
