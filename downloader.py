#!/usr/bin/env python

"""
Author: Santiago Andrigo <albionx@gmail.com>

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""

import json
import sqlite3
import urllib.request
from time import sleep

def runDataImporter(resourcePathCursor):

    MoreData = True
    queryTally = int()

    while MoreData:
        # This path provides initial access, the API will give a cursor to the next results
        resourceUrl = '{0}{1}'.format(BASE_URL, resourcePathCursor)
        resourceData = getJsonContents(resourceUrl)

        # Catch failures on their backend
        if resourceData.get('success') != True: 
            print ('Query didn\'t execute successfully:', resourceData['success'])
            break

        # Count total number of records in dataset and in this particular resultset
        queryResults = len(resourceData['result']['records'])
        queryTotal = resourceData['result']['total']
        queryTally += queryResults
        queryInsertions = writeToDB(resourceData)

        # Establish progress in an unnecessarily visual way
        print('Downloading {table} data: {progress}{remnant} {percentage}% ({tally}/{total}) \r'.format(
            table=SOURCE_NAME,
            progress="█" * round((queryTally / queryTotal) * 20),
            remnant="░" * (20 - round((queryTally / queryTotal) * 20)),
            percentage=round((queryTally / queryTotal) * 100),
            tally=queryTally,
            total=queryTotal
            ), end="")

        # update the cursor
        resourcePathCursor = resourceData['result']['_links']['next']

        # Exit if we've reached 100% already
        if queryTally == queryTotal: 
            print ("\nSUCCESS! Data saved in the '{0}' table, in the '{1}' file.".format(SOURCE_NAME, DATABASE_NAME))
            MoreData = False

def writeToDB(resourceData):

    # Retrieve the schema and helper strings
    fullSchema = str() # needed because I'm building on schema
    fieldTypes = dict() # this dictionary stores each field and the type, which we'll use when doing the WHERE clause

    for field in resourceData['result']['fields']:
        fullSchema = '{0}, {1} {2}'.format(fullSchema, field['id'], field['type'])
        fieldTypes[field['id']] = field['type']

    fullSchema = fullSchema[1:]

     # Create table with the fullSchema collected before
    with sqlite3.connect(DATABASE_NAME) as database:
        cursor = database.cursor()
        cursor.execute('CREATE TABLE IF NOT EXISTS {0} ({1})'.format(SOURCE_NAME, fullSchema))

        # Loop through records to insert into the database. Keep track of additions.
        queryInsertions = int()

        for record in resourceData['result']['records']:

            # helper values
            fields = (str(list(record.keys()))[1:-1])
            conditions = buildQueryCondition(record, fieldTypes)

            # See if the record is present, and if so, skip
            # TODO: Allow for records to be updated, by checking only for an ID and then verifying if the contents are different before doing an update
            select_sql = 'SELECT ROWID FROM {0} WHERE {1}'.format(SOURCE_NAME, conditions)

            try:
                cursor.execute(select_sql)
            except:
                print ('Problem running query:', select_sql)

            if cursor.fetchone() is None:
                cursor.execute('INSERT INTO {0} ({1}) VALUES (?)'.format(SOURCE_NAME, fields), record.values())
                queryInsertions += 1

        database.commit()

    return queryInsertions

def getJsonContents(url):
    data = None
    try:
        sleep(DELAY_IN_SECONDS)
        with urllib.request.urlopen(url) as urlHandler:
            urlContents = urlHandler.read().decode()
            response_code = urlHandler.getcode()
        if response_code == 200:
            data = json.loads(urlContents)
        else:
            print('Error retrieving contents from URL {0}'.format(url))
    except Exception as ex:
        print ('Problem obtaining or parsing the data from: {0} - Error {1}'.format(url, ex))
    return data

def buildQueryCondition(record, fieldTypes):
    conditions = str()
    try:
        for fieldName in record.keys():
            # Deal with None values by using "". In text fields this becomes a "" field. In numeric fields it becomes NULL.
            if record[fieldName] == None: 
                conditions = '{0} AND {1} = ""'.format(conditions, fieldName)
                continue
            # Deal with formatting and SQL injection protection.
            if fieldTypes[fieldName].lower() == 'text' or fieldTypes[fieldName].lower() == 'timestamp':
                conditions = '{0} AND {1} = "{2}"'.format(conditions, fieldName, record[fieldName].replace('\"','\''))
            else:
                conditions = '{0} AND {1} = {2}'.format(conditions, fieldName, str(record[fieldName]).replace(' ',''))
        return conditions[5:]
    except:
        print (record)
        print ((str(list(record.keys()))[1:-1]))
        print ((str(list(record.values()))[1:-1]))
        quit()

if __name__ == '__main__':

    DATABASE_NAME = 'congressData.sqlite'
    BASE_URL = 'https://datos.hcdn.gob.ar:443'
    DELAY_IN_SECONDS = 0 

    try:

        decision = input(
        """
Which dataset do you want to download:

    1. COVID19 Subsidies
    2. Laws
    3. House of Representatives Sessions (Diputados)
    4. List of Representatives (Diputados)
    5. Quit

""")
        if decision.lower() == '1':
            SOURCE_NAME = 'Subsidies'
            resourcePath = '/api/3/action/datastore_search?resource_id=2cdaef71-f802-4067-bfa0-810dd3a22583'
        elif decision.lower() == '2':
            SOURCE_NAME = 'Laws'
            resourcePath = '/api/3/action/datastore_search?resource_id=a88b42c3-d375-4072-8542-92b11db1d711'
        elif decision.lower() == '3':
            SOURCE_NAME = 'Sessions'
            resourcePath = '/api/3/action/datastore_search?resource_id=4ac70a51-a82d-428b-966a-0a203dd0a7e3'
        elif decision.lower() == '4':
            SOURCE_NAME = 'Representatives'
            resourcePath = '/api/3/action/datastore_search?resource_id=16cd699d-83fb-4d5f-afd4-0af9b47b1bd7'
        elif decision.lower() == '5' or decision.lower() == '':
            quit()

        runDataImporter(resourcePath)

    except KeyboardInterrupt:
        print ('\nUser quit with an interrupt')