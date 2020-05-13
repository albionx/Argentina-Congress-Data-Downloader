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
    queryTally = int()

    # This path provides initial access, the API will give a cursor to the next results
    resourceUrl = '{}{}'.format(BASE_URL, resourcePathCursor)
    resourceData = getJsonContents(resourceUrl)

    while True:
        # Catch failures on their backend
        if resourceData.get('success') != True: 
            print ('Query didn\'t execute successfully:', resourceData['success'])
            break

        # Count total number of records in dataset and in this particular resultset
        queryResults = len(resourceData['result']['records'])
        queryTotal = resourceData['result']['total']
        queryTally = queryTally + queryResults

        queryInsertions = writeToDB(resourceData)

        # Establish progress in an unnecessarily visual way
        progress = (queryTally / queryTotal)
        print('Downloading {} data: {}{} {}% ({}/{}) \r'.format(
            SOURCE_NAME,
            "█" * round(progress * 20),
            "░" * (20 - round(progress * 20)),
            round(progress * 100),
            queryTally,
            queryTotal
            ), end="")
        #print ('Found {} results in this page: inserted {} records and skipped {}.'.format(queryResults, queryInsertions, queryResults - queryInsertions))
        #print ('We are {}% of the way there'.format(round((queryTally / queryTotal) * 100)))
        #print ('=' * 30)

        # Exit if we've reached 100% already
        if queryTally == queryTotal: 
            print ('\nSUCCESS! Data saved in the \'{}\' table, in the \'{}\' file.'.format(SOURCE_NAME, DATABASE_NAME))
            break

        # Look for the next results
        if queryResults > 0: # there is potentially more to be obtained
            resourcePathCursor = resourceData['result']['_links']['next']
            resourceUrl = '{}{}'.format(BASE_URL, resourcePathCursor)
            resourceData = getJsonContents(resourceUrl)

def writeToDB(resourceData):

    # Retrieve the schema and helper strings
    fullSchema = str() # needed because I'm building on schema
    fieldTypes = dict() # this dictionary stores each field and the type, which we'll use when doing the WHERE clause

    for field in resourceData['result']['fields']:
        fullSchema = '{}, {} {}'.format(fullSchema, field['id'], field['type'])
        fieldTypes[field['id']] = field['type']

    fullSchema = fullSchema[1:]

     # Create table with the fullSchema collected before
    database = sqlite3.connect(DATABASE_NAME)
    cursor = database.cursor()
    sql = 'CREATE TABLE IF NOT EXISTS {} ({})'.format(SOURCE_NAME, fullSchema)
    cursor.execute(sql)

    # Loop through records to insert into the database. Keep track of additions.
    queryInsertions = int()

    for record in resourceData['result']['records']:

        # helper values
        fields = (str(list(record.keys()))[1:-1])
        # build the equivalent to 'fields' but changing the None values for "", so that i can use it in the SELECT query
        values = list()
        for value in record.values():
            if value is None: 
                values.append("")
            else:
                values.append(value)
        values = str(values)[1:-1]
        conditions = buildQueryCondition(record, fieldTypes)

        # See if the record is present, and if so, skip
        # TODO: Allow for records to be updated, by checking only for an ID and then verifying if the contents are different before doing an update
        select_sql = 'SELECT ROWID FROM {} WHERE {}'.format(SOURCE_NAME, conditions)
        try:
            cursor.execute(select_sql)
        except:
            print ('Problem running query:', sql)

        if cursor.fetchone() is None:
            insert_sql = 'INSERT INTO {} ({}) VALUES ({})'.format(SOURCE_NAME, fields, values)
            cursor.execute(insert_sql)
            queryInsertions += 1

    database.commit()

    return queryInsertions

def getJsonContents(url):
    data = None
    try:
        sleep(DELAY_IN_SECONDS)
        urlHandler = urllib.request.urlopen(url)
        urlContents = urlHandler.read().decode()
        response_code = urlHandler.getcode()
        if response_code == 200:
            data = json.loads(urlContents)
        else:
            print('Error retrieving contents from URL {}'.format(url))
    except Exception as ex:
        print ('Problem obtaining or parsing the data from: {} - Error {}'.format(url, ex))
    return data

def buildQueryCondition(record, fieldTypes):
    conditions = str()
    try:
        for fieldName in record.keys():
            # Deal with None values by using "". In text fields this becomes a "" filed. In numeric fields it becomes NULL.
            if record[fieldName] == None: 
                conditions = '{} AND {} = ""'.format(conditions, fieldName)
                continue
            # Deal with formatting and SQL injection protection.
            if fieldTypes[fieldName].lower() == 'text' or fieldTypes[fieldName].lower() == 'timestamp':
                conditions = '{} AND {} = "{}"'.format(conditions, fieldName, record[fieldName].replace('\"','\''))
            else:
                conditions = '{} AND {} = {}'.format(conditions, fieldName, str(record[fieldName]).replace(' ',''))
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
            resourcePathCursor = '/api/3/action/datastore_search?resource_id=2cdaef71-f802-4067-bfa0-810dd3a22583'
        elif decision.lower() == '2':
            SOURCE_NAME = 'Laws'
            resourcePathCursor = '/api/3/action/datastore_search?resource_id=a88b42c3-d375-4072-8542-92b11db1d711'
        elif decision.lower() == '3':
            SOURCE_NAME = 'Sessions'
            resourcePathCursor = '/api/3/action/datastore_search?resource_id=4ac70a51-a82d-428b-966a-0a203dd0a7e3'
        elif decision.lower() == '4':
            SOURCE_NAME = 'Representatives'
            resourcePathCursor = '/api/3/action/datastore_search?resource_id=16cd699d-83fb-4d5f-afd4-0af9b47b1bd7'
        elif decision.lower() == '5' or decision.lower() == '':
            quit()

        runDataImporter(resourcePathCursor)

    except KeyboardInterrupt:
        print ('\nUser quit with an interrupt')