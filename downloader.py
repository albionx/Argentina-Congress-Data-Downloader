#!/usr/bin/env python
""" Downloads the data from el Congreso Nacional de Argentina """

import json
import sqlite3
import time
import urllib.request

BASE_URL = 'https://datos.hcdn.gob.ar:443/'
SOURCE_NAME = 'Leyes'
DATABASE_NAME = 'congreso.sqlite'

def runDataImporter():
    queryTally = int()

    # This path provides initial access, the API will give a cursor to the next results
    resourcePathCursor = 'api/3/action/datastore_search?resource_id=a88b42c3-d375-4072-8542-92b11db1d711'
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

        # Establish progress
        print ('Found {} results in this page: inserted {} records and skipped {}.'.format(queryResults, queryInsertions, queryResults - queryInsertions))
        print ('We are {}% of the way there'.format(round((queryTally / queryTotal) * 100)))
        print ('=' * 30)

        # Look for the next results
        if queryResults > 0: # there is potentially more to be obtained
            resourcePathCursor = resourceData['result']['_links']['next']
            resourceUrl = '{}{}'.format(BASE_URL, resourcePathCursor)
            resourceData = getJsonContents(resourceUrl)
        else:
            print('Done here!')
            break

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

    # Loop through records to insert into the database. Keep track of skips and additions.
    queryInsertions = int()

    for record in resourceData['result']['records']:
        # helper values
        fields = (str(list(record.keys()))[1:-1])
        values = (str(list(record.values()))[1:-1])
        conditions = buildQueryCondition(record, fieldTypes)

        # See if the record is present, and if so, skip
        sql = 'SELECT ROWID FROM {} WHERE {}'.format(SOURCE_NAME, conditions)
        try:
            cursor.execute(sql)
        except:
            print ('Problem running query:', sql)

        if cursor.fetchone() is None:
            sql = 'INSERT INTO {} ({}) VALUES ({})'.format(SOURCE_NAME, fields, values)
            cursor.execute(sql)
            queryInsertions += 1

    database.commit()

    return queryInsertions

def getJsonContents(url):
    data = None
    try:
        print ('Retrieving: ', url)
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
    for fieldName in record.keys():
        if fieldTypes[fieldName].lower() == 'text':
            # avoid SLQ injection by preventing closing of the double quotes
            conditions = '{} AND {} = "{}"'.format(conditions, fieldName, record[fieldName].replace('\"','\''))
        else:
            # avoid SLQ injection by preventing a space
            conditions = '{} AND {} = {}'.format(conditions, fieldName, str(record[fieldName]).replace(' ',''))
    return conditions[5:]

if __name__ == '__main__':
    runDataImporter()
