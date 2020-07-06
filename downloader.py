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
import requests
from time import sleep
try:
    from PyInquirer import style_from_dict, Token, prompt
except ModuleNotFoundError:
    print ('PyInquirer missing. To install, please run: pip install PyInquirer')
    quit()
try:
    from tqdm import tqdm
except ModuleNotFoundError:
    print ('TQDM missing. To install, please run: pip install TQDM')
    quit()

def runDataImporter(SOURCE_NAME, resourcePathCursor):

    MoreData = True
    queryTally = int()

    with tqdm(total=100, desc='Download dataset of {}: '.format(SOURCE_NAME), bar_format = '{desc}{bar}| {percentage:3.0f}% [Duration: {elapsed}]') as progressBar:

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

            # Update progress bar
            progressBar.update(round(queryResults/queryTotal*100,2))

            # update the cursor
            resourcePathCursor = resourceData['result']['_links']['next']

            # Exit if we've reached 100% already
            if queryTally == queryTotal: 
                MoreData = False

def writeToDB(resourceData):

    # Retrieve the schema and helper strings
    fullSchema = str() # needed because I'm building on schema
    fieldTypes = dict() # this dictionary stores each field and the type, which we'll use when doing the WHERE clause

    # Ensure there is at least one field
    assert len(resourceData['result']['fields']) > 43 

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
        with requests.get(url) as urlHandler:
            if urlHandler.status_code == 200:
                data = urlHandler.json()
            else:
                print('Error retrieving contents from URL {0}'.format(url))
    except Exception as ex:
        print ('Problem obtaining or parsing the data from: {0} - Error {1}'.format(url, ex))
    
    # Testing that the data var is populated correctly as a Dictionary
    assert data is not None
    assert type(data) == dict
    
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

def obtainDecision():

    style = style_from_dict({
        Token.Separator: '#6C6C6C',
        Token.QuestionMark: '#FF9D00 bold',
        Token.Selected: '#5F819D',
        Token.Pointer: '#FF9D00 bold',
        Token.Answer: '#5F819D bold',
    })

    dataset_question = [
        {
        'type': 'checkbox', 
        'name': 'chosen',
        'message': 'Select all datasets you want to download:',
        'choices': [
            {'name': 'COVID19 Subsidies'},
            {'name': 'Laws'},
            {'name': 'House of Representatives Sessions (Diputados)'},
            {'name': 'List of Representatives (Diputados)'}
            ]
        }
        ]

    dataset_answer = prompt(dataset_question, style=style)

    confirmation_question = [
        {
        'type': 'confirm',
        'message': 'Should we go forward with these datasets?: {}'.format(str(dataset_answer['chosen'])[1:-1]),
        'name': 'confirmation',
        'default': True,
        }
        ]
    confirmation_answer = prompt(confirmation_question, style=style)
    
    if confirmation_answer['confirmation'] == False:
        print ('Ok, exiting...')
        quit()

    return (dataset_answer['chosen'])

if __name__ == '__main__':

    DATABASE_NAME = 'congressData.sqlite'
    BASE_URL = 'https://datos.hcdn.gob.ar:443'
    DELAY_IN_SECONDS = 0 

    try:

        datasets = obtainDecision()

        for dataset in datasets:
            if dataset == 'COVID19 Subsidies':
                SOURCE_NAME = 'Subsidies'
                resourcePath = '/api/3/action/datastore_search?resource_id=2cdaef71-f802-4067-bfa0-810dd3a22583'
            elif dataset == 'Laws':
                SOURCE_NAME = 'Laws'
                resourcePath = '/api/3/action/datastore_search?resource_id=a88b42c3-d375-4072-8542-92b11db1d711'
            elif dataset == 'House of Representatives Sessions (Diputados)':
                SOURCE_NAME = 'Sessions'
                resourcePath = '/api/3/action/datastore_search?resource_id=4ac70a51-a82d-428b-966a-0a203dd0a7e3'
            elif dataset == 'List of Representatives (Diputados)':
                SOURCE_NAME = 'Representatives'
                resourcePath = '/api/3/action/datastore_search?resource_id=16cd699d-83fb-4d5f-afd4-0af9b47b1bd7'
            runDataImporter(SOURCE_NAME, resourcePath)

    except KeyboardInterrupt:
        print ('\nUser quit with an interrupt')