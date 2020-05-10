#!/usr/bin/env python
"""
Hay un par de convenciones respecto a como arrancar con archivos. Este
comentario en particular me parece interesante para revisar:
https://stackoverflow.com/a/9225336
basicamente sugiere arrancar con un hashbang (#!) para ayudar la ejecucion
de scripts, y luego imports separados por enters. Lo importante de imports
explicitos es que de dejan importar funciones especificas en vez de modulos
completos.
"""

# en vscode y otros, command + shift + o te optimiza imports, lo cual automaticamente
# convierte tu multi_import en single imports
import json
import sqlite3
import time
import urllib.request

# Una cosa que haria en general es usar brackets para completar el formato de strings, en vez
# de usar concatenacion. Es decir, usar 'Formatea URL {}/{} con esos parametros'.format(URL, query_params)
# Otra cosa que haria es usar variables capitalizadas o underscores consistentemente
# Para constantes, como no podes usar mayusculas, uso underscores
# Aca tu "base url" es siempre la misma, es medio pedante pero usaremos constantes para separar
# lo estatico de lo dinamico

BASE_URL = 'https://datos.hcdn.gob.ar:443/'
SOURCE_NAME = 'Leyes'


def runTablesImporter():
    """
     This module downloads the data from el Congreso Nacional de Argentina
    -> Dada una URL que escupe un JSON con 100 entradas de las Leyes tratatas en la Camara de Diputados
    -> Parsea los contenidos del JSON e identifica el siguiente recurso para seguir obteniendo informacion (son 4285 entradas)
    -> Crea una tabla (en SQLite) con el schema que aprende del JSON
    -> Por cada record, identifica si ya esta presente en la tabla, y si no lo esta, la escribe
    """
    queryTally = int()

    # This path provides initial access, the API will give a cursor to the next results
    # in the query respose
    resourcePathCursor = 'api/3/action/datastore_search?resource_id=a88b42c3-d375-4072-8542-92b11db1d711'

    resourceUrl = '{}{}'.format(BASE_URL, resourcePathCursor)
    resourceData = getJsonContents(resourceUrl)
    moreData = resourceData is not None

    # Tema de estilo tambien, en este caso prefiero controlar por un flag booleano en vez del recurso directamente
    while moreData:

        # Catch failures on their backend
        if resourceData['success'] == True:
            # Count total number of records in dataset and in this particular resultset
            queryResults = len(resourceData['result']['records'])
            queryTotal = resourceData['result']['total']
            print ('Found {} results in this page, and a total of {} for this dataset'.format(queryResults, queryTotal))

            # =====================
            # Write on the database
            # =====================
            # Retrieve the schema and helper strings
            fullSchema = str() # needed because I'm building on schema
            fieldTypes = dict() # this dictionary stores each field and the type, which we'll use when doing the WHERE clause
            for field in resourceData['result']['fields']:

                # Nota: Podes usar format aca.
                # Aca me gusta como estas completando FieldType. para fullSchema podrias armar
                # una funcioncita. Si bien vas a terminar iterando sobre el schema nuevamente
                # (y podrias argumentar que no es optimo), aca el codigo se esta mezclando mucho.
                # Ojo, la variable schema no la usaste para nada asi que la borre

                fullSchema = fullSchema + ', ' + field['id'] + ' ' + field['type']
                fieldTypes[field['id']] = field['type']

            fullSchema = fullSchema[1:]

            # Create table with the fullSchema collected before
            database = sqlite3.connect('congreso.sqlite')
            cursor = database.cursor()
            sql = 'CREATE TABLE IF NOT EXISTS ' + SOURCE_NAME + ' (' + fullSchema + ')'
            cursor.execute(sql)

            # Loop through records to insert into the database
            for record in resourceData['result']['records']:
                # helper values
                fields = (str(list(record.keys()))[1:-1])
                values = (str(list(record.values()))[1:-1])
                conditions = buildQueryCondition(record, fieldTypes)

                # See if the record is present, and if so, skip
                sql = 'SELECT ROWID FROM ' + SOURCE_NAME + ' WHERE ' + conditions
                try:
                    cursor.execute(sql)
                except:
                    print ('Problem running query:', sql)

                if cursor.fetchone() is None:
                    sql = 'INSERT INTO ' + SOURCE_NAME + ' (' + fields + ') VALUES (' + values + ')'
                    cursor.execute(sql)
                    print ('Added 1 record')
                else:
                    print ('Skipped one record')
        
            database.commit()

            # Establish progress
            queryTally = queryTally + queryResults
            # Aca pongo espacio entre operadores
            print ('We are ' + str(round((queryTally / queryTotal) * 100)) + '% of the way there!')
            print ('=' * 30)

            if queryResults > 0:

                resourcePathCursor = None
                if resourceData.get('result') is not None:
                    if resourceData['result'].get('_links') is not None:
                        resourcePathCursor = resourceData['result']['_links']['next']

                if resourcePathCursor is not None:
                    resourceUrl = '{}{}'.format(BASE_URL, resourcePathCursor)
                    resourceData = getJsonContents(resourceUrl)
                    moreData = resourceData is not None
                else:
                    moreData = False
            else:
                print('Done here!')
                moreData = False


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
    """
    this complicated function tries to assemble the list of conditions for the WHERE clause,
    since I can't use qmark given I want to dynamically generate both the field names and their values

    Aca movi la funcion fuera del codigo, fijate que "fieldTypes" estaba dentro del 
    scope de tu definicion, por lo tanto no necesitabas pasarlo como parametro. No es una
    muy buena practica depender de scoping para encontrar variables - incluso alguna gente
    se ofende bastante con variables globales (en general no tenes que darles bola). En este
    caso puntual, mi preferencia es ser explicito en las variables a las que la funcion tiene acceso,
    en este caso requeris 2 cosas: record y fieldTypes

    OJO! Nota:
    Un tema bastante CHOTO de esta funcion (o este codigo) es la creacion de SQL dinamico
    usando concatenacion de strings. Te expone a lo que se llama SQL Injection Attack, el famoso
    chiste de XKCD: https://xkcd.com/327/.  Fijate si podes refactorizarlo completamente
    """
    conditions = str()
    for fieldName in record.keys():
        if fieldTypes[fieldName].lower() == 'text':
            conditions = conditions + ' AND ' + fieldName + ' = "' + record[fieldName] + '"'
        else:
            conditions = conditions + ' AND ' + fieldName + ' = ' + str(record[fieldName]) + ''
    return conditions[5:]

if __name__ == '__main__':
    runTablesImporter()
