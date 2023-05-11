#///////////////////////////////////////////////////////////////////////////////
# FILE: test.py
# AUTHOR: David Ruvolo
# CREATED: 2023-05-11
# MODIFIED: 2023-05-11
# PURPOSE: test EMX2 script for emx2 client
# STATUS: stable
# PACKAGES: **see below**
# COMMENTS: import xlsx file into a emx2 instance
#///////////////////////////////////////////////////////////////////////////////

from emx2.api.emx2 import Molgenis as EMX2
from dotenv import load_dotenv
from os import environ
import random
load_dotenv()

# connect to database
emx2 = EMX2(environ['MOLGENIS_EMX2_HOST'])
emx2.signin(environ['MOLGENIS_EMX2_USR'],environ['MOLGENIS_EMX2_PWD'])


# create example data
data =[
  {'commonName': 'test-560'},
  {'commonName': 'test-561'},
  {'commonName': 'test-562'},
  {'commonName': 'test-563'},
  {'commonName': 'test-564'},
  {'commonName': 'test-565'},
  {'commonName': 'test-566'},
  {'commonName': 'test-567'},
  {'commonName': 'test-568'},
  {'commonName': 'test-569'},
  {'commonName': 'test-570'},
]

# import
emx2.add(database='BirdDataAU', table='Species', data=data)


# add columns
for row in data:
  row['count'] = random.randrange(100, 1000)
  row['reportingRate'] = random.random()
  row['reportingLocation'] = {'name': 'AU'}

# update
emx2.update(database='BirdDataAU', table='Species', data=data)


# get columns to pass into query
columns = [
  column['name']
  for column in emx2.getSchema(database='BirdDataAU', table='species')['columns']
  if column['name'] != 'reportingLocation'
]

# find data
export = emx2.query(
  database="BirdDataAU",
  query="""{
      Species(filter: {commonName: {like: "test"}}) {
        columns
      }
    }
  """.replace('columns', '\n      '.join(columns))
).json()['data']['Species']
export 

# delete records
emx2.delete(database='BirdDataAU', table='Species', data=data)

