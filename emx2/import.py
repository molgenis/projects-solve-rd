#///////////////////////////////////////////////////////////////////////////////
# FILE: import.py
# AUTHOR: David Ruvolo
# CREATED: 2023-05-10
# MODIFIED: 2023-05-10
# PURPOSE: import files into emx2 instance
# STATUS: in.progress
# PACKAGES: NA
# COMMENTS: NA
#///////////////////////////////////////////////////////////////////////////////

from os import environ
from emx2.api.emx2 import Molgenis as EMX2
from dotenv import load_dotenv
load_dotenv()


# connect to database
emx2 = EMX2(environ['MOLGENIS_EMX2_HOST'])
emx2.signin(environ['MOLGENIS_EMX2_USR'],environ['MOLGENIS_EMX2_PWD'])

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

emx2.add(database='BirdDataAU', table='Species', data=data)

import random
for row in data:
  row['count'] = random.randrange(100, 1000)
  row['reportingRate'] = random.random()
  row['reportingLocation'] = {'name': 'AU'}

# update
emx2.update(database='BirdDataAU', table='Species', data=data)


emx2.delete(database='BirdDataAU', table='Species', data=data)


# emx2.query(
#   database='BirdDataAU',
#   query="""
#     mutation delete($key:[SpeciesInput]) {
#       delete(Species:$key) {
#         status
#         message
#       }
#     }
#   """,
#   variables={
#     'key': [
#       {'commonName':'test-561'},
#       {'commonName':'test-562'},
#     ]
#   }
# )

# emx2.query(
#   database='RD3',
#   query="""{
#     mutation($key:[$tableInput]) {
#       delete($key:$key) {
#         status
#         message
#       }
#     }
#   }""",
#   variables={'key': 'Disease', "table":'disease'}
# )

# import ontologies


emx2.importCsvFile(database='RD3', table='subjects',file='emx2/data/subjects.csv')
