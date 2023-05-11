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


# import all files
files = ['subjects','subjectinfo', 'samples', 'labinfo', 'files', 'overview']
for file in files:
  path = f"emx2/data/{file}.csv"
  emx2.importCsvFile(database='RD3', table=files, file=path)


#///////////////////////////////////////

# MISC

# reimport disease
response = emx2.query(
  database='RD3',
  query="""{
    Disease {
      name
    }
  }
  """
)
data = response.json()['data']['Disease']

for batch in range(0, len(data), 1000):
  emx2.delete(
    database='RD3',
    table='Disease',
    data = data[batch:batch+1000]
  )
  
emx2.importCsvFile(database='RD3',table='disease', file='emx2/ontologies/disease.csv')