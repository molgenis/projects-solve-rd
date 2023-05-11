#///////////////////////////////////////////////////////////////////////////////
# FILE: import.py
# AUTHOR: David Ruvolo
# CREATED: 2023-05-10
# MODIFIED: 2023-05-11
# PURPOSE: import files into emx2 instance
# STATUS: stable
# PACKAGES: NA
# COMMENTS: NA
#///////////////////////////////////////////////////////////////////////////////

from emx2.api.emx2 import Molgenis as EMX2
from os import environ,path, listdir
from dotenv import load_dotenv
load_dotenv()

# connect to database
emx2 = EMX2(environ['MOLGENIS_EMX2_HOST'])
emx2.signin(environ['MOLGENIS_EMX2_USR'],environ['MOLGENIS_EMX2_PWD'])

#///////////////////////////////////////

# ~ 1 ~
# import all files

# import ontologies
ontologies = listdir('emx2/ontologies')
for file in ontologies:
  emx2.importCsvFile(
    database='RD3',
    table=file.replace('.csv',''),
    file=f"emx2/ontologies/{file}"
  )


# import RD3 specific ontologies (note order!!)
for file in ['organisations_2', 'persons', 'datareleases', 'library']:
  emx2.importCsvFile(
    database='RD3',
    table=file.replace('_2', ''),
    file=f"emx2/data/{file}.csv"
  )


# import RD3 metadata
for file in ['subjects','subjectinfo', 'samples', 'labinfo', 'files', 'overview']:
  emx2.importCsvFile(
    database='RD3',
    table=file,
    file=f"emx2/data/{file}.csv"
  )


#///////////////////////////////////////

# MISC

# update subjectinfo references
from datatable import dt, f
import numpy as np

subjectsDT = dt.Frame(
  emx2.session.get(
    url=f"{emx2.host}/RD3/api/csv/subjects"
  ).text 
)

subjectInfoDT = dt.Frame(
  emx2.session.get(
    url=f"{emx2.host}/RD3/api/csv/subjectinfo"
  ).text
)

subjectsDT['subjectInformation'] = dt.Frame([
  value if value in subjectInfoDT['subjectID'].to_list()[0] else None
  for value in subjectsDT['subjectID'].to_list()[0]
])

importData = subjectsDT[:, (f.subjectID,f.subjectInformation)] \
  .to_pandas() \
  .replace({np.nan: None}) \
  .to_dict('records')

for row in importData:
  row['subjectInformation'] = {'subjectID': row['subjectInformation']}

emx2.update(
  database='RD3',
  table='Subjects',
  data = importData
)



# reimport disease
# response = emx2.query(
#   database='RD3',
#   query="""{
#     Disease {
#       name
#     }
#   }
#   """
# )
# data = response.json()['data']['Disease']

# for batch in range(0, len(data), 1000):
#   emx2.delete(
#     database='RD3',
#     table='Disease',
#     data = data[batch:batch+1000]
#   )
  
# emx2.importCsvFile(database='RD3',table='disease', file='emx2/ontologies/disease.csv')