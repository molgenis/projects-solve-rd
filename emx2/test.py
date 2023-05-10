
from emx2.api import Molgenis as emx2
from os import environ
from dotenv import load_dotenv
import pandas as pd
load_dotenv()

db = emx2(url=environ['MOLGENIS_EMX2_HOST'])
db.signin(
  environ['MOLGENIS_EMX2_USR'],
  environ['MOLGENIS_EMX2_PWD']
)

schema = db.getSchema('BirdData','species')

columns = [col['name'] for col in schema['columns']]
columns = columns[:5]

query = """{
  Species {
    columns
  }
}
"""
query = query.replace('columns', '\n    '.join(columns))

data= db.query('BirdData',query)['Species']

for row in data:
  row['primaryReportingTerritories'] = 'AU-NSW'


db.importData(
  database='BirdData',
  table='species',
  data = pd.DataFrame(data).to_csv(index=False)
)