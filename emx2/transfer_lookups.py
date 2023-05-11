#///////////////////////////////////////////////////////////////////////////////
# FILE: emx2.py
# AUTHOR: David Ruvolo
# CREATED: 2023-05-08
# MODIFIED: 2023-05-08
# PURPOSE: set up EMX2 instance
# STATUS: in.progress
# PACKAGES: **see below**
# COMMENTS: NA
#///////////////////////////////////////////////////////////////////////////////

from rd3.api.molgenis2 import Molgenis
from rd3.api.emx2.emx2 import Molgenis as EMX2
from dotenv import load_dotenv
from datatable import dt, f, as_type
from os import environ,path
import requests
import csv
load_dotenv()

def toTextCsv(data):
  return data.to_pandas() \
    .to_csv(
      index=False,
      encoding='utf-8',
      quoting=csv.QUOTE_ALL
    )

rd3 = Molgenis(environ['MOLGENIS_PROD_HOST'])
rd3.login(environ['MOLGENIS_PROD_USR'],environ['MOLGENIS_PROD_PWD'])

emx2 = EMX2(environ['MOLGENIS_EMX2_HOST'])
emx2.signin(environ['MOLGENIS_EMX2_USR'],environ['MOLGENIS_EMX2_PWD'])

#///////////////////////////////////////////////////////////////////////////////

# ~ 1 ~
# Transfer Lookup tables


# ~ 1a ~
# transfer: solverd::anatomicallocation --> RD3::anatomicallocation
anatomicallocation = dt.Frame(
  rd3.get(
    'solverd_lookups_anatomicallocation',
    batch_size=10000
  )
)[:, {
  'name': f.id,
  'label': f.label,
  'codesystem': f.ontology,
  'code': f.id,
  'ontologyTermURI': f.uri
}]

anatomicallocation[:, dt.update(name=as_type(f.name,int))]
anatomicallocation=anatomicallocation[:, :, dt.sort(f.name)]
anatomicallocation[:, dt.update(name=as_type(f.name,str))]


emx2.importData(
  database='RD3',
  table='anatomicallocation',
  data=toTextCsv(data=anatomicallocation)
)

#///////////////////////////////////////

# ~ 1b ~
# transfer: solverd::dataUseConditions --> RD3::datauseconditions
datauseconditions = dt.Frame(
  rd3.get('solverd_lookups_dataUseConditions')
)[:, {
  'name': f.id,
  'label': f.label,
  'definition': f.description
}]

datauseconditions['label'] = dt.Frame([
  value.replace('�','') if value else value
  for value in datauseconditions['label'].to_list()[0]
])

datauseconditions['definition'] = dt.Frame([
  value.replace('�','') if value else value
  for value in datauseconditions['definition'].to_list()[0]
])


emx2.importData(
  database='RD3',
  table='datauseconditions',
  data=toTextCsv(data=datauseconditions)
)

#///////////////////////////////////////

# ~ 1c ~
# transfer: solverd_lookups::diseases --> RD3::disease

diseases = rd3.get('solverd_lookups_disease', batch_size=10000)

for row in diseases:
  if bool(row['parentId']):
    row['parentId'] = ','.join([entry['id'] for entry in row['parentId']])
  else:
    row['parentId'] = None

diseasesDT = dt.Frame(diseases)[:, {
  'name': f.id,
  'label': f.label,
  'codesystem': f.ontology,
  'code': f.id,
  'parent': f.parentId,
  'ontologyTermURI': f.uri
}]

for column in diseasesDT.names:
  diseasesDT[column] = dt.Frame([
    value.replace('�','') if value else value
    for value in diseasesDT[column].to_list()[0]
  ])

# there are duplicate values in the diseases table
diseasesDT['name'] = dt.Frame([
  value.strip() if value else value
  for value in diseasesDT['name'].to_list()[0]
])

diseasesDT['isDuplicate'] = dt.Frame([
  diseasesDT[f.name==value,:].nrows > 1
  for value in diseasesDT['name'].to_list()[0]
])

# diseasesDT[:, dt.count(), dt.by(f.isDuplicate)]
diseasesDT = diseasesDT[:, dt.first(f[:]), dt.by(f.name)]


# make sure all urls are formated correctly
diseasesDT[dt.re.match(f.ontologyTermURI, '.*,.*'),:]
diseasesDT['ontologyTermURI'] =dt.Frame([
  value.split(',')[0] if value else value
  for value in diseasesDT['ontologyTermURI'].to_list()[0]
])

# for now, drop the parent column all column values are treated like a single
# string
del diseasesDT['parent']

# import in two steps: first all terms and then the parent IDs
emx2.importData(
  database='RD3',
  table='disease',
  data=toTextCsv(data=diseasesDT)
)

# emx2.importData(
#   database='RD3',
#   table='disease',
#   data=toTextCsv(data=diseasesDT)
# )

#///////////////////////////////////////

# ~ 1d ~
# transfer: solverd_lookups::typeFile -> RD3::fileformats

formats = dt.Frame(rd3.get('solverd_lookups_typeFile'))[:, {
  'name': f.id,
  'label': f.label,
  'definition': f.description,
  'codesystem': f.codesystem,
  'code': f.code,
  'ontologyTermURI': f.iri
}]

emx2.importData(
  database='RD3',
  table='fileformat',
  data=toTextCsv(data=formats)
)


#///////////////////////////////////////

# ~ 1e ~
# transfer: solverd_lookups::libraryType --> RD3:: librarytype

library = dt.Frame(rd3.get('solverd_lookups_libraryType'))[:, {
  'name': f.id
}]

emx2.importData(
  database='RD3',
  table='librarytype',
  data=toTextCsv(data=library)
)


#///////////////////////////////////////

# ~ 1f ~
# transer: solverd_lookups::materialType --> RD3::materialtype

material = dt.Frame(rd3.get('solverd_lookups_materialType'))[:, {
  'name': f.id,
  'label': f.label,
  'codesystem': f.source,
  'definition': f.description,
  'ontologyTermURI': f.uri
}]

emx2.importData(
  database='RD3',
  table='materialtype',
  data=toTextCsv(data=material)
)

#///////////////////////////////////////

# ~ 1g ~
# transfer: solverd::noyesunknown --> RD3::noyesunknown
noyesunknown = dt.Frame(rd3.get('solverd_lookups_noyesunknown'))[:,{
  'name': f.id,
  'label': f.label,
  'codesystem': f.codesystem,
  'code': f.code,
  'ontologyTermURI': f.iri,
  'definition': f.description
}]

emx2.importData(
  database='RD3',
  table='noyesunknown',
  data=toTextCsv(data=noyesunknown)
)


#///////////////////////////////////////

# ~ 1h ~
# create erns
# I submitted the ERN data to the ROR ontology it is possible to query the ERNs
# by calling the parent organisation "ERN Board of Member States", and then
# extracting the child organisations

# get parent organisation
response = requests.get('https://api.ror.org/organizations/00r7apq26')
rawdata = response.json()
data = [{
  'label': rawdata['name'],
  'id': rawdata['id'],
  'type': 'index'
}]

for row in rawdata.get('relationships'):
  data.append(row)

organisations = dt.Frame(data)[:, {
  'name': f.label,
  'label': f.label,
  'codesystem': 'ROR',
  'ontologyTermURI': f.id,
  'type': f.type
}]

organisations['code'] = dt.Frame([
  path.basename(value)
  for value in organisations['ontologyTermURI'].to_list()[0]
])

# set parent relationships
organisations['parent'] = dt.Frame([
  organisations[f.type=='index','name'].to_list()[0][0] if value == 'Child' else (
    organisations[f.type=='Parent','name'].to_list()[0][0] if value == 'index' else None
  )
  for value in organisations['type'].to_list()[0]
])

# set definition using other name
for value in organisations['code'].to_list()[0]:
  response = requests.get(f"https://api.ror.org/organizations/{value}")
  if bool(response.json().get('aliases')):
    organisations[f.code==value,'definition'] = ','.join(response.json().get('aliases'))

emx2.importData(
  database='RD3',
  table='organisations',
  data=toTextCsv(data=organisations)
)

#///////////////////////////////////////

# ~ 1i ~
# transfer: solverd_lookups::pathologicalstate --> RD3::pathologicalstate

pathology = dt.Frame(rd3.get('solverd_lookups_pathologicalstate'))[:,{
  'name': f.value,
  'label': f.value,
  'definition': f.description,
  'code': f.code,
  'codesystem': f.codesystem,
  'ontologyTermURI': f.iri
}]

emx2.importData(
  database='RD3',
  table='pathologicalstate',
  data=toTextCsv(data=pathology)
)

#///////////////////////////////////////

# ~ 1J ~
# transfer: solverd_lookups::phenotype --> RD3::phenotype

# for now, discard parents until ref_array is implemented
phenotype = dt.Frame(
  rd3.get(
    'solverd_lookups_phenotype',
    attributes='id,label,description,uri',
    batch_size=10000
  )
)[:, {
  'name': f.label,
  'label': f.label,
  'codesystem': 'HPO',
  'code': f.id,
  'definition': f.description
}]

for column in phenotype.names:
  phenotype[column] = dt.Frame([
    value.replace('�','') if value else value
    for value in phenotype[column].to_list()[0]
  ])

emx2.importData(
  database='RD3',
  table='phenotype',
  data=toTextCsv(data=phenotype)
)

#///////////////////////////////////////

# ~ 1K ~
# transfer: solverd_lookups::seqType --> RD3::seqtype

seqtype = dt.Frame(rd3.get('solverd_lookups_seqType'))[:,{
  'name': f.id,
  'label': f.label
}]

for column in seqtype.names:
  seqtype[column] = dt.Frame([
    value.replace('�','') if value else value
    for value in seqtype[column].to_list()[0]
  ])

emx2.importData(database='RD3',table='seqtype',data=toTextCsv(seqtype))

#///////////////////////////////////////

# ~ 1L ~
# transfer: solverd_lookups_sex --> RD3::sex

sex = dt.Frame(rd3.get('solverd_lookups_sex'))[:, {
  'name': f.id,
  'label': f.label,
  'definition': f.description
}]

emx2.importData(database='RD3', table='sex', data=toTextCsv(sex))

#///////////////////////////////////////

# ~ 1M ~
# transfer: solverd_lookups_tissueType --> RD3::tissuetype

tissue = dt.Frame(rd3.get('solverd_lookups_tissueType'))[:, {
  'name': f.id
}]

emx2.importData(database='RD3',table='tissuetype',data=toTextCsv(tissue))


#///////////////////////////////////////////////////////////////////////////////

# ~ 2 ~
# Save all files

anatomicallocation.to_csv('emx2/anatomicallocation.csv')
datauseconditions.to_csv('emx2/datauseconditions.csv')
diseasesDT.to_csv('emx2/disease.csv')
formats.to_csv('emx2/fileformat.csv')
library.to_csv('emx2/librarytype.csv')
material.to_csv('emx2/materialtype.csv')
noyesunknown.to_csv('emx2/noyesunknown.csv')
organisations.to_csv('emx2/organisations.csv')
pathology.to_csv('emx2/pathologicalstate.csv')
phenotype.to_csv('emx2/phenotype.csv')
seqtype.to_csv('emx2/seqtype.csv')
sex.to_csv('emx2/sex.csv')
tissue.to_csv('emx2/tissuetype.csv')
