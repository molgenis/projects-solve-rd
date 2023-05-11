#///////////////////////////////////////////////////////////////////////////////
# FILE: emx2_setup.py
# AUTHOR: David Ruvolo
# CREATED: 2023-05-09
# MODIFIED: 2023-05-09
# PURPOSE: import files and data into emx2 instance
# STATUS: in.progress
# PACKAGES: **See below**
# COMMENTS: NA
#///////////////////////////////////////////////////////////////////////////////

from rd3.utils.utils import flattenDataset, recodeValue
from rd3.api.emx2.emx2 import Molgenis as EMX2
from rd3.api.molgenis2 import Molgenis
from datatable import dt, f, as_type
from os import environ, path,listdir
from dotenv import load_dotenv
from csv import QUOTE_ALL
from tqdm import tqdm
load_dotenv()

def toTextCsv(data):
  return data.to_pandas().to_csv(index=False,encoding='UTF-8',quoting=QUOTE_ALL)

def recodeCommaStrings(mappings,value):
  codes = value.split(',')
  terms = []
  for code in codes:
    value = recodeValue(mappings, code,'HPO')
    terms.append(value)
  return ','.join(terms)


rd3 = Molgenis(environ['MOLGENIS_PROD_HOST'])
rd3.login(environ['MOLGENIS_PROD_USR'],environ['MOLGENIS_PROD_PWD'])

emx2 = EMX2(environ['MOLGENIS_EMX2_HOST'])
emx2.signin(environ['MOLGENIS_EMX2_USR'],environ['MOLGENIS_EMX2_PWD'])

#///////////////////////////////////////////////////////////////////////////////

# ~ 1 ~
# Import Lookup Files

filesToIgnore=['rd3_emx2.xlsx','SEMANTICS.csv','~$rd3_emx2.xlsx']
files = [f"emx2/{f}" for f in listdir('emx2/') if f not in filesToIgnore]

for file in tqdm(files):
  emx2.importCsvFile(
    database='RD3',
    table=path.basename(file).split('.')[0],
    file=file
  )

#///////////////////////////////////////////////////////////////////////////////

# ~ 2 ~
# Pull data from RD3

# ~ 2a ~
# Pull solve-rd lookups

# organisations
organisations=dt.Frame(rd3.get('solverd_info_organisations'))[:, {
  'name': f.value,
  'label': f.value,
  'codesystem': f.codesystem,
  'code': f.code,
  'ontolotgyTermURI': f.iri
}]

emx2.importData(database='RD3', table='organisations',data=toTextCsv(organisations))

# persons
persons = dt.Frame(
  flattenDataset(
    data=rd3.get('solverd_info_persons'),
    columnPatterns='value'
  )
)
persons.names = {'id': 'name'}

emx2.importData(database='RD3',table='persons',data=toTextCsv(persons))


# datareleases
datareleases = rd3.get('solverd_info_datareleases')
datareleasesDT = dt.Frame(flattenDataset(datareleases,'id'))
datareleasesDT.names = {'name': 'label', 'id': 'name'}

emx2.importData(database='RD3',table='datareleases',data=toTextCsv(datareleasesDT))

# library information
library = dt.Frame(rd3.get('solverd_lookups_library'))[:, {
  'name': f.id,
  'libraryLayoutId': f.libraryLayoutId
}]

emx2.importData(database='RD3', table='library',data=toTextCsv(library))

#///////////////////////////////////////

# ~ 2b ~
# Pull RD3 metadata

# create a subset of RD3 using the familyID of subjects with 3 or more samples
overview = rd3.get('solverd_overview',q='numberOfSamples=ge=3',num=1)
overviewDT = dt.Frame(flattenDataset(overview,'id|subjectID|experimentID|value|sampleID'))
ids = overviewDT['fid'].to_list()[0]

# ~ 2b.i ~
# pull metadata from subjects and subjectinfo
fidQuery = ','.join([f"fid=q={id}" for id in ids])
subjects = rd3.get(entity='solverd_subjects', q=f"({fidQuery})")

subjectsDT = dt.Frame(flattenDataset(subjects,columnPatterns='subjectID|id|value'))

subjectsQuery = ','.join([
  f"subjectID=q={id}" for id in subjectsDT['subjectID'].to_list()[0]
])

subjectInfo = rd3.get(entity='solverd_subjectinfo',q=subjectsQuery)
subjectInfoDT = dt.Frame(flattenDataset(subjectInfo,'subjectID|id'))

# ~ 2b.ii ~
# pull samples metadata
sampleQuery=','.join([
  f"belongsToSubject=q={id}" for id in subjectsDT['subjectID'].to_list()[0]
])

samples = rd3.get(entity='solverd_samples',q=sampleQuery)
samplesDT = dt.Frame(flattenDataset(samples,'subjectID|id|value'))


# ~ 2b.iii ~
# Pull experiments metadata

labinfoQuery = ','.join([
  f"sampleID=q={id}" for id in samplesDT['sampleID'].to_list()[0]
])

labinfo = rd3.get('solverd_labinfo', q=labinfoQuery)
labinfoDT = dt.Frame(flattenDataset(labinfo,'sampleID|id'))


# ~ 2b.iv ~
# Pull file metadtaa

files=rd3.get('solverd_files',q=subjectsQuery)
filesDT=dt.Frame(flattenDataset(files,'subjectID|sampleID|experimentID|id'))

filesDT['missingExperiment'] = dt.Frame([
  id not in labinfoDT['experimentID'].to_list()[0]
  for id in filesDT['experimentID'].to_list()[0]
])

filesDT['missingSample'] = dt.Frame([
  id not in samplesDT['sampleID'].to_list()[0]
  for id in filesDT['sampleID'].to_list()[0]
])

# drop rows where sample was not included in the export
filesDT[:, dt.count(), dt.by(f.missingSample)]
filesDT[:, dt.count(), dt.by(f.missingExperiment)]

filesDT=filesDT[f.missingSample!=True,:]

#///////////////////////////////////////

# ~ 2c ~
# Recode ERNs
ernMappings = {
  'ern_euro_nmd': 'ERN EURO-NMD',
  'ern_rnd': 'ERN-RND', 
}

subjectsDT['ERN'] = dt.Frame([
  recodeValue(mappings=ernMappings, value = value, label='ERN')
  if value else value
  for value in subjectsDT['ERN'].to_list()[0]
])

samplesDT['ERN'] = dt.Frame([
  recodeValue(mappings=ernMappings, value = value, label='ERN')
  if value else value
  for value in samplesDT['ERN'].to_list()[0]
])

overviewDT['ERN'] = dt.Frame([
  recodeValue(mappings=ernMappings, value = value, label='ERN')
  if value else value
  for value in overviewDT['ERN'].to_list()[0]
])


# ~ 2d ~
# Recode phenotypes

query="""{
  Phenotype {
    name
    code
  }
}
"""
hpo = emx2.query(database='RD3', query=query)['Phenotype']
hpoMappings = {}
for entry in hpo:
  hpoMappings[entry['code']] = entry['name'] 


subjectsDT['phenotype'] = dt.Frame([
  recodeCommaStrings(hpoMappings,value) if value else value
  for value in subjectsDT['phenotype'].to_list()[0]
])

subjectsDT['hasNotPhenotype'] = dt.Frame([
  recodeCommaStrings(hpoMappings,value) if value else value
  for value in subjectsDT['hasNotPhenotype'].to_list()[0]
])

if 'ageOfOnset' in subjectInfoDT.names:
  subjectInfoDT['ageOfOnset'] = dt.Frame([
    recodeCommaStrings(hpoMappings,value) if value else value
    for value in subjectInfoDT['ageOfOnset'].to_list()[0]
  ])
  
  
overviewDT['phenotype'] = dt.Frame([
  recodeCommaStrings(hpoMappings,value) if value else value
  for value in overviewDT['phenotype'].to_list()[0]
])

overviewDT['hasNotPhenotype'] = dt.Frame([
  recodeCommaStrings(hpoMappings,value) if value else value
  for value in overviewDT['hasNotPhenotype'].to_list()[0]
])

# ~ 2e ~
# Recode Diseases
disease = emx2.query(
  database='RD3',
  query="""{
    Disease {
      name
      label
    }
  }
  """
)['Disease']

diseaseMappings={}
for entry in disease:
  diseaseMappings[entry['name']] = entry['label']
  

subjectsDT['disease'] = dt.Frame([
  recodeCommaStrings(diseaseMappings, value) if value else value
  for value in subjectsDT['disease'].to_list()[0]
])

overviewDT['disease'] = dt.Frame([
  recodeCommaStrings(diseaseMappings, value) if value else value
  for value in overviewDT['disease'].to_list()[0]
])

#///////////////////////////////////////

# ~ 3 ~
# Import data

subjects_str = toTextCsv(subjectsDT)
subjectinfo_str = toTextCsv(subjectInfoDT)

emx2.importData(database='RD3',table='subjects', data=subjects_str)
emx2.importData(database='RD3',table='subjectinfo', data=subjectinfo_str)