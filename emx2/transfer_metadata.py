#///////////////////////////////////////////////////////////////////////////////
# FILE: transfer_metadata.py
# AUTHOR: David Ruvolo
# CREATED: 2023-05-09
# MODIFIED: 2023-05-10
# PURPOSE: import files and data into emx2 instance
# STATUS: stable
# PACKAGES: **See below**
# COMMENTS: NA
#///////////////////////////////////////////////////////////////////////////////

from emx2.api.emx2 import Molgenis as EMX2
from emx2.utils import to_csv, recodeCommaStrings
from rd3.utils.utils import flattenDataset, recodeValue
from rd3.api.molgenis2 import Molgenis
from datatable import dt, f
from dotenv import load_dotenv
from os import environ
load_dotenv()


rd3 = Molgenis(environ['MOLGENIS_PROD_HOST'])
rd3.login(environ['MOLGENIS_PROD_USR'],environ['MOLGENIS_PROD_PWD'])

emx2 = EMX2(environ['MOLGENIS_EMX2_HOST'])
emx2.signin(environ['MOLGENIS_EMX2_USR'],environ['MOLGENIS_EMX2_PWD'])

#///////////////////////////////////////////////////////////////////////////////

# ~ 1 ~
# Pull RD3 metadata

# create a subset of RD3 using the familyID of subjects with 3 or more samples
overview = rd3.get('solverd_overview',q='numberOfSamples=ge=3',num=10)
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
hpo = emx2.query(
  database='RD3',
  query="""{
    Phenotype {
      name
      code
    }
  }
  """  
)['Phenotype']

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

to_csv('emx2/data/subjects.csv',subjectsDT)
to_csv('emx2/data/subjectinfo.csv', subjectInfoDT)
to_csv('emx2/data/samples.csv', samplesDT)
to_csv('emx2/data/labinfo.csv', labinfoDT)
to_csv('emx2/data/files.csv', filesDT)
to_csv('emx2/data/overview.csv', overviewDT)

rd3.logout()