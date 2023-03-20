#///////////////////////////////////////////////////////////////////////////////
# FILE: data_stats.py
# AUTHOR: David Ruvolo
# CREATED: 2023-01-31
# MODIFIED: 2023-01-31
# PURPOSE: calculate stats on RD3
# STATUS: in.progress
# PACKAGES: **see below**
# COMMENTS: NA
#///////////////////////////////////////////////////////////////////////////////

from rd3.api.molgenis2 import Molgenis
from rd3.utils.utils import flattenDataset
from dotenv import load_dotenv
from datatable import dt, f
from os import environ
load_dotenv()

rd3 = Molgenis(environ['MOLGENIS_PROD_HOST'])
rd3.login(environ['MOLGENIS_PROD_USR'], environ['MOLGENIS_PROD_PWD'])

#///////////////////////////////////////

# ~ 1 ~
# How many subjects, samples, experiments, and files are there?

subjectsDT = dt.Frame(
  rd3.get(
    entity='solverd_subjects',
    attributes='subjectID',
    batch_size=10000
  )
)['subjectID']

samplesDT = dt.Frame(
  rd3.get(
    entity='solverd_samples',
    attributes='sampleID',
    batch_size=10000
  )
)['sampleID']

experimentsDT = dt.Frame(
  rd3.get(
    entity='solverd_labinfo',
    attributes='experimentID',
    batch_size=10000
  )
)['experimentID']

filesDT = dt.Frame(
  rd3.get(
    entity='solverd_files',
    attributes='EGA',
    batch_size=10000
  )
)['EGA']

# we don't need to use dt.unique as IDs are already unique :-p
counts = [
  { 'type': 'subjects', 'count': subjectsDT.nrows },
  { 'type': 'samples', 'count': samplesDT.nrows },
  { 'type': 'experiments', 'count': experimentsDT.nrows },
  { 'type': 'files', 'count': filesDT.nrows },
]

countsDT = dt.Frame(counts)

#///////////////////////////////////////////////////////////////////////////////

# ~ 2 ~
# Types of Sequences

experiments = rd3.get(
  entity = 'solverd_labinfo',
  attributes = 'experimentID,seqType,partOfRelease',
  batch_size = 10000
)

for row in experiments:
  if row['seqType']:
    row['seqType'] = ','.join([ d['id'] for d in row['seqType'] ])
  else:
    row['seqType'] = None
    
  if row['partOfRelease']:
    row['partOfRelease'] = ','.join([ d['id'] for d in row['partOfRelease'] ])
  else:
    row['partOfRelease'] = None

experimentsDT = dt.Frame(experiments)
del experimentsDT['_href']


dt.unique(experimentsDT[dt.re.match(f.partOfRelease, 'freeze1.*|freeze2.*|freeze3.*'),'seqType'])


#///////////////////////////////////////////////////////////////////////////////

# ~ 3 ~
# Summary

# Most common phenotypes
hpo = rd3.get(
  entity='solverd_subjects',
  attributes='subjectID,phenotype',
  batch_size=10000
)

hpoCodes = []
for row in hpo:
  if row['phenotype']:
    codes = row['phenotype'].split(',')
    for code in codes:
      hpoCodes.append(code)

hpoDT = dt.Frame(codes=hpoCodes)
hpoDT[:, dt.count(), dt.by(f.codes)][:, :, dt.sort(f.count)]


# Most common data providers
providers = rd3.get(
  entity='solverd_samples',
  attributes='ERN,organisation',
  batch_size=1000
)

providersDT = dt.Frame(flattenDataset(data=providers, columnPatterns='id|value'))
providersDT[:, dt.count(), dt.by(f.ERN)][:, :, dt.sort(f.count, reverse=True)]

#///////////////////////////////////////////////////////////////////////////////


# ~ 4 ~
# Summary: Count of records by ERN

ern = rd3.get('solverd_subjects', attributes='ERN',batch_size=10000)
ernDT = dt.Frame(flattenDataset(ern, 'id'))

ernDT[:, dt.count(), dt.by(f.ERN)]