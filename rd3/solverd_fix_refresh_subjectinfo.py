#///////////////////////////////////////////////////////////////////////////////
# FILE: solverd_fix_refresh_subjectinfo.py
# AUTHOR: David Ruvolo
# CREATED: 2023-02-02
# MODIFIED: 2023-02-02
# PURPOSE: refresh subjectinfo table
# STATUS: in.progress
# PACKAGES: **see below**
# COMMENTS: Sometimes the SubjectInfo can become outdated. 
#///////////////////////////////////////////////////////////////////////////////

from rd3.api.molgenis2 import Molgenis
from rd3.utils.utils import timestamp
from dotenv import load_dotenv
from datatable import dt, f
from os import environ
from tqdm import tqdm
load_dotenv()

# Connect to databases
rd3_acc = Molgenis(environ['MOLGENIS_ACC_HOST'])
rd3_acc.login(environ['MOLGENIS_ACC_USR'], environ['MOLGENIS_ACC_PWD'])

rd3_prod = Molgenis(environ['MOLGENIS_PROD_HOST'])
rd3_prod.login(environ['MOLGENIS_PROD_USR'], environ['MOLGENIS_PROD_PWD'])


# pull subjects
subjects = rd3_prod.get(
  entity = 'solverd_subjects',
  attributes = 'subjectID,partOfRelease',
  batch_size=10000
)

# flatten releases
for row in subjects:
  row['partOfRelease'] = ','.join([ d['id'] for d in row['partOfRelease'] ])

# convert to datatable object
subjects = dt.Frame(subjects)
del subjects['_href']

# pull IDs in subjectInfo table
subjectInfoIDs = dt.Frame(
  rd3_prod.get(
    entity = 'solverd_subjectinfo',
    attributes = 'subjectID',
    batch_size=10000
  )
)['subjectID']

# determine which IDs are missing from the `subjectinfo` table
subjects['isMissing'] = dt.Frame([
  not (id in subjectInfoIDs['subjectID'].to_list()[0])
  for id in tqdm(subjects['subjectID'].to_list()[0])
])

# how many are missing?
subjects[:, dt.count(), dt.by(f.isMissing)]
subjects[f.isMissing,:]

# get subjects to import into subjectInfo
missingSubjects = subjects[f.isMissing, (f.subjectID, f.partOfRelease)]
missingSubjects['dateRecordCreated'] = timestamp()
missingSubjects['recordCreatedBy'] = 'rd3-bot'


# import missing subjects
rd3_acc.importDatatableAsCsv(pkg_entity='solverd_subjectinfo', data = missingSubjects)
rd3_prod.importDatatableAsCsv(pkg_entity='solverd_subjectinfo', data = missingSubjects)
