#///////////////////////////////////////////////////////////////////////////////
# FILE: data_missing_metadata.py
# AUTHOR: David Ruvolo
# CREATED: 2023-03-22
# MODIFIED: 2024-01-11
# PURPOSE: find missing metadata and import
# STATUS: stable
# PACKAGES: **see below**
# COMMENTS: NA
#///////////////////////////////////////////////////////////////////////////////

from rd3.api.molgenis2 import Molgenis
from dotenv import load_dotenv
from datatable import dt, f, fread
from datetime import datetime
from pytz import timezone
from os import environ
load_dotenv()

def current_date():
  """Get the today's date as yyyy-mm-dd"""
  return datetime.now(tz=timezone('Europe/Amsterdam')).strftime('%Y-%m-%d')

rd3 = Molgenis(environ['MOLGENIS_PROD_HOST'])
rd3.login(environ['MOLGENIS_PROD_USR'], environ['MOLGENIS_PROD_PWD'])

#///////////////////////////////////////////////////////////////////////////////

# ~ 0 ~
# Retrieve metadata

# get portal data
all_dt = dt.Frame(
  rd3.get(
    'rd3_portal_release_patches',
    q='processed==false',
    batch_size=1000
  )
)
del all_dt['_href']

# get subject info
subjects_dt = dt.Frame(
  rd3.get(
    entity='solverd_subjects',
    attributes='subjectID',
    batch_size=1000
  )
)['subjectID']

# get samples
samples_dt = dt.Frame(
  rd3.get(
    entity='solverd_samples',
    attributes='sampleID',
    batch_size=1000
  )
)['sampleID']

# get experiments
experiments_dt = dt.Frame(
  rd3.get(
    entity = 'solverd_labinfo',
    attributes='experimentID',
    batch_size=10000  
  )
)['experimentID']

# isolate IDs
subj_ids = subjects_dt['subjectID'].to_list()[0]
samp_ids = samples_dt['sampleID'].to_list()[0]
expr_ids = experiments_dt['experimentID'].to_list()[0]

#///////////////////////////////////////////////////////////////////////////////

# ~ 1 ~
# Identify missing metadata

all_dt['is_new_subject'] = dt.Frame([
  value not in subj_ids
  for value in all_dt['subject_id'].to_list()[0]
])

all_dt['is_new_sample'] = dt.Frame([
  (value not in samp_ids) and (f'VS{value}' not in samp_ids)
  for value in all_dt['samples_id'].to_list()[0]
])

all_dt['is_new_experiment'] = dt.Frame([
  value not in expr_ids
  for value in all_dt['samples_id'].to_list()[0]
])

all_dt['date_added'] = '2023-03-21'
all_dt['date_last_updated'] = current_date()
all_dt['processed'] = True

# view records
all_dt[:, dt.count(), dt.by(f.is_new_subject)]
all_dt[:, dt.count(), dt.by(f.is_new_sample)]
all_dt[:, dt.count(), dt.by(f.is_new_experiment)]
all_dt[:, dt.count(), dt.by(f.is_new_subject,f.is_new_sample)]
all_dt[:, dt.count(), dt.by(f.is_new_subject,f.is_new_sample,f.is_new_experiment)]

all_dt[(f.is_new_subject) | (f.is_new_sample), :]
all_dt[f.is_new_sample,:]
all_dt[f.is_new_experiment, :]


#///////////////////////////////////////////////////////////////////////////////

# ~ 2 ~
# Import

rd3.importDatatableAsCsv('rd3_portal_release_patches',all_dt)


# newDT = all_dt[(f.isNewSubject) | (f.isNewSample) | (f.isNewExperiment), :]
# newDT = fread('~/Desktop/rd3_portal_release_new.csv')
# rd3.delete('rd3_portal_release_new')
# rd3.importDatatableAsCsv(pkg_entity='rd3_portal_release_new', data=newDT)
