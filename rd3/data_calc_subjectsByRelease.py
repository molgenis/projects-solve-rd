#///////////////////////////////////////////////////////////////////////////////
# FILE: data_calc_subjectsByRelease.py
# AUTHOR: David Ruvolo
# CREATED: 2023-01-25
# MODIFIED: 2023-01-25
# PURPOSE: summary the number of subjects by release
# STATUS: in.progress
# PACKAGES: **see below**
# COMMENTS: NA
#///////////////////////////////////////////////////////////////////////////////

from rd3.api.molgenis2 import Molgenis
from dotenv import load_dotenv
from datatable import dt, f
from os import environ
load_dotenv()

db = Molgenis(environ['MOLGENIS_PROD_HOST'])
db.login(environ['MOLGENIS_PROD_USR'], environ['MOLGENIS_PROD_PWD'])

# get subjects
subjects = db.get(
  entity='solverd_subjects',
  attributes='subjectID,partOfRelease',
  batch_size=10000
)

# split `partOfRelease` and append to new object
subjectsDT = []
for row in subjects:
  for release in row['partOfRelease']:
    subjectsDT.append({
      'subjectID': row['subjectID'],
      'partOfRelease': release['id']
    })

subjectsDT = dt.Frame(subjectsDT)

dt.unique(subjectsDT['partOfRelease'])
subjectsDT[:, dt.count(f[:]), dt.by(f.partOfRelease)]


