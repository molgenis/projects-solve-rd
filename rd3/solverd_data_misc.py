#///////////////////////////////////////////////////////////////////////////////
# FILE: solverd_data_misc.py
# AUTHOR: David Ruvolo
# CREATED: 2023-03-31
# MODIFIED: 2023-03-31
# PURPOSE: miscellaneous data management tasks
# STATUS: ongoing
# PACKAGES: ** see below **
# COMMENTS: NA
#///////////////////////////////////////////////////////////////////////////////

from rd3.api.molgenis2 import Molgenis
from rd3.utils.utils import flattenDataset
from dotenv import load_dotenv
from datetime import datetime
from datatable import dt, f, as_type
from os import environ
from tqdm import tqdm
load_dotenv()

rd3 = Molgenis(environ['MOLGENIS_PROD_HOST'])
rd3.login(environ['MOLGENIS_PROD_USR'], environ['MOLGENIS_PROD_PWD'])

def today():
  return datetime.now().strftime('%Y-%m-%d')


#///////////////////////////////////////////////////////////////////////////////

# ~ 0 ~
# Copy `claimed sex` to samples table
# In the subjects and samples tables, the column identifier for `claimed sex`
# is `sex1`.

# get subject metadata
subjects = rd3.get('solverd_subjects',batch_size=10000)
subjectsDT = flattenDataset(data=subjects, columnPatterns='subjectID|id|value')

subjectsDT = dt.Frame(subjectsDT)
subjectIDs = subjectsDT['subjectID'].to_list()[0]

del subjectsDT['_href']

# get samples metadata
samples = rd3.get('solverd_samples', batch_size=10000)
samplesDT = flattenDataset(data=samples,columnPatterns='subjectID|id|value')

samplesDT = dt.Frame(samplesDT)
sampleSubjectIDs = dt.unique(
  samplesDT[f.belongsToSubject != None, 'belongsToSubject']
).to_list()[0]

del samplesDT['_href']

# update claimed sex value
for id in tqdm(sampleSubjectIDs):
  currentValue = None
  if 'sex1' in samplesDT.names:
    currentValue = samplesDT[f.belongsToSubject == id, 'sex1'].to_list()[0][0]
  newValue = subjectsDT[f.subjectID == id, 'sex1'].to_list()[0][0]
  if (currentValue is None) and (newValue is not None):
    samplesDT[f.belongsToSubject == id, 'sex1'] = newValue

# del currentValue, newValue, id

# check mappings
samplesDT[:, dt.count(), dt.by('sex1')]


# set metadata columns
samplesDT['dateRecordUpdated'] = today()
samplesDT['wasUpdatedBy'] = 'rd3-bot'
samplesDT['comments'] = 'copied claimedSex from subjects table'


# make sure all bool columns as reset
# for name in samplesDT.names:
#   if str(samplesDT[name].type) == 'Type.bool8':
#     print(name)
samplesDT[:, dt.update(flag= as_type(f.flag, str))]

# import data
rd3.importDatatableAsCsv(pkg_entity='solverd_samples',data = samplesDT)

# sign out
rd3.logout()
