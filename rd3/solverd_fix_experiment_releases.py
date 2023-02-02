#///////////////////////////////////////////////////////////////////////////////
# FILE: solverd_fix_experiment_releases.py
# AUTHOR: David Ruvolo
# CREATED: 2023-02-02
# MODIFIED: 2023-02-02
# PURPOSE: fix release associations in the labinfo table
# STATUS: stable
# PACKAGES: **see below**
# COMMENTS: Experiments were assigned to the wrong release. This script will
# fix the association by comparing experiment IDs that are in the portal table
#///////////////////////////////////////////////////////////////////////////////

from rd3.utils.utils import dtFrameToRecords
from rd3.api.molgenis import Molgenis
from dotenv import load_dotenv
from datatable import dt, f
from os import environ
load_dotenv()

# connect to RD3
rd3_prod = Molgenis(environ['MOLGENIS_PROD_HOST'])
rd3_prod.login(environ['MOLGENIS_PROD_USR'], environ['MOLGENIS_PROD_PWD'])

rd3_acc = Molgenis(environ['MOLGENIS_ACC_HOST'])
rd3_acc.login(environ['MOLGENIS_ACC_USR'], environ['MOLGENIS_ACC_PWD'])


# get data current experiments and releases
freeze = rd3_prod.get(
  entity = 'solverd_labinfo',
  attributes = 'experimentID,partOfRelease',
  batch_size=10000
)

for row in freeze:
  row['partOfRelease'] = ','.join([ d['id'] for d in row['partOfRelease'] ])

freezeDT = dt.Frame(freeze)
del freezeDT['_href']

dt.unique(freezeDT['partOfRelease'])
freezeDT[:, dt.count(), dt.by(f.partOfRelease)]

# get raw data from the portal

freezeTwo = dt.Frame(
  rd3_prod.get(
    entity = 'rd3_portal_release_freeze2',
    attributes = 'labinfo_sample,subject_id',
    batch_size=10000
  )
)[:,['labinfo_sample','subject_id']]

freezeThree = dt.Frame(
  rd3_prod.get(
    entity = 'rd3_portal_release_freeze3',
    attributes='labinfo_sample,subject_id',
    batch_size=10000
  )
)[:,['labinfo_sample','subject_id']]


# pull experiment IDs
freezeTwoIDs = freezeTwo['labinfo_sample'].to_list()[0]
freezeThreeIDs = freezeThree['labinfo_sample'].to_list()[0]

# update affiliation
freezeDT['newReleases'] = dt.Frame([
  'freeze3_original'
  if (row[1] == 'freeze2_original') and (row[0] in freezeThreeIDs) and (row[0] not in freezeTwoIDs) else row[1]
  for row in freezeDT[:, [f.experimentID, f.partOfRelease]].to_tuples()
])

# check mappings
freezeDT
dt.unique(freezeDT['newReleases'])
freezeDT[:, dt.count(), dt.by(f.partOfRelease)]
freezeDT[:, dt.count(), dt.by(f.newReleases)]

del freezeDT['partOfRelease']
freezeDT.names = { 'newReleases': 'partOfRelease' }

# import
importData = dtFrameToRecords(freezeDT)

rd3_acc.updateColumn(
  entity = 'solverd_labinfo',
  attr = 'partOfRelease',
  data = importData
)

rd3_prod.updateColumn(
  entity = 'solverd_labinfo',
  attr = 'partOfRelease',
  data = importData
)

rd3_acc.logout()
rd3_prod.logout()
