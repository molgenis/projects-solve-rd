#///////////////////////////////////////////////////////////////////////////////
# FILE: rd3_ped_02_validation.py
# AUTHOR: David Ruvolo
# CREATED: 2022-09-21
# MODIFIED: 2023-02-02
# PURPOSE: Validate new PED data
# STATUS: stable
# PACKAGES: datatable, os, tqdm, dotenv
# COMMENTS: NA
#///////////////////////////////////////////////////////////////////////////////

from rd3.api.molgenis2 import Molgenis
from dotenv import load_dotenv
from datatable import dt, f, fread, as_type
from os import environ
from tqdm import tqdm
load_dotenv()

rd3_prod = Molgenis(environ['MOLGENIS_PROD_HOST'])
rd3_prod.login(environ['MOLGENIS_PROD_USR'], environ['MOLGENIS_PROD_PWD'])

rd3_acc = Molgenis(environ['MOLGENIS_ACC_HOST'])
rd3_acc.login(environ['MOLGENIS_ACC_USR'], environ['MOLGENIS_ACC_PWD'])

#///////////////////////////////////////////////////////////////////////////////

# ~ 0 ~
# Get data from RD3

# get subject patch information to join later on
subjects = rd3_prod.get(
  entity = 'solverd_overview',
  attributes='subjectID,partOfRelease',
  batch_size=10000
)

# flatten nested attribute
for row in subjects:
  if row['partOfRelease']:
    row['partOfRelease'] = ','.join([ r['id'] for r in row['partOfRelease'] ])
  else: 
    row['partOfRelease'] = None
    

subjects = dt.Frame(subjects)[:,['subjectID','partOfRelease']]
subjectIDs = dt.unique(subjects['subjectID']).to_list()[0]

# Get sex codes
sexcodes = dt.Frame(rd3_prod.get('solverd_lookups_sex'))['id'].to_list()[0]

#//////////////////////////////////////////////////////////////////////////////

# ~ 1 ~
# Validate extracted PED data and update records
# Pull data from the data from the rd3_portal. Validate subject identifier and
# merge release data.

pedDT = dt.Frame(rd3_prod.get(entity='rd3_portal_cluster_ped', batch_size = 10000))
del pedDT['_href']

# ~ 1a ~
# Check to see if all subject identifiers exist in RD3
pedDT['subjectExists'] = dt.Frame([
  id in subjectIDs for id in tqdm(pedDT['subjectID'].to_list()[0])
])

pedDT[:, dt.count(), dt.by(f.subjectExists)]

# ~ 1b ~
# Merge subject rleases associated with each subject
subjects.key = 'subjectID'
pedDT = pedDT[:, :, dt.join(subjects)]

pedDT.names = {'partOfRelease': 'releasesWhereSubjectExists'}

# import
rd3_prod.importDatatableAsCsv(pkg_entity = 'rd3_portal_cluster_ped', data = pedDT)
rd3_acc.importDatatableAsCsv(pkg_entity='rd3_portal_cluster_ped', data=pedDT)

#///////////////////////////////////////////////////////////////////////////////

# ~ 2 ~
# Validate Data
# There are several columns that need to be checked to make sure all values were
# correctly recoded and they match values in lookup tables.
#
#   - maternal identifiers
#   - paternal identifiers
#   - sex codes
#
# If any cases have been found, import the data and notify the data management
# committee.

# ~ 2a ~
# Check maternal and paternal identifiers
pedDT['unknownMID'] = dt.Frame([
  id if (id is not None) and (id not in subjectIDs) else None
  for id in pedDT['mid'].to_list()[0]
])

pedDT['unknownPID'] = dt.Frame([
  id if (id is not None) and (id not in subjectIDs) else None
  for id in pedDT['pid'].to_list()[0]
])

# ~ 2b ~
# Validate sex codes
pedDT['unknownSexCodes'] = dt.Frame([
  code if (code is not None) and (code not in sexcodes) else None
  for code in pedDT['sex1'].to_list()[0]
])


# check
pedDT[f.unknownMID != None, :]
pedDT[f.unknownPID != None, :]
pedDT[f.unknownSexCodes != None, :]

# if there are any unknown cases, import the dataset
# rd3.importDatatableAsCsv(pkg_entity = 'rd3_portal_cluster_ped', data = pedDT)
# rd3_acc.importDatatableAsCsv(pkg_entity='rd3_portal_cluster_ped', data=pedDT)