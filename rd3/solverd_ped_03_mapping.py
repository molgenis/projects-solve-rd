#///////////////////////////////////////////////////////////////////////////////
# FILE: rd3_ped_03_mapping.py
# AUTHOR: David Ruvolo
# CREATED: 2022-09-21
# MODIFIED: 2022-01-24
# PURPOSE: Map PED data into RD3
# STATUS: stable
# PACKAGES: dotenv, datatable, os, rd3.utils, rd3.api
# COMMENTS: 
#///////////////////////////////////////////////////////////////////////////////

from rd3.utils.utils import dtFrameToRecords
from rd3.api.molgenis import Molgenis
from dotenv import load_dotenv
from datatable import dt, f, as_type
from os import environ
load_dotenv()

def uniqueStringValues(value, separator=','):
  """Split a comma-string and return unique values"""
  values = value.split(separator)
  return ','.join(list(set(values)))

# Connect to RD3: acc and prod
# By this point, the staging table on the ACC and PROD databases
# are updated. We can, theoretically, update both databases in one go. 
rd3_acc = Molgenis(environ['MOLGENIS_ACC_HOST'])
rd3_acc.login(environ['MOLGENIS_ACC_USR'], environ['MOLGENIS_ACC_PWD'])

rd3_prod = Molgenis(environ['MOLGENIS_PROD_HOST'])
rd3_prod.login(environ['MOLGENIS_PROD_USR'], environ['MOLGENIS_PROD_PWD'])

#///////////////////////////////////////////////////////////////////////////////

# ~ 1 ~
# Apply pre-import transformations

# get data
pedDT = dt.Frame(rd3_acc.get('rd3_portal_cluster_ped', batch_size=10000))
del pedDT['_href']

# rename releases column to RD3 terminology
pedDT.names = {'releasesWhereSubjectExists': 'partOfRelease'}

# change clinical_status to string
pedDT['clinical_status'] = as_type(pedDT['clinical_status'], dt.Type.str32)
pedDT['clinical_status'] = dt.Frame([
  value.lower() if value is not None else value
  for value in pedDT['clinical_status'].to_list()[0]
])

# make sure the 'clusterRelease' is in the 'partOfRelease' column
pedDT['partOfRelease'] = dt.Frame([
  f"{row[0]},{row[1]}" if row[1] not in row[0] else row[0]
  for row in pedDT[:, ['partOfRelease', 'clusterRelease']].to_tuples()
])

pedDT['partOfRelease'] = dt.Frame([
  uniqueStringValues(value) if value else value
  for value in pedDT['partOfRelease'].to_list()[0]
])

# check for duplicate rows
pedDT.nrows
dt.unique(pedDT['id']).nrows == pedDT.nrows
# pedDT[:, dt.first(f[:]), dt.by(f.id)].nrows
# pedDT[:, dt.first(f[:]), dt.by(f.id)].nrows == pedDT.nrows
# pedDT = pedDT[:, dt.first(f[:]), dt.by(f.subjectID)]
# dt.unique(pedDT['id']).nrows == pedDT.nrows

#///////////////////////////////////////////////////////////////////////////////

# ~ 1 ~
# Prepare import objects
# Everything is ready for import into 'solverd_subjects'. The columns that will
# be updated are:
#  - family identifier: `fid`
#  - maternal identifer: `mid`
#  - paternal identifer: `pid`
#  - affected status: `clinical_status`
#
# We will create a separate dataset for each column that we would like to 
# updated. Each dataset will contain `subjectID` and the target column.
# We will also filter the dataset to rows that have data (i.e., remove NAs).
# This will allow us to update the necessary rows rather than potentially
# overwriting values with blank values.

# create datasets to import
familyIDs = pedDT[f.fid!=None, (f.subjectID, f.fid)]
maternalIDs = pedDT[f.mid!=None, (f.subjectID, f.mid)]
paternalIDs = pedDT[f.pid!=None, (f.subjectID, f.pid)]
clinicalStatus = pedDT[f.clinical_status!=None, (f.subjectID, f.clinical_status)]
releases = pedDT[f.partOfRelease != None, (f.subjectID, f.partOfRelease)]

# update `fid`
# rd3_acc.updateColumn(
#   entity = 'solverd_subjects',
#   attr = 'fid',
#   data = dtFrameToRecords(familyIDs)
# )

# rd3_prod.updateColumn(
#   entity = 'solverd_subjects',
#   attr = 'fid',
#   data = dtFrameToRecords(familyIDs)
# )

# update `mid`
# rd3_acc.updateColumn(
#   entity = 'solverd_subjects',
#   attr = 'mid',
#   data = dtFrameToRecords(maternalIDs)
# )
# rd3_prod.updateColumn(
#   entity = 'solverd_subjects',
#   attr = 'mid',
#   data = dtFrameToRecords(maternalIDs)
# )

# update `pid`
# rd3_acc.updateColumn(
#   entity = 'solverd_subjects',
#   attr = 'pid',
#   data = dtFrameToRecords(paternalIDs)
# )

# rd3_prod.updateColumn(
#   entity = 'solverd_subjects',
#   attr = 'pid',
#   data = dtFrameToRecords(paternalIDs)
# )

# update `clinical_status`
# rd3_acc.updateColumn(
#   entity = 'solverd_subjects',
#   attr = 'clinical_status', 
#   data= dtFrameToRecords(clinicalStatus)
# )

# rd3_prod.updateColumn(
#   entity = 'solverd_subjects',
#   attr = 'clinical_status', 
#   data= dtFrameToRecords(clinicalStatus)
# )

# update patch
# rd3_acc.updateColumn(
#   entity = 'solverd_subjects',
#   attr = 'partOfRelease',
#   data = dtFrameToRecords(releases)
# )

# rd3_prod.updateColumn(
#   entity = 'solverd_subjects',
#   attr = 'partOfRelease',
#   data = dtFrameToRecords(releases)
# )
     
# disconnect
rd3_acc.logout()
rd3_prod.logout()
