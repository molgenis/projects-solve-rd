#///////////////////////////////////////////////////////////////////////////////
# FILE: rd3_ped_03_mapping.py
# AUTHOR: David Ruvolo
# CREATED: 2022-09-21
# MODIFIED: 2023-03-27
# PURPOSE: Map PED data into RD3
# STATUS: stable
# PACKAGES: dotenv, datatable, os, rd3.utils, rd3.api
# COMMENTS: NA
#///////////////////////////////////////////////////////////////////////////////

from rd3.utils.utils import timestamp, flattenDataset
from rd3.api.molgenis2 import Molgenis
from datatable import dt, f, as_type
from dotenv import load_dotenv
from os import environ
from tqdm import tqdm
load_dotenv()

def uniqueStringValues(value, separator=','):
  """Split a comma-string and return unique values"""
  values = value.split(separator)
  return ','.join(list(set(values)))
  
def uniqueValuesById(data, groupby, column, dropDuplicates=True, keyGroupBy=True):
  """Unique Values By Id
  For a datatable object, collapse all unique values by ID into a comma
  separated string.

  @param data datatable object
  @param groupby name of the column that will serve as the grouping variable
  @param column name of the column that contains the values to collapse
  @param dropDuplicates If True, all duplicate rows will be removed
  @param keyGroupBy If True, returned object will be keyed using the value named in groupby
  
  @return datatable object
  """
  df = data.to_pandas()
  df[column] = df.dropna(subset=[column]) \
    .groupby(groupby)[column] \
    .transform(lambda val: ','.join(set(map(str, val))))
  if dropDuplicates:
    df = df[[groupby, column]].drop_duplicates()
  output = dt.Frame(df)
  if keyGroupBy:
    output.key = groupby
  return output

#///////////////////////////////////////////////////////////////////////////////

# ~ 0 ~
# Connect and retrieve data

# Connect to RD3: acc and prod
# By this point, the staging table on the ACC and PROD databases
# are updated. We can, theoretically, update both databases in one go. 
rd3_acc = Molgenis(environ['MOLGENIS_ACC_HOST'])
rd3_acc.login(environ['MOLGENIS_ACC_USR'], environ['MOLGENIS_ACC_PWD'])

rd3_prod = Molgenis(environ['MOLGENIS_PROD_HOST'])
rd3_prod.login(environ['MOLGENIS_PROD_USR'], environ['MOLGENIS_PROD_PWD'])

# get new ped data
rawPedDT = dt.Frame(rd3_acc.get('rd3_portal_cluster_ped', batch_size=1000))
pedDT = rawPedDT.copy()
del pedDT['_href']

# get subjects
rawsubjects = rd3_prod.get('solverd_subjects', batch_size=1000)
# rawsubjects = rd3_acc.get('solverd_subjects', batch_size=1000)
subjects= flattenDataset(rawsubjects, columnPatterns='subjectID|id|value')
subjectsDT = dt.Frame(subjects)
subjectsDT[:, dt.update(clinical_status=as_type(f.clinical_status, 'str'))]
subjectIDs = subjectsDT['subjectID'].to_list()[0]


#///////////////////////////////////////////////////////////////////////////////

# ~ 1 ~
# Prep PED data 

# ~ 1a ~
# make sure all subjects exist
pedDT['subjectExists'] = dt.Frame([
  id in subjectIDs for id in pedDT['subjectID'].to_list()[0]
])

# get counts and create a subset of missing subjects for verification
pedDT[:, dt.count(), dt.by(f.subjectExists)]
unknownSubjects = pedDT[f.subjectExists==False,:]
if unknownSubjects.nrows > 0:
  print('Warning: Unknown subjects detected. Saving data to file....')
  unknownSubjects.to_csv(f"data/rd3_ped_unknown_subjects_{timestamp()}.csv")

  # remove unknown subjects from processing
  pedDT = pedDT[f.subjectExists, :]

#///////////////////////////////////////

# ~ 1b ~
# change clinical_status to string
pedDT['clinical_status'] = as_type(pedDT['clinical_status'], dt.Type.str32)
pedDT['clinical_status'] = dt.Frame([
  value.lower() if value is not None else value
  for value in pedDT['clinical_status'].to_list()[0]
])

# check for duplicate rows
pedDT.nrows
if dt.unique(pedDT['id']).nrows != pedDT.nrows:
  print('Warning: There are duplicate rows....')
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
# To take advantage of the faster imports, we will use the file import API to
# upload the data. However, we will have to merge the new ped data with the
# data in the subjects table. The easiest method is to loop through all IDs
# and assign the values in the subjects data.

pedIDs = pedDT['subjectID'].to_list()[0]

for id in tqdm(pedIDs):
  newSubjectPedData = pedDT[f.subjectID == id, :]
  
  # if there is more than one ID, it is likely that a subject has more than
  # FID value. Collapse these values, get distinct records, and update FIDs
  if newSubjectPedData.nrows > 1:
    subjectPedFIDs = uniqueValuesById(
      data = newSubjectPedData,
      groupby = 'subjectID',
      column = 'fid',
      keyGroupBy = False
    )
    
    newSubjectPedData = newSubjectPedData[:, dt.first(f[:]), dt.by(f.subjectID)]
    newSubjectPedData['fid'] = subjectPedFIDs['fid']
  
  # update values -- set row metadata here rather than globally
  subjectsDT[f.subjectID == id, 'fid'] = newSubjectPedData['fid'].to_list()[0]
  subjectsDT[f.subjectID == id, 'mid'] = newSubjectPedData['mid'].to_list()[0]
  subjectsDT[f.subjectID == id, 'pid'] = newSubjectPedData['pid'].to_list()[0]
  subjectsDT[f.subjectID == id, 'dateRecordUpdated'] = timestamp()
  subjectsDT[f.subjectID == id, 'wasUpdatedBy'] = 'rd3-bot'
  subjectsDT[
    f.subjectID == id, 'clinical_status'
  ] = newSubjectPedData['clinical_status'].to_list()[0]

#///////////////////////////////////////////////////////////////////////////////


# ~ 2 ~
# Import data and update portal status

# update processed data
rawPedDT['processed'] = 'true'
if unknownSubjects.nrows > 0:
  print('Setting processed status for unknown subjects')
  for id in unknownSubjects['most'].to_list()[0]:
    rawPedDT[f.id ==id,'processed'] = 'false'

# import
rd3_acc.importDatatableAsCsv(pkg_entity='solverd_subjects', data = subjectsDT)
rd3_acc.importDatatableAsCsv(pkg_entity='rd3_portal_cluster_ped', data=rawPedDT)

rd3_prod.importDatatableAsCsv(pkg_entity='solverd_subjects', data = subjectsDT)
rd3_prod.importDatatableAsCsv(pkg_entity='rd3_portal_cluster_ped', data=rawPedDT)

# disconnect
rd3_acc.logout()
rd3_prod.logout()
