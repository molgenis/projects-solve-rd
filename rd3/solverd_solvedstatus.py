#///////////////////////////////////////////////////////////////////////////////
# FILE: solverd_solvedstatus.py
# AUTHOR: David Ruvolo
# CREATED: 2023-03-20
# MODIFIED: 2023-04-04
# PURPOSE: update solved status metadata in RD3
# STATUS: stable
# PACKAGES: **see below**
# COMMENTS: NA
#///////////////////////////////////////////////////////////////////////////////

import molgenis.client as molgenis
from os.path import abspath
from datatable import dt, f
import numpy as np
import datetime
import tempfile
import pytz
import csv
import re

def now(tz='Europe/Amsterdam'):
  return datetime.datetime.now(tz=pytz.timezone(tz)).strftime('%H:%M:%S.%f')[:-3]

def print2(*args):
  """Status Message
  Print a log-style message, e.g., "[16:50:12.245] Hello world!"
  @param *args one or more strings containing a message to print
  @return string
  """
  print(f"[{now()}] {' '.join(map(str, args))}")

class Molgenis(molgenis.Session):
  def __init__(self, *args, **kwargs):
    super(Molgenis, self).__init__(*args, **kwargs)
    self.fileImportEndpoint = f"{self._root_url}plugin/importwizard/importFile"
  
  def _datatableToCsv(self, path, datatable):
    """To CSV
    Write datatable object as CSV file

    @param path location to save the file
    @param data datatable object
    """
    data = datatable.to_pandas().replace({np.nan: None})
    data.to_csv(path, index=False, quoting=csv.QUOTE_ALL)
  
  def importDatatableAsCsv(self, pkg_entity: str, data):
    """Import Datatable As CSV
    Save a datatable object to as csv file and import into MOLGENIS using the
    importFile api.
    
    @param pkg_entity table identifier in emx format (package_entity)
    @param data a datatable object
    @param label a description to print (e.g., table name)
    """
    with tempfile.TemporaryDirectory() as tmpdir:
      filepath=f"{tmpdir}/{pkg_entity}.csv"
      self._datatableToCsv(filepath, data)
      with open(abspath(filepath),'r') as file:
        response = self._session.post(
          url = self.fileImportEndpoint,
          headers = self._headers.token_header,
          files = {'file': file},
          params = {'action': 'add_update_existing', 'metadataAction': 'ignore'}
        )
        if (response.status_code // 100 ) != 2:
          print2('Failed to import data into', pkg_entity, '(', response.status_code, ')')
        else:
          print2('Imported data into', pkg_entity)
        return response

def flattenDataset(data, columnPatterns=None):
  """Flatten Dataset
  Flatten all nested attributes in a recordset based on a specific column names.
  
  @param data a recordset
  @param column string containing row headers to detect: "subjectID|id|value"
  @return a new recordset containing flattened data
  """
  newData = data
  for row in newData:
    if '_href' in row:
      del row['_href']
    for column in row.keys():
      if isinstance(row[column], dict):
        if bool(row[column]):
          columnMatch = re.search(columnPatterns, ','.join(row[column].keys()))
          if bool(columnMatch):
            row[column] = row[column][columnMatch.group()]
          else:
            print(f'Variable {column} is type "dict", but no target column found')
        else:
          row[column] = None
      if isinstance(row[column], list):
        if bool(row[column]):
          values = []
          for nestedrow in row[column]:
            columnMatch = re.search(columnPatterns, ','.join(nestedrow.keys()))
            if bool(columnMatch):
              values.append(nestedrow[columnMatch.group()])
            else:
              print(f'Variable {column} is type "list", but no target column found')
          if bool(values):
            row[column] = ','.join(values)
        else:
          row[column] = None
  return newData
      
#///////////////////////////////////////////////////////////////////////////////

# ~ 0 ~
# Retrieve metadata
print2('Connecting to RD3 and retrieving data....')

# for local dev
# from dotenv import load_dotenv
# from os import environ
# load_dotenv()
# rd3 = Molgenis(environ['MOLGENIS_PROD_HOST'])
# rd3.login(environ['MOLGENIS_PROD_USR'], environ['MOLGENIS_PROD_PWD'])

# when deployed
rd3 = Molgenis('http://localhost/api/', token='${molgenisToken}')

# ~ 0b ~
# Retrieve data and flatten to data table objects
# get data from the portal and freezes
portal_data = rd3.get(
  entity = 'rd3_portal_recontact_solved',
  q='process_status=="N"',
  batch_size=10000
)

# Retrieve subject metadata to determine if a status has changed
solveRdSubjects=rd3.get('solverd_subjects', q="retracted!='Y'", batch_size=10000)
subjects = flattenDataset(
  data=solveRdSubjects,
  columnPatterns='subjectID|id|value'
)

#///////////////////////////////////////////////////////////////////////////////

# ~ 1 ~
# Process Freeze Subjects
# Store current solved status metadata (i.e., contact, recontact and solved status)
# Create a new object that will be used for comparison with incoming portal
# information
print2('Processing subject information & storing current solved status metadata...')

subject = {}
no_solved_update = 0
for row in subjects:
  contactInfo = 'missing'
  date_solved=''
  recontact = 'U'
  update_solved_status = 'Y'
  
  if 'recontact' in row:
    recontact = row['recontact']
  
  if 'contact' in row:
    contactInfo = row['contact']

  if 'date_solved' in row:
    if (
      (date_solved == '2019-10-01') and 
      (row.get('remarks') == 'Solved before the initial start of the project')
    ):
      update_solved_status = 'N'
      no_solved_update += 1

  if row['subjectID'] not in subjects:
    subject[row['subjectID']]={
      'id': row['subjectID'],
      'solved': row['solved'] if 'solved' in row else None,
      'date_solved': date_solved,
      'recontact': recontact,
      'contact': contactInfo,
      'update_solved_status': update_solved_status
    }
  else:
    print2(f"\tID {id} already exists")
    ids=subject[row['subjectID']]
    ids.append(row['subjectID'])
    if row['solved'] != subject[row['subjectID']]['solved']:
      raise SystemExit('Differences in current solved status in RD3 for '+row['subjectID']+' FIRST CHECK THIS!')
    if recontact != subject[row['subjectID']]['recontact']:
      raise SystemExit('Differences in current Recontact Incidental Findings in RD3 for '+row['subjectID']+' FIRST CHECK THIS!')
    if contactInfo != subject[row['subjectID']]['contact']:
      raise SystemExit('Differences in current Contact Information in RD3 for '+row['subjectID']+' FIRST CHECK THIS!')
    subject[row['subjectID']]={
      'id': ids,
      'solved': row['solved'],
      'date_solved': date_solved,
      'recontact': recontact,
      'contact': contactInfo,
      'update_solved_status': update_solved_status
    }

#///////////////////////////////////////////////////////////////////////////////

# ~ 2 ~
# Process Portal Status
# Determine if any of the solved status variables have changed
print2('Processing new portal status data....')

# init objects that will be used to store values to update in RD3
updateContact = []
updateDate = []
updatePtlHistory = []
updatePtlStatus = []
updatePtlRemark = []
updateRecontact = []
updateRemark = []
updateSolved = []

# process portal information
for row in portal_data:
  history='N'
  contactNew = None
  dateNew = None
  ptlRemark = None
  ptlStatus = None
  recontactNew = None
  remarkNew = None
  solvedNew = None
  
  # make sure subject exists
  status = row['subject'] in subject
  if status:
    id = subject[row['subject']]['id']
    
    # set solved status
    if row['solved'] == 'solved':
      solved = True
    elif row['solved'] == 'unsolved':
      solved = False
    elif row['solved'] == 'nA':
      solved = None
    else:
      raise SystemExit(
        f"Unknown Portal solved status {row['solved']} for {row['subject']}"
      )
    
    # if the case is unsolved, did it change from solved?
    if subject[row['subject']]['solved'] != solved:
      if subject[row['subject']]['update_solved_status'] == 'Y':
        if not subject[row['subject']]['solved']:
          solvedNew = solved
          dateNew = row['date_solved']
          remarkNew = None
        else:
          solvedNew = solved
          dateNew = None
          remarkNew = f"Solved status changed from solved to unsolved on {row['date_solved']}"
      else:
        ptlRemark = 'New solved status, but not updated!'
        ptlStatus='D'

    # update recontact information            
    if 'recontact' in row:
      if subject[row['subject']]['recontact'] == 'U':
        recontactNew = row['recontact']
      elif subject[row['subject']]['recontact'] != row['recontact']:
        recontactNew = row['recontact']

    # update contact information
    if 'contact' in row:
      if subject[row['subject']]['contact'] == 'missing':
        contactNew = row['contact']
      elif subject[row['subject']]['contact'] != row['contact']:
        contactNew = row['contact']

    # if contact, recontact, or solved status is present, update portal history 
    if contactNew != None or recontactNew != None or solvedNew != None:
      
      # if there is new contact information, set the history accordingly
      if contactNew != None:
        if ptlRemark == None:
          ptlRemark='Contact info updated'
        else:
          ptlRemark = ptlRemark +  ' Contact info updated'

        ptlStatus = 'P'

        # add record to portal updates dataset
        if {'id': id, 'contact': contactNew} not in updateContact:
          updateContact.append({'id': id, 'contact': contactNew})

      # set portal history for new recontact information
      if recontactNew != None:
        if ptlRemark == None:
          ptlRemark='Recontact info updated'
        else:
          ptlRemark = ptlRemark + ' and recontact info updated'
        
        ptlStatus='P'

        if {'id': id, 'recontact': recontactNew} not in updateRecontact:
          updateRecontact.append({'id': id, 'recontact': recontactNew})

      # set portal history for new solved statuses
      if solvedNew != None:
        if ptlRemark == None:
          ptlRemark = 'Solved Status updated'
        else:
          ptlRemark = ptlRemark + ' and solved status updated'

        ptlStatus = 'P'

        # add record to portal updates
        if {'id': id, 'solved': solvedNew} not in updateSolved:
          updateSolved.append({'id': id, 'solved': solvedNew})
          updateDate.append({'id': id, 'date_solved': dateNew})
          updateRemark.append({'id': id, 'remarks': remarkNew})
    
    # add history to portal table
    if ptlRemark != None:
      updatePtlHistory.append({'id': row['id'], 'history': 'Y'})
      updatePtlRemark.append({'id': row['id'], 'remark': ptlRemark})
      updatePtlStatus.append({'id': row['id'], 'process_status': ptlStatus})
    else:
      updatePtlHistory.append({'id': row['id'], 'history': 'Y'})
      updatePtlRemark.append({'id': row['id'], 'remark': 'No new information'})
      updatePtlStatus.append({'id': row['id'], 'process_status': 'P'})
  else:
    print2('Subject not in RD3')
    updatePtlHistory.append({'id': row['id'], 'history': 'Y'})
    updatePtlRemark.append({'id': row['id'], 'remark': 'subject not yet in RD3'})
    updatePtlStatus.append({'id': row['id'], 'process_status': 'N'})
    
#///////////////////////////////////////////////////////////////////////////////

# ~ 3 ~
# Merge portal update datasets
#
# NOTE: this above steps are from scripts that I inherited and I'm not sure how to
# properly update these steps without losing important information and checks so
# that I can use the File Import API. Instead, merge all `updatePlt*` objects and
# create a new datatable object that can be imported into the portal and the
# subjects table.
#

print2('Creating datasets for import via File API...')

# ~ 3a ~
# Create new subject dataset containing information to update

# to datatable
print2('\tCreating data for solverd_subjects....')
updateContactDT = dt.Frame(updateContact)
updateRecontactDT = dt.Frame(updateRecontact)
updateSolvedDT = dt.Frame(updateSolved)
updateDateDT = dt.Frame(updateDate)
updateRemarkDT = dt.Frame(updateRemark)

# create a flag that determines if the import step should run
shouldImportSubjectMetadta = False


# pull all IDs to create one dataset to import
portalIDs = []
for obj in [updateContactDT, updateRecontactDT, updateSolvedDT, updateDateDT, updateRemarkDT]:
  if 'id' in obj.names:
    portalIDs += obj['id'].to_list()[0]

# only run if there are updates
if portalIDs:
  shouldImportSubjectMetadta = True
  subjectUpdates = dt.unique(dt.Frame(id=portalIDs))
  subjectUpdates.key = 'id'

  # join datasets if they exist
  print2('\t\tMerging new datasets into one...')
  for obj in [updateContactDT, updateRecontactDT, updateSolvedDT, updateDateDT, updateRemarkDT]:
    if 'id' in obj.names:
      obj.key = 'id'
      subjectUpdates = subjectUpdates[:, :, dt.join(obj)]


  # pull rows to update
  subjectsDT = dt.Frame([
    row for row in subjects if row['subjectID'] in subjectUpdates['id'].to_list()[0]
  ])

  # to make sure no data is overwritten, merge values one-by-one where the update
  # dataset is not missing. Otherwise, return the existing value
  for column in ['contact', 'recontact', 'solved','date_solved', 'remarks']:
    print2(f"\t\tMerging data into solverd_subjects manually for {column}....")
    if (column in subjectUpdates.names) and (column in subjectsDT.names):
      subjectsDT[column] = dt.Frame([
        subjectUpdates[f.id == row[0], column].to_list()[0][0]
        if subjectUpdates[f.id == row[0], column].to_list()[0][0] != None
        else row[1]
        for row in subjectsDT[:, ['subjectID', column]].to_tuples()
      ])
else:
  print2('No new data to import into RD3. Check comments')

#///////////////////////////////////////

# ~ 3b ~
# Create new portal dataset containing information to update
print2('\tCreating data for portal table....')

# to data.table
updatePtlHistoryDT = dt.Frame(updatePtlHistory)
updatePtlStatusDT = dt.Frame(updatePtlStatus)
updatePtlRemarkDT = dt.Frame(updatePtlRemark)

# pull all IDs to create one dataset
print2('\t\tCollapsing IDs....')

portalUpdates = []
for obj in [updatePtlHistoryDT, updatePtlStatusDT, updatePtlRemarkDT]:
  if 'id' in obj.names:
    portalUpdates += obj['id'].to_list()[0]

portalUpdates = dt.unique(dt.Frame(id=portalUpdates))

# join data
print2('\t\tMerging portal datasets into one....')
updatePtlHistoryDT.key = 'id'
updatePtlStatusDT.key = 'id'
updatePtlRemarkDT.key = 'id'

portalUpdates = portalUpdates[:, :, dt.join(updatePtlHistoryDT)]
portalUpdates = portalUpdates[:, :, dt.join(updatePtlStatusDT)]
portalUpdates = portalUpdates[:, :, dt.join(updatePtlRemarkDT)]

# create portal dataset to import
portalDT = [
  row for row in portal_data if row['id'] in portalUpdates['id'].to_list()[0]
]
portalDT = flattenDataset(data = portalDT, columnPatterns='id')
portalDT = dt.Frame(portalDT)

# to make sure no data is overwritten, merge values one-by-one where the update
# dataset is not missing. Otherwise, return the existing value
for column in ['history','process_status','remark']:
  print2(f"Manually updating data for column {column} in main portal_status dataset")
  if (column in portalUpdates.names):

    if column not in portalDT.names:
      portalDT[column] = None
    
    portalDT[column] = dt.Frame([
      portalUpdates[f.id == row[0], column].to_list()[0][0]
      if portalUpdates[f.id == row[0], column].to_list()[0][0] != None
      else row[1]
      for row in portalDT[:, ['id', column]].to_tuples()
    ])

#///////////////////////////////////////////////////////////////////////////////

# ~ 4 ~
# Import data

print2('Importing datasets....')

# import subject metadata only if there is new data
if shouldImportSubjectMetadta:
  rd3.importDatatableAsCsv(pkg_entity='solverd_subjects', data = subjectsDT)

# update portal history
rd3.importDatatableAsCsv(pkg_entity='rd3_portal_recontact_solved', data=portalDT)
