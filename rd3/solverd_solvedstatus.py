#///////////////////////////////////////////////////////////////////////////////
# FILE: solverd_solvedstatus.py
# AUTHOR: David Ruvolo
# CREATED: 2023-03-20
# MODIFIED: 2024-01-11
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
  """Get the current time based on current time zone
  
  :param tz: region to present time in
  :type tz: str
  
  :returns: string containing the current time formatted as HH:MM:SS
  :rtype: str
  
  """
  return datetime.datetime.now(tz=pytz.timezone(tz)).strftime('%H:%M:%S.%f')[:-3]

def print2(*args):
  """Print one or more strings as console message with a timestamp
  
  :param *args: one or more strings containing a message to print
  :param *args: str
  """
  print(f"[{now()}] {' '.join(map(str, args))}")

class Molgenis(molgenis.Session):
  def __init__(self, *args, **kwargs):
    super(Molgenis, self).__init__(*args, **kwargs)
    self.fileImportEndpoint = f"{self._root_url}plugin/importwizard/importFile"
  
  def _datatableToCsv(self, path, datatable):
    """To CSV
    Write datatable object as CSV file

    :param path: location to save the file
    :type path: string
    
    :param data: datatable object to save
    :type: datatable
    """
    data = datatable.to_pandas().replace({np.nan: None})
    data.to_csv(path, index=False, quoting=csv.QUOTE_ALL)
  
  def importDatatableAsCsv(self, pkg_entity: str, data):
    """Import datatable object as a CSV file
    
    :param pkg_entity: table identifier in emx format (package_entity)
    :param pkg_entity: str
    
    :param data: datatable object to import into an EM1 instance
    :param data: datatable
    
    :param label: a description to print (e.g., table name)
    :type label: str 
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

def flattenDataset(data, nested_cols=None):
  """Flatten a dataset with nested attributes into a recordset
  
  :param data: recordset containing nested attributes
  :type data: recordset; list of dicts
  
  :param nested_cols: one or more attribute names used to extract the target
      value when there is nested data(e.g., "subjectID|id|value")
  :type column: string
  
  :returns: a new recordset containing flattened data
  :rtype: recordset
  """
  new_data = data
  for row in new_data:
    if '_href' in row:
      del row['_href']
    for column in row.keys():
      if isinstance(row[column], dict):
        if bool(row[column]):
          column_match = re.search(nested_cols, ','.join(row[column].keys()))
          if bool(column_match):
            row[column] = row[column][column_match.group()]
          else:
            print(f'Variable {column} is type "dict", but no target column found')
        else:
          row[column] = None
      if isinstance(row[column], list):
        if bool(row[column]):
          values = []
          for nested_row in row[column]:
            column_match = re.search(nested_cols, ','.join(nested_row.keys()))
            if bool(column_match):
              values.append(nested_row[column_match.group()])
            else:
              print(f'Variable {column} is type "list", but no target column found')
          if bool(values):
            row[column] = ','.join(values)
        else:
          row[column] = None
  return new_data

def recode_val(val: str=None, map: dict=None, force_quit: bool=False):
  """Recode a raw value to a 
  
  :param val: a single value to recode
  :type val: str
  
  :param map: an object containing one-to-one mappings
  :type map: dict
  
  :param force_quit: if true, errors will quit process. Otherwise, the message
    will be printed
  :type force_quit: bool
  
  :returns: a new value
  :rtype: sting
  """
  try:
    return map[val]
  except KeyError as err:
    msg=f"Failed to convert {val}"
    if force_quit:
      raise KeyError(f"msg\n{err}")
    else:
      print(msg)
      return None

def collapse_remarks(*args):
  """Reduce multiple strings into a single string and filter NoneTypes"""
  values = list(args)
  values_cleaned = list(filter(None,values))
  return ';'.join(values_cleaned)

#///////////////////////////////////////////////////////////////////////////////

# ~ 0 ~
# Retrieve metadata
print2('Connecting to RD3 and retrieving data....')

# for local dev
from dotenv import load_dotenv
from os import environ
load_dotenv()
rd3 = Molgenis(environ['MOLGENIS_PROD_HOST'])
rd3.login(environ['MOLGENIS_PROD_USR'], environ['MOLGENIS_PROD_PWD'])

# when deployed
# rd3 = Molgenis('http://localhost/api/', token='${molgenisToken}')

# ~ 0b ~
# Retrieve data and flatten to data table objects
# get data from the portal and freezes
# ['2023-03-23', '2023-11-07', '2023-11-13', '2023-12-12']
portal_data_raw = rd3.get(
  entity = 'rd3_portal_recontact_solved',
  # q='process_status=="N";(date_solved=ge=2023-03-23;date_solved=le=2023-03-23)',
  # q='process_status=="N";(date_solved=ge=2023-11-07;date_solved=le=2023-11-07)',
  # q='process_status=="N";(date_solved=ge=2023-11-13;date_solved=le=2023-11-13)',
  q='process_status=="N";(date_solved=ge=2023-12-12;date_solved=le=2023-12-12)',
  batch_size=10000
)

portal_data = flattenDataset(portal_data_raw, 'id')
portal_dt = dt.Frame(portal_data)

# retrive subject metadata and flatten
# the solved status metadata will be added to the subject dataset,
# subjects_raw = rd3.get('solverd_subjects', q="retracted!='Y'", batch_size=10000)
subjects_raw = rd3.get('solverd_subjects', q="retracted!='Y'", batch_size=1000)
subjects = flattenDataset(subjects_raw,'subjectID|id|value')
subjects_dt = dt.Frame(subjects)

#///////////////////////////////////////////////////////////////////////////////

# ~ 1 ~
# Process current subject metadata
#
# To determine how to update solved status with incoming metadata, determine
# which records should be updated. By default, all records should be evaluated.
# However, there are a few circumstances where records should be ignored. The
# following steps will identify these cases.
print2('Processing current subject information...')

# by default, set all records to True
subjects_dt['should_update'] = True

# check to see if record was solved before the start of the project
subjects_dt['should_update'] = dt.Frame([
  False
  if row[1] == '2019-10-01' and
    row[2] == 'Solved before the initial start of the project'
  else row[0]
  for row in subjects_dt[:, ['should_update', 'date_solved', 'remarks']].to_tuples()
])

# print counts
should_update_counts = subjects_dt[:, dt.count(), dt.by(f.should_update)]
print2('Summary of subjects that will be updated:\n\n',
  '\tAvailable:       ', should_update_counts[f.should_update, 'count'].to_list()[0][0],'\n'
  '\tUnavailable*:    ', should_update_counts[f.should_update==False,'count'].to_list()[0][0],
  '\n',
  '\n',
  '*Unavailable subjects are subjects that were solved before the start of the project (2019-10-01)'
)

# compile a list of subject ids that should be updated
subject_ids = subjects_dt[f.should_update, 'subjectID'].to_list()[0]

#///////////////////////////////////////////////////////////////////////////////

# ~ 2 ~
# Process incoming metadata

print2('Processing incoming metadata...')

# ~ 2a ~
# Identify unknown subjects
#
# Check to see if there are any unknown identifiers. If there are any cases,
# compile them into a new list and send to the data management office

portal_dt['is_unknown_subject'] = dt.Frame([
  value not in subject_ids
  for value in portal_dt['subject'].to_list()[0]
])

# portal_dt[:, dt.count(), dt.by(f.is_unknown_subject)]

# for any unknown subjects, make sure they are flagged properly
portal_dt[['history', 'remark', 'process_status']] = dt.Frame([
  ('Y', 'subject not yet in RD3', 'N')
  if value is True else (None,None,None)
  for value in portal_dt['is_unknown_subject'].to_list()[0]
])

# warn if there are any unknown subjects
unknown_count = portal_dt[f.is_unknown_subject, 'subject'].nrows
if unknown_count > 0:
  print2(f"Identified unknown subjects (n={unknown_count})", 'Isolating records and importing...')
  
  # select records where the subject is unknown and import
  unknown_dt = portal_dt[f.is_unknown_subject, :]
  rd3.importDatatableAsCsv('rd3_portal_recontact_solved', unknown_dt)

  # remove unknown subject cases from dataset and determine if script should exit  
  portal_dt = portal_dt[f.is_unknown_subject==False,:]
  if not portal_dt.nrows:
    raise SystemExit('No new records to import as all records contain unknown subject IDs. Please check this.')

#///////////////////////////////////////

# ~ 2b ~
# Process solved status
#
# Recode the incoming solved status values and determine if
# the status has changed
print2('Processing new solved status data...')

# Recode solved status and check to see if solved status is new
solved_status_mappings = {
  'solved': True,
  'unsolved': False,
  'nA': None
}

# make sure all incoming solved status values are valid. 
# if not, stop processing data
official_vals = solved_status_mappings.keys()
incoming_vals = dt.unique(portal_dt['solved']).to_list()[0]

for val in incoming_vals:
  if val not in official_vals:
    raise ValueError(f"Solved status value {val} is not valid")


# recode solved status
portal_dt['is_solved'] = dt.Frame([
  recode_val(val=value, map=solved_status_mappings)
  for value in portal_dt['solved'].to_list()[0]
])


# set remark value for solved status and flag cases where they should be updated
default_solved_record = (None, 'Y', 'P', True, False)
portal_dt[['remark', 'history', 'process_status', 'should_update', 'should_update_solved']] = dt.Frame([
    (
      collapse_remarks(row[3], f"Solved status changed from solved to unsolved on {row[2]}"),
     'Y',
     'P',
     True,
     True
    )
    if row[1] in subject_ids and row[0] is True
    else default_solved_record
  for row in portal_dt[:, ['is_solved', 'subject', 'date_solved','remark']].to_tuples()
])

#///////////////////////////////////////

# ~ 2c ~
# Process recontact data
#
# Recontact information does not need to be cleaned. If the existing value is
# unknown or the incoming value is different, then replace the value in the
# subjects table with the incoming value
print2('Processing recontact data...')

portal_dt[['remark', 'history', 'process_status', 'should_update', 'should_update_recontact']] = dt.Frame([
  (
    collapse_remarks(row[2], 'Recontact info updated'),
    'Y',
    'P',
    True,
    True
  )
  if subjects_dt[f.subjectID == row[1], 'recontact'].to_list()[0][0] != row[0]
  else (row[2], 'Y', 'P', True, False)
  for row in portal_dt[:, ['recontact', 'subject', 'remark']].to_tuples()
])


#///////////////////////////////////////

# ~ 2d ~
# Set remark for records that have not changed
print2('Identifying records that are processed and have no changes...')

portal_dt['should_update_count'] = dt.Frame([
  sum([int(value) for value in row])
  for row in portal_dt[:, ['should_update','should_update_recontact','should_update_solved']].to_tuples()
])

portal_dt['remark'] = dt.Frame([
  'No new information' if row[0] == 1 else row[1]
  for row in portal_dt[:, ['should_update_count', 'remark']].to_tuples()
])


#///////////////////////////////////////////////////////////////////////////////


# ~ 3 ~
# Update database with new data
#
# Update subject metadata with new contact and recontact information, as well as
# new solved status information. To reduce import times, indicate which 
# records should be imported (i.e., changed) and separate

print2('Updating values in subjects table...')

# reduce data to update records only
update_dt = portal_dt[f.should_update & (f.should_update_recontact | f.should_update_solved), :]
update_ids = update_dt['subject'].to_list()[0]

print2('Will update records of',len(update_ids),'subjects')

subjects_dt['should_import'] = False

for id in update_ids:
  row = update_dt[f.subject==id,:]
  
  # update solved status
  if row['should_update_solved'].to_list()[0][0] is True:
    new_solved_values = list(row[:, ['is_solved', 'date_solved']].to_tuples()[0]) + [True]
    subjects_dt[f.subjectID==id, ['solved', 'date_solved', 'should_import']] = new_solved_values
    
  # update recontact status
  if row['should_update_recontact'].to_list()[0][0] is True:
    new_recontact_values = row['recontact'].to_list()[0] + [True]
    subjects_dt[f.subjectID==id, ['recontact', 'should_import']] = new_recontact_values
  

# isolate records to import to reduce import times
subjects_import_dt = subjects_dt[f.should_import, :]

#///////////////////////////////////////////////////////////////////////////////

# ~ 4 ~
# Import data
rd3.importDatatableAsCsv(pkg_entity='solverd_subjects', data=subjects_import_dt)
rd3.importDatatableAsCsv(pkg_entity='rd3_portal_recontact_solved', data=portal_dt)


# subj_unknown = rd3.get(
#   entity='rd3_portal_recontact_solved',
#   q='process_status==N;remark=="subject not yet in RD3"',
#   batch_size=10000
# )

# subj_unknown_dt = dt.Frame(
#   flattenDataset(
#     data = subj_unknown,
#     nested_cols='id'
#   )
# )

# subj_unknown_dt[
#   :, dt.first(f[:]), dt.by(f.subject)
# ][
#   :, (f.subject,f.process_status, f.remark)
# ].to_csv('~/Desktop/solverd_2024-01-11_unknown_subjects.csv')
