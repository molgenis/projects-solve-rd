#'////////////////////////////////////////////////////////////////////////////
#' FILE: molgenis2.py
#' AUTHOR: David Ruvolo
#' CREATED: 2022-07-28
#' MODIFIED: 2022-12-07
#' PURPOSE: molgenis.client extensions for DataTable
#' STATUS: stable
#' PACKAGES: **see below**
#' COMMENTS: NA
#'////////////////////////////////////////////////////////////////////////////

import molgenis.client as molgenis
from os.path import abspath
import numpy as np
import datetime
import requests
import tempfile
import pytz
import json
import csv

def now():
  return datetime.datetime.now(tz=pytz.timezone('Europe/Amsterdam')).strftime('%H:%M:%S.%f')[:-3]

def print2(*args):
  """Status Message
  Print a log-style message, e.g., "[16:50:12.245] Hello world!"
  @param *args one or more strings containing a message to print
  @return string
  """
  message = ' '.join(map(str, args))
  print(f'[{now()}] {message}')

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
  
  def batchUpdate(self, pkg_entity, column, data):
      """Batch Update
      Batch update a column in a table for 1000+ entities. Smaller datasets also work too.
      
      @param pkg_entity table identifier in emx format (package_entity)
      @param column name of column to update
      @param data a recordset to import
      
      @return a response code
      """
      url=f"{self._root_url}api/v2/{pkg_entity}/{column}"
      headers=self._headers.ct_token_header
      
      batches = range(0, len(data), 1000)
      print(f"Importing {len(batches)} batch{'es'[:len(batches)^1]} into {pkg_entity}${column}")
      for batch in batches:
        batchNumber = round(batch / 1000, ndigits=None) + 1
        batchToImport = json.dumps({'entities': data[batch:batch+1000] })
        response = self._session.put(url, headers=headers, data=batchToImport)
        try:
          if response.status_code // 100 == 2:
            print(f'Batch {batchNumber}: Imported {len(data)} entities')
          response.raise_for_status()
        except requests.exceptions.HTTPError as err:
          print(f'Batch {batchNumber} Error: unable to import data:\n{str(err)}')
