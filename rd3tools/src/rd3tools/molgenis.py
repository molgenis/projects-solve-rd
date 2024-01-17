import molgenis.client as molgenis
from datetime import datetime
from os.path import abspath
import numpy as np
import tempfile
import pytz
import csv

def now(tz='Europe/Amsterdam', strftime=True):
  """Print the current time
  
  :param tz: string containing a timezone name
  :type tz: str
  
  :param strftime: if True, the current time will be formatted as HOUR:MINUTE:SECOND
  :type: strftime: bool
  
  :returns: current time
  :rtype: datetime or str
  """
  time = datetime.now(tz=pytz.timezone(tz))
  if strftime:
    return time.strftime('%H:%M:%S.%f')[:-3]
  return time
  
def print2(*args):
  """Print message with timestamp
  
  :param *args: one or more strings containing a message to print
  :type *args: str
  
  :returns: a message with a timestamp
  :rtype: str
  """
  message = ' '.join(map(str, args))
  print(f"[{now()}] {message}")


class Molgenis(molgenis.Session):
  def __init__(self, *args, **kwargs):
    super(Molgenis, self).__init__(*args, **kwargs)
    self.api_file_import = f"{self._root_url}plugin/importwizard/importFile"
  
  def _dt_to_csv(self, path, datatable):
    """Write datatable object to csv

    :param path: location to save the file
    :type path: str
    
    :param data: dataset to save
    :type data: datatable
    """
    data = datatable.to_pandas().replace({np.nan: None})
    data.to_csv(path, index=False, quoting=csv.QUOTE_ALL)
  
  def import_dt(self, pkg_entity: str, data):
    """Import datatable object as a CSV file
    
    :param pkg_entity: the identifier of a table in EMX format (package_entity)
    :type pkg_entity: str
    
    :param data: the dataset to import
    :type data: datatable
    
    :param label: a description to print (e.g., table name)
    :type label: str
    
    :returns: response
    :rtype: response
    """
    with tempfile.TemporaryDirectory() as tmp_dir:
      file_path=f"{tmp_dir}/{pkg_entity}.csv"
      self._dt_to_csv(file_path, data)

      with open(abspath(file_path),'r') as file:
        response = self._session.post(
          url = self.api_file_import,
          headers = self._headers.token_header,
          files = {'file': file},
          params = {
            'action': 'add_update_existing',
            'metadataAction': 'ignore'
          }
        )

        if (response.status_code // 100 ) != 2:
          print2('Failed to import data into', pkg_entity, '(', response.status_code, ')')
        else:
          print2('Imported data into', pkg_entity)

      file.close()
      tmp_dir.close()

      return response
