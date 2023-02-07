# '////////////////////////////////////////////////////////////////////////////
# ' FILE: utils.py
# ' AUTHOR: David Ruvolo
# ' CREATED: 2022-04-25
# ' MODIFIED: 2023-02-07
# ' PURPOSE: misc RD3 tools
# ' STATUS: stable
# ' PACKAGES: **see below**
# ' COMMENTS: NA
# '////////////////////////////////////////////////////////////////////////////

from datetime import datetime
from tqdm import tqdm
import numpy as np
import pytz
import re

def statusMsg(*args):
  """Status Message
  Print a log-style message, e.g., "[16:50:12.245] Hello world!"

  @param *args one or more strings containing a message to print
  @return string
  """
  t = datetime.utcnow().strftime('%H:%M:%S.%f')[:-3]
  print('\033[94m[' + t + '] \033[0m' + ' '.join(map(str, args)))


def addForwardSlash(path: str = None):
  """Add Forward Slash
  Adds forward slash to the end of a path if missing

  @param path a string containing a path to a given location (file, url, etc.)
  @returns string
  """
  return path + '/' if path[len(path)-1] != '/' else path


def buildRd3Paths(freeze, patch, baseFilePath):
  """Build all RD3 Entity IDs and Paths
  @param freeze       (str) : name of the freeze (i.e., "freeze1")
  @param patch        (str) : name of the patch (i.e., "patch1")
  @param baseFilePath (str) : base file path of the cluster
  @return dict of entity IDs and file paths
  """
  rd3EntityBase = f'rd3_{freeze}'
  clusterBasePath = f'{baseFilePath}/{freeze}-{patch}'
  return {
    'rd3_subjects': f'{rd3EntityBase}_subject',
    'rd3_subjectinfo': f'{rd3EntityBase}_subjectinfo',
    'rd3_sample': f'{rd3EntityBase}_sample',
    'rd3_labInfo': f'{rd3EntityBase}_labInfo',
    'rd3_file': f'{rd3EntityBase}_file',
    'cluster_ped': f'{clusterBasePath}/ped/',
    'cluster_phenopacket': f'{clusterBasePath}/phenopacket/'
  }


def createUrlFilter(columnName, array):
  """Create Url Filter
  Collapse an array of values into a molgenis friendly data explorer filter.
  It is recommended to pass unique values. Make sure the returned value is
  quoted. (urllib.parse.quote)

  @param columnName name of column used to limit the results
  @param array list of values

  @examples
  ```
  createUrlFilter('gender', ['Female', 'Male'])

  #> 'gender=q=Female,gender=q=Male'
  ```

  @return string
  """
  return ','.join([f"{columnName}=q={d}" for d in list(set(array))])


def dtFrameToRecords(data):
  """Datatable object to records
  @param data : datatable object
  @return list of dictionaries
  """
  return data.to_pandas().replace({np.nan: None}).to_dict('records')

def flattenBoolArray(array, keepTrueValues=True):
  """Flatten Bool Array
  Collapse a list of boolean values. Remove null values before using this
  function.

  @param array list containing bool values
  @param keepTrueValues if True, if the value 'True' exists in the array,
      then True is returned.
  @examples
  ```py
  flattenBoolArray([True,True,True, None])

  #> True
  ```

  @return bool
  """
  values = list(set(filter(lambda d: d is not None, array)))
  if len(values) >= 2:
    if keepTrueValues:
      return True in values
    else:
      return ','.join(map(str, values))
  elif len(values) == 1:
    return values[0]
  else:
    return None

def flattenDataset(data, columnPatterns=None):
  """Flatten Dataset
  Flatten all nested attributes in a recordset based on a specific column names.
  
  @param data a recordset
  @param column string containing row headers to detect: "subjectID|id|value"
  @return a new recordset containing flattened data
  """
  newData = data
  for row in tqdm(newData):
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

def flattenStringArray(array):
  """Flatten String Array
  Return a comma separated string of unique values for multiple items in
  an array

  @param array list containing values
  @examples
  ````
  array=['a,b,c', 'x,y,z', 'l,m,n,o,p']

  flattenStringArray(array)

  #> 'a,b,c,l,m,n,o,p,x,y,z'
  ```

  @return string
  """
  values = set([value for element in array for value in element.split(',')])
  return ','.join(sorted(values))


def flattenValueArray(array):
  """Flatten Value Array
  Return a string unique values from a list of values

  @param array list containing values
  @example
  ```
  array=['apple','orange','pear','lemon','apple']
  flattenValueArray(array)
  #> 'apple,lemon,orange,pear'
  ```
  """
  return ','.join(sorted(set(array)))


def recodeValue(mappings: None, value: str = None, label: str = None, warn=True):
  """Recode value
  Recode values using new mappings. It is recommended to define all
  mappings using lowercase letters and the the input for value should
  also be lowered.

  @param mappings a datatable object containing where each key
      corresponds to a new value
  @param value string containing a value to recode
  @param label string that indicates the mapping type for error messages
  @param warn If True (default), a message will be displayed when a value
      cannot be mapped

  @examples
      mappings = {'First Name': 'firstname', 'Last Name': 'surname'}
      recodeValue(mappings=mappings, value='Last Name', label='Name)

  @return string or NoneType
  """
  try:
    return mappings[value]
  except:
    if bool(value) and warn:
      error = f'Error in {label if label else ""} recoding: \"{value}\" not found'
      statusMsg(error)
    return None


def timestamp(tz='Europe/Amsterdam', timeFormat='%Y-%m-%d'):
  """Timestamp
  Return a timestamp specific to user's tz and desired time format

  @param tz string containing a timezone
  @param timeFormat string indicating how to format the output

  @examples
      timestamp()

  @return datetime object
  """
  return datetime.now(tz=pytz.timezone(tz)).strftime(timeFormat)


def toKeyPairs(data, keyAttr: str = None, valueAttr: str = None):
  """To Key Pairs
  Convert a list of dictionaries into a key-value dictionary. This method
  is useful for creating objects for mapping tables.

  @param data list of dictionaries that you wish to convert
  @param keyAttr attribute that will be key
  @param valueAttr attribute that contains the value

  @return a dictionary
  """
  maps = {}
  for d in data:
    maps[d[keyAttr]] = d.get(valueAttr)
  return maps
