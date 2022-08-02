# '////////////////////////////////////////////////////////////////////////////
# ' FILE: utils.py
# ' AUTHOR: David Ruvolo
# ' CREATED: 2022-04-25
# ' MODIFIED: 2022-05-30
# ' PURPOSE: misc RD3 tools
# ' STATUS: stable
# ' PACKAGES: **see below**
# ' COMMENTS: NA
# '////////////////////////////////////////////////////////////////////////////

from datetime import datetime
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


class pedtools:
  """Pedigree File Tools"""

  @staticmethod
  def recodeSexCodes(value):
    """Recode Sex Code
    Ped files use the code 1, 2, or other to indicate patient's sex.
    Recode these values into RD3 terminology.

    @param value string containing a sex code to recode
    @return string containing an RD3 sex code
    """
    sexCodeMappings = {'1': 'M', '2': 'F', 'other': 'U'}
    return recodeValue(mappings=sexCodeMappings, value=value, label='PED Sex Code')

  @staticmethod
  def recodeAffectedStatus(value):
    """Recode Affected Status
    Recode PED code for affected status into RD3 terminology

    @param value string containing a value to recode
    @return string containing an RD3 affected status code
    """
    affectedStatusMappings = {'-9': None, '0': None, '1': False, '2': True}
    return recodeValue(
      mappings=affectedStatusMappings,
      value=value,
      label='Affected Status'
    )

  @staticmethod
  def parseFileRow(row: dict = None):
    """Parse Pedigree File Row
    Transform a row from a PED file into RD3 terminology and shape

    @param row dictionary that is a row from a PED file
    @return dictionary
    """
    return {
      'id': row[1],
      'subjectID': row[1],
      'fid': row[0],
      'mid': row[3],
      'pid': row[2],
      'sex1': pedtools.recodeSexCodes(value=row[4]),
      'clinical_status': pedtools.recodeAffectedStatus(value=row[5]),
      'upload': True
    }

  @staticmethod
  def validatedFileRow(row: dict = None, ids: list = None):
    """Validate PED Row
    Validate a single line of data extracted from a PED file

    @param data a dict containing a single line of data from a PED file.
        This is the output from `pedtools.parseFileRow`
    @param ids a list of IDs to compare to
    @param a dictionary containing validated data

    @return a dictionary
    """
    line = row
    if ('FAM' in line['id']) or (line['id'] not in ids):
      statusMsg(
        'ID {} from family {} does not exist'
        .format(line['id'], line['fid'])
      )
      line['upload'] = False
    if ('FAM' in line['mid']) or (line['mid'] == '0') or (line['mid'] not in ids):
      line['error_mid'] = line['mid']
      statusMsg(
        'removed mid {} as it starts with `FAM`, is `0`, or it does not exist'
        .format(line['mid'])
      )
      line['mid'] = None
    if ('FAM' in line['pid']) or (line['pid'] == '0') or (line['pid'] not in ids):
      line['error_pid'] = line['pid']
      statusMsg(
        'removed pid {} as it starts with `FAM`, is `0`, or it does not exist'
        .format(line['pid'])
      )
      line['pid'] = None
    return line

  @staticmethod
  def parseFileContents(contents, ids, filename):
    """Extract contents from a PED file

    Extract the contents of a pedigree file

    @param contents output from `cluster_read_file`
    @param ids a list of reference IDs to check against
    @param filename string containing the name of the file (for validation)

    @return list of dictionaries
    """
    data = []
    for line in contents:
      row = line.split()
      if len(row) == 6:
        row_data = pedtools.parseFileRow(line=row)
        processed_row_data = pedtools.validatedFileRow(
          data=row_data,
          ids=ids
        )
        data.append(processed_row_data)
      else:
        statusMsg('Line in ', filename, 'has',len(row),'columns instead of 6')
    return data


class phenotools:
  """Phenopacket Tools"""

  @staticmethod
  def recodeSexCodes(value):
      """Recode Phenopacket set value
      Record phenopackets sex values into RD3 terminology

      @param value string containing a sex code to recode
      @return string containing an RD3 sex code
      """
      mappings = {'female': 'F', 'male': 'M', 'unknown_sex': 'U'}
      return recodeValue(mappings=mappings, value=value, label="Sex Code")

  @staticmethod
  def formatDate(value):
    """Format date

    @param value string containing date to recode
    @return date string in yyyy-mm-dd format
    """
    try:
        return re.sub(r'(T00:00:00Z)$', '', value).split('-')[0]
    except:
        return value

  @staticmethod
  def unpackPhenotypicFeatures(data):
    """Unpack Phenotypic Features data
    Extract `phenotypicFetures` and separate into 'observed' and
    'unobserved' phenotypes.

    @param data output from data['phenopacket']['phenotypicFeatures']
    @return dictionary with lists for observed and unobserved phenotypes
    """
    result = {'phenotype': [], 'hasNotPhenotype': []}
    for row in data:
      if 'type' in row:
        hpoId = re.sub(r'^(HP:)', 'HP_', row['type']['id'])
        if not (hpoId in result['phenotype']) and not (hpoId in result['hasNotPhenotype']):
          if row.get('negated'):
            if row['negated']:
              result['hasNotPhenotype'].append(hpoId)
            if not row['negated']:
              result['phenotype'].append(hpoId)
          else:
            result['phenotype'].append(hpoId)
    return result

  @staticmethod
  def unpackDiseaseCodes(data, mappings: dict = None):
    """Unpack Diseases
    Extract disease IDs Unique ontologies: ['HP', 'Orphanet', 'HGNC', 'OMIM']
    and recode where necessary.

    @param data list of dictionaries from data['phenopacket']['diseases]
    @param mappings a dictionary containing 'incorrect' codes and new mappings

    @return dict with list of diagnostic- and onset codes
    """
    codes = {'diagnostic': [], 'onset': []}
    for row in data:
      if 'term' in row:
        if 'id' in row['term']:
          code1 = row['term']['id']
          if re.search(r'^((Orphanet:)|(ORDO:))', code1):
            code1 = re.sub(r'^((Orphanet:)|(ORDO:))', 'ORDO_', code1)
            if re.search(r'^((OMIM:)|(MIM:))', code1):
              code1 = re.sub(r'^((OMIM:)|(MIM:))', 'MIM_', code1)
              if mappings:
                if code1 in mappings:
                  code1 = recodeValue(
                    mappings=mappings,
                    value=code1,
                    label='Disease Code'
                  )
              if not (code1 in codes['diagnostic']):
                codes['diagnostic'].append(code1)
      if 'classOfOnset' in row:
        if 'id' in row['classOfOnset']:
          code2 = row['classOfOnset']['id']
          code2 = re.sub(r'^(HP:)', 'HP_', code2)
          if not (code2 in codes['onset']):
            codes['onset'].append(code2)
    return codes
